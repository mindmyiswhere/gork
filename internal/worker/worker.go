package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orchestratorv1 "github.com/mindmyiswhere/gork/gen/go/orchestrator/v1"
	"github.com/mindmyiswhere/gork/internal/config"
)

// Worker — клиент оркестратора, который выполняет задачи.
type Worker struct {
	cfg      config.WorkerConfig
	registry *HandlerRegistry
	logger   *slog.Logger

	mu        sync.Mutex
	running   map[string]context.CancelFunc
	cancelled map[string]struct{}
}

func New(cfg config.WorkerConfig, registry *HandlerRegistry, logger *slog.Logger) *Worker {
	return &Worker{
		cfg:       cfg,
		registry:  registry,
		logger:    logger,
		running:   make(map[string]context.CancelFunc),
		cancelled: make(map[string]struct{}),
	}
}

// Run запускает воркер. Возвращает, когда ctx отменён или стрим упал неисправимо.
func (w *Worker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker shutdown requested")
			return nil
		default:
		}

		err := w.runSession(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("session failed", "err", err)
		}

		w.logger.Info("reconnecting", "backoff", w.cfg.ReconnectBackoff)
		select {
		case <-time.After(w.cfg.ReconnectBackoff):
		case <-ctx.Done():
			return nil
		}
	}
}

// runSession — одна сессия: подключение, регистрация, работа до отключения.
func (w *Worker) runSession(ctx context.Context) error {
	conn, err := grpc.NewClient(
		w.cfg.OrchestratorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("grpc client: %w", err)
	}
	defer conn.Close()

	client := orchestratorv1.NewWorkerServiceClient(conn)
	stream, err := client.Work(ctx)
	if err != nil {
		return fmt.Errorf("open work stream: %w", err)
	}

	workerID := w.cfg.ID
	if workerID == "" {
		workerID = uuid.NewString()
	}
	types := w.cfg.SupportedTypes
	if len(types) == 0 {
		types = w.registry.Types()
	}

	if err := stream.Send(&orchestratorv1.WorkerMessage{
		Msg: &orchestratorv1.WorkerMessage_Register{
			Register: &orchestratorv1.Register{
				WorkerId:       workerID,
				SupportedTypes: types,
				Concurrency:    w.cfg.Concurrency,
			},
		},
	}); err != nil {
		return fmt.Errorf("send register: %w", err)
	}
	w.logger.Info("registered with orchestrator",
		"worker_id", workerID,
		"types", types,
		"concurrency", w.cfg.Concurrency,
	)

	sendCh := make(chan *orchestratorv1.WorkerMessage, 16)
	writerErr := make(chan error, 1)

	go func() {
		defer close(writerErr)
		for {
			select {
			case <-ctx.Done():
				writerErr <- ctx.Err()
				return
			case msg, ok := <-sendCh:
				if !ok {
					return
				}
				if err := stream.Send(msg); err != nil {
					writerErr <- err
					return
				}
			}
		}
	}()

	go w.heartbeatLoop(ctx, sendCh, workerID)

	recvErr := make(chan error, 1)
	go func() {
		defer close(recvErr)
		for {
			assignment, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				recvErr <- nil
				return
			}
			if err != nil {
				recvErr <- err
				return
			}
			switch m := assignment.Msg.(type) {
			case *orchestratorv1.TaskAssignment_Task:
				w.dispatch(ctx, m.Task, sendCh, workerID)
			case *orchestratorv1.TaskAssignment_Cancel:
				w.cancelTask(m.Cancel.TaskId)
			}
		}
	}()

	select {
	case <-ctx.Done():
		w.stopAll()
		return ctx.Err()
	case err := <-writerErr:
		w.stopAll()
		return err
	case err := <-recvErr:
		w.stopAll()
		return err
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context, sendCh chan<- *orchestratorv1.WorkerMessage, workerID string) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			running := w.runningTaskIDs()
			msg := &orchestratorv1.WorkerMessage{
				Msg: &orchestratorv1.WorkerMessage_Heartbeat{
					Heartbeat: &orchestratorv1.Heartbeat{
						WorkerId:       workerID,
						RunningTaskIds: running,
					},
				},
			}
			select {
			case sendCh <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (w *Worker) runningTaskIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.running))
	for id := range w.running {
		out = append(out, id)
	}
	return out
}

func (w *Worker) dispatch(parentCtx context.Context, task *orchestratorv1.Task, sendCh chan<- *orchestratorv1.WorkerMessage, workerID string) {
	w.mu.Lock()
	_, wasCancelled := w.cancelled[task.Id]
	if wasCancelled {
		delete(w.cancelled, task.Id)
	}
	w.mu.Unlock()

	if wasCancelled {
		w.logger.Info("task already cancelled, skipping", "task_id", task.Id)
		return
	}

	handler, err := w.registry.Get(task.Type)
	if err != nil {
		w.logger.Warn("no handler for task", "task_id", task.Id, "type", task.Type)
		w.sendResult(sendCh, &orchestratorv1.TaskResult{
			TaskId:  task.Id,
			Status:  orchestratorv1.TaskStatus_TASK_STATUS_FAILED,
			Error:   err.Error(),
			Attempt: 1,
		})
		return
	}

	taskCtx, cancel := context.WithCancel(parentCtx)
	if task.Timeout != nil && task.Timeout.AsDuration() > 0 {
		taskCtx, cancel = context.WithTimeout(parentCtx, task.Timeout.AsDuration())
	}

	w.mu.Lock()
	w.running[task.Id] = cancel
	w.mu.Unlock()

	go func() {
		defer func() {
			cancel()
			w.mu.Lock()
			delete(w.running, task.Id)
			w.mu.Unlock()
		}()

		w.logger.Info("task started", "task_id", task.Id, "type", task.Type)

		start := time.Now()
		output, err := handler(taskCtx, task.Payload)
		elapsed := time.Since(start)

		result := &orchestratorv1.TaskResult{
			TaskId:  task.Id,
			Attempt: task.Attempt,
			Output:  output,
		}
		if err != nil {
			result.Status = orchestratorv1.TaskStatus_TASK_STATUS_FAILED
			result.Error = err.Error()
			w.logger.Warn("task failed",
				"task_id", task.Id,
				"elapsed", elapsed,
				"err", err)
		} else {
			result.Status = orchestratorv1.TaskStatus_TASK_STATUS_SUCCEEDED
			w.logger.Info("task succeeded",
				"task_id", task.Id,
				"elapsed", elapsed)
		}

		w.sendResult(sendCh, result)
	}()
}

func (w *Worker) sendResult(sendCh chan<- *orchestratorv1.WorkerMessage, r *orchestratorv1.TaskResult) {
	msg := &orchestratorv1.WorkerMessage{
		Msg: &orchestratorv1.WorkerMessage_Result{Result: r},
	}
	select {
	case sendCh <- msg:
	case <-time.After(2 * time.Second):
		w.logger.Error("failed to enqueue result, dropping", "task_id", r.TaskId)
	}
}

func (w *Worker) stopAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, cancel := range w.running {
		w.logger.Info("cancelling running task", "task_id", id)
		cancel()
	}
}

func (w *Worker) cancelTask(taskID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cancel, ok := w.running[taskID]
	if !ok {
		w.cancelled[taskID] = struct{}{}
		w.logger.Info("received cancel for non-running task", "task_id", taskID)
		return
	}

	w.logger.Info("cancelling running task by orchestrator", "task_id", taskID)
	cancel()
	delete(w.running, taskID)
}
