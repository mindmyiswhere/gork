package grpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orchestratorv1 "github.com/mindmyiswhere/gork/gen/go/orchestrator/v1"
	"github.com/mindmyiswhere/gork/internal/domain"
	"github.com/mindmyiswhere/gork/internal/usecase"
)

type WorkerService struct {
	orchestratorv1.UnimplementedWorkerServiceServer
	assign       *usecase.AssignTask
	handleRes    *usecase.HandleResult
	registry     *usecase.WorkerRegistry
	queue        usecase.TaskQueue
	cancels      usecase.TaskCancelStore
	lease        time.Duration
	pollInterval time.Duration
}

func NewWorkerService(
	assign *usecase.AssignTask,
	handleResult *usecase.HandleResult,
	registry *usecase.WorkerRegistry,
	queue usecase.TaskQueue,
	cancels usecase.TaskCancelStore,
	lease time.Duration,
) *WorkerService {
	return &WorkerService{
		assign:       assign,
		handleRes:    handleResult,
		registry:     registry,
		queue:        queue,
		cancels:      cancels,
		lease:        lease,
		pollInterval: 500 * time.Millisecond,
	}
}

func (s *WorkerService) Work(stream orchestratorv1.WorkerService_WorkServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	reg := first.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "first message must be Register")
	}
	if reg.WorkerId == "" {
		return status.Error(codes.InvalidArgument, "worker_id is required")
	}

	workerID := reg.WorkerId
	s.registry.Register(&usecase.WorkerInfo{
		ID:             workerID,
		SupportedTypes: reg.SupportedTypes,
		Concurrency:    max(1, reg.Concurrency),
	})
	defer s.registry.Unregister(workerID)

	slog.Info("worker registered",
		"worker_id", workerID,
		"types", reg.SupportedTypes,
		"concurrency", reg.Concurrency,
	)

	streamCtx := stream.Context()

	// Единый канал для всех сообщений воркеру — одна writer-горутина
	sendCh := make(chan *orchestratorv1.TaskAssignment, 16)
	writerErr := make(chan error, 1)

	go func() {
		defer close(writerErr)
		for {
			select {
			case <-streamCtx.Done():
				writerErr <- streamCtx.Err()
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

	// Горутина-assigner: вытаскивает задачи из очереди и кладёт в sendCh
	go func() {
		for {
			select {
			case <-streamCtx.Done():
				return
			default:
			}

			task, err := s.assign.Execute(streamCtx, workerID)
			if err != nil {
				slog.Error("assign task failed", "worker_id", workerID, "err", err)
				time.Sleep(s.pollInterval)
				continue
			}
			if task == nil {
				time.Sleep(s.pollInterval)
				continue
			}

			assignment := &orchestratorv1.TaskAssignment{
				Msg: &orchestratorv1.TaskAssignment_Task{
					Task: taskToProto(task),
				},
			}
			select {
			case sendCh <- assignment:
				slog.Info("task assigned", "worker_id", workerID, "task_id", task.ID)
			case <-streamCtx.Done():
				return
			}
		}
	}()

	// Горутина-сторож: следит за флагами отмены running-задач
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
				w, ok := s.registry.Get(workerID)
				if !ok {
					return
				}
				for taskID := range w.RunningTasks {
					cancelled, err := s.cancels.IsCancelled(streamCtx, taskID)
					if err != nil {
						slog.Error("check cancelled failed", "task_id", taskID, "err", err)
						continue
					}
					if !cancelled {
						continue
					}
					cmd := &orchestratorv1.TaskAssignment{
						Msg: &orchestratorv1.TaskAssignment_Cancel{
							Cancel: &orchestratorv1.CancelCommand{
								TaskId: taskID,
								Reason: "cancelled by user",
							},
						},
					}
					select {
					case sendCh <- cmd:
						slog.Info("sent cancel command", "worker_id", workerID, "task_id", taskID)
					case <-streamCtx.Done():
						return
					}
				}
			}
		}
	}()

	// Основной цикл: читаем сообщения от воркера
	for {
		select {
		case err := <-writerErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		default:
		}

		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		switch m := msg.Msg.(type) {
		case *orchestratorv1.WorkerMessage_Heartbeat:
			s.registry.Heartbeat(workerID, m.Heartbeat.RunningTaskIds)
			for _, taskID := range m.Heartbeat.RunningTaskIds {
				if err := s.queue.ExtendLease(streamCtx, taskID, s.lease); err != nil {
					slog.Error("extend lease failed", "task_id", taskID, "err", err)
				}
			}
		case *orchestratorv1.WorkerMessage_Result:
			s.handleResult(streamCtx, workerID, m.Result)
		case *orchestratorv1.WorkerMessage_Register:
			// повторная регистрация — игнорируем
		}
	}
}

func (s *WorkerService) handleResult(ctx context.Context, workerID string, r *orchestratorv1.TaskResult) {
	status := domain.StatusFailed
	if r.Status == orchestratorv1.TaskStatus_TASK_STATUS_SUCCEEDED {
		status = domain.StatusSucceeded
	}

	outcome, err := s.handleRes.Execute(ctx, usecase.HandleResultInput{
		TaskID: r.TaskId,
		Status: status,
		Output: r.Output,
		Error:  r.Error,
	})
	if err != nil {
		slog.Error("handle result failed",
			"worker_id", workerID,
			"task_id", r.TaskId,
			"err", err)
		return
	}

	slog.Info("task result processed",
		"worker_id", workerID,
		"task_id", r.TaskId,
		"attempt", r.Attempt,
		"outcome", outcome)
}
