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
	"github.com/mindmyiswhere/gork/internal/usecase"
)

type WorkerService struct {
	orchestratorv1.UnimplementedWorkerServiceServer
	assign       *usecase.AssignTask
	registry     *usecase.WorkerRegistry
	pollInterval time.Duration
}

func NewWorkerService(assign *usecase.AssignTask, registry *usecase.WorkerRegistry) *WorkerService {
	return &WorkerService{
		assign:       assign,
		registry:     registry,
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
	sendErr := make(chan error, 1)

	go func() {
		defer close(sendErr)
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

			assignment := &orchestratorv1.TaskAssignment{Task: taskToProto(task)}
			if err := stream.Send(assignment); err != nil {
				sendErr <- err
				return
			}
			slog.Info("task assigned", "worker_id", workerID, "task_id", task.ID)
		}
	}()

	for {
		select {
		case err := <-sendErr:
			if err != nil {
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
			slog.Debug("heartbeat", "worker_id", workerID, "running", len(m.Heartbeat.RunningTaskIds))
		case *orchestratorv1.WorkerMessage_Result:
			s.handleResult(streamCtx, workerID, m.Result)
		case *orchestratorv1.WorkerMessage_Register:
		}
	}
}

func (s *WorkerService) handleResult(ctx context.Context, workerID string, r *orchestratorv1.TaskResult) {
	slog.Info("task result received",
		"worker_id", workerID,
		"task_id", r.TaskId,
		"status", r.Status.String(),
		"attempt", r.Attempt,
		"err", r.Error,
	)
}
