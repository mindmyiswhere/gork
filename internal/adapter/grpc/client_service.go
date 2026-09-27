package grpc

import (
	"context"
	"log/slog"
	"time"

	orchestratorv1 "github.com/mindmyiswhere/gork/gen/go/orchestrator/v1"
	"github.com/mindmyiswhere/gork/internal/usecase"
)

type ClientService struct {
	orchestratorv1.UnimplementedClientServiceServer
	submitTask *usecase.SubmitTask
	getTask    *usecase.GetTask
}

func NewClientService(submitTask *usecase.SubmitTask, getTask *usecase.GetTask) *ClientService {
	return &ClientService{
		submitTask: submitTask,
		getTask:    getTask,
	}
}

func (s *ClientService) SubmitTask(ctx context.Context, req *orchestratorv1.SubmitTaskRequest) (*orchestratorv1.SubmitTaskResponse, error) {
	var timeout time.Duration
	if req.Timeout != nil {
		timeout = req.Timeout.AsDuration()
	}

	out, err := s.submitTask.Execute(ctx, usecase.SubmitTaskInput{
		Type:       req.Type,
		Payload:    req.Payload,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
		Timeout:    timeout,
	})
	if err != nil {
		slog.Error("submit task failed", "err", err)
		return nil, mapError(err)
	}

	return &orchestratorv1.SubmitTaskResponse{TaskId: out.TaskID}, nil
}

func (s *ClientService) GetTask(ctx context.Context, req *orchestratorv1.GetTaskRequest) (*orchestratorv1.TaskResult, error) {
	out, err := s.getTask.Execute(ctx, req.TaskId)
	if err != nil {
		slog.Error("get task failed", "task_id", req.TaskId, "err", err)
		return nil, mapError(err)
	}
	return taskResultFromDomain(out.Task), nil
}

func (s *ClientService) WatchTask(req *orchestratorv1.WatchTaskRequest, stream orchestratorv1.ClientService_WatchTaskServer) error {
	return stream.Send(&orchestratorv1.TaskResult{
		TaskId: req.TaskId,
		Status: orchestratorv1.TaskStatus_TASK_STATUS_PENDING,
	})
}

func (s *ClientService) CancelTask(ctx context.Context, req *orchestratorv1.CancelTaskRequest) (*orchestratorv1.CancelTaskResponse, error) {
	return &orchestratorv1.CancelTaskResponse{Cancelled: true}, nil
}
