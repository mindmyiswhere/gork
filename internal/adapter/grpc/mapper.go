package grpc

import (
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestratorv1 "github.com/mindmyiswhere/gork/gen/go/orchestrator/v1"
	"github.com/mindmyiswhere/gork/internal/domain"
)

// taskStatusToProto превращает доменный статус в proto-enum.
func taskStatusToProto(s domain.TaskStatus) orchestratorv1.TaskStatus {
	switch s {
	case domain.StatusPending:
		return orchestratorv1.TaskStatus_TASK_STATUS_PENDING
	case domain.StatusRunning:
		return orchestratorv1.TaskStatus_TASK_STATUS_RUNNING
	case domain.StatusSucceeded:
		return orchestratorv1.TaskStatus_TASK_STATUS_SUCCEEDED
	case domain.StatusFailed:
		return orchestratorv1.TaskStatus_TASK_STATUS_FAILED
	case domain.StatusCancelled:
		return orchestratorv1.TaskStatus_TASK_STATUS_CANCELLED
	default:
		return orchestratorv1.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

// taskToProto собирает proto-Task из домена.
func taskToProto(t *domain.Task) *orchestratorv1.Task {
	task := &orchestratorv1.Task{
		Id:         t.ID,
		Type:       t.Type,
		Payload:    t.Payload,
		Priority:   t.Priority,
		MaxRetries: t.MaxRetries,
		CreatedAt:  timestamppb.New(t.CreatedAt),
	}
	if t.Timeout > 0 {
		task.Timeout = durationpb.New(t.Timeout)
	}
	return task
}

// taskResultFromDomain собирает proto-TaskResult.
func taskResultFromDomain(t *domain.Task) *orchestratorv1.TaskResult {
	return &orchestratorv1.TaskResult{
		TaskId:  t.ID,
		Status:  taskStatusToProto(t.Status),
		Attempt: t.Attempt,
	}
}

// durationFromProto безопасно достаёт time.Duration из proto.
func durationFromProto(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

// mapError превращает доменные ошибки в gRPC-статусы.
func mapError(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidTask):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrTaskNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
