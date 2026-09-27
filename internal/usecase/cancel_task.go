package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// CancelTaskOutput — результат попытки отмены.
type CancelTaskOutput struct {
	Cancelled bool
}

// CancelTask — сценарий отмены задачи клиентом.
type CancelTask struct {
	repo    TaskRepository
	queue   TaskQueue
	cancels TaskCancelStore
	bus     TaskEventBus
}

func NewCancelTask(
	repo TaskRepository,
	queue TaskQueue,
	cancels TaskCancelStore,
	bus TaskEventBus,
) *CancelTask {
	return &CancelTask{repo: repo, queue: queue, cancels: cancels, bus: bus}
}

func (uc *CancelTask) Execute(ctx context.Context, taskID string) (*CancelTaskOutput, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: id is required", domain.ErrInvalidTask)
	}

	task, err := uc.repo.Get(ctx, taskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			return nil, domain.ErrTaskNotFound
		}
		return nil, fmt.Errorf("get task: %w", err)
	}

	switch task.Status {
	case domain.StatusPending:
		return uc.cancelPending(ctx, task)

	case domain.StatusRunning:
		return uc.cancelRunning(ctx, task)

	case domain.StatusSucceeded, domain.StatusFailed:
		return &CancelTaskOutput{Cancelled: false}, nil

	case domain.StatusCancelled:
		return &CancelTaskOutput{Cancelled: true}, nil

	default:
		return nil, fmt.Errorf("unknown task status: %s", task.Status)
	}
}

// cancelPending — задача ещё в очереди, отменяем сразу.
func (uc *CancelTask) cancelPending(ctx context.Context, task *domain.Task) (*CancelTaskOutput, error) {
	if err := uc.queue.Remove(ctx, task.ID); err != nil {
		return nil, fmt.Errorf("remove from queue: %w", err)
	}
	if err := uc.repo.UpdateStatus(ctx, task.ID, domain.StatusCancelled); err != nil {
		return nil, fmt.Errorf("update status: %w", err)
	}
	if err := uc.repo.UpdateWorkerID(ctx, task.ID, ""); err != nil {
		return nil, fmt.Errorf("clear worker id: %w", err)
	}

	task.Status = domain.StatusCancelled
	if err := uc.bus.Publish(ctx, task); err != nil {
		slog.Error("publish cancelled event failed", "task_id", task.ID, "err", err)
	}

	slog.Info("task cancelled while pending", "task_id", task.ID)
	return &CancelTaskOutput{Cancelled: true}, nil
}

// cancelRunning — задача выполняется. Ставим флаг, воркер прервёт сам.
func (uc *CancelTask) cancelRunning(ctx context.Context, task *domain.Task) (*CancelTaskOutput, error) {
	if err := uc.cancels.MarkCancelled(ctx, task.ID); err != nil {
		return nil, fmt.Errorf("mark cancelled: %w", err)
	}

	slog.Info("task marked for cancellation while running",
		"task_id", task.ID,
		"worker_id", task.WorkerID)
	return &CancelTaskOutput{Cancelled: true}, nil
}
