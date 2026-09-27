package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// ReapTasks — обработка задач с истёкшим lease.
type ReapTasks struct {
	repo    TaskRepository
	queue   TaskQueue
	bus     TaskEventBus
	cancels TaskCancelStore
}

func NewReapTasks(repo TaskRepository, queue TaskQueue, bus TaskEventBus, cancels TaskCancelStore) *ReapTasks {
	return &ReapTasks{repo: repo, queue: queue, bus: bus, cancels: cancels}
}

// ReapOnce обрабатывает одну пачку задач с истёкшим lease.
// Возвращает количество обработанных задач.
func (uc *ReapTasks) ReapOnce(ctx context.Context, limit int) (int, error) {
	ids, err := uc.queue.ExpiredLeases(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("expired leases: %w", err)
	}

	reaped := 0
	for _, id := range ids {
		if err := uc.reapTask(ctx, id); err != nil {
			slog.Error("reap task failed", "task_id", id, "err", err)
			continue
		}
		reaped++
	}
	return reaped, nil
}

func (uc *ReapTasks) reapTask(ctx context.Context, taskID string) error {
	if err := uc.queue.RemoveLease(ctx, taskID); err != nil {
		return fmt.Errorf("remove lease: %w", err)
	}

	task, err := uc.repo.Get(ctx, taskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			return nil
		}
		return fmt.Errorf("get task: %w", err)
	}

	if task.Status != domain.StatusRunning {
		slog.Debug("task no longer running, skip",
			"task_id", taskID, "status", task.Status)
		return nil
	}

	cancelled, err := uc.cancels.IsCancelled(ctx, task.ID)
	if err != nil {
		slog.Error("check cancelled flag failed", "task_id", task.ID, "err", err)
	}
	if cancelled {
		return uc.finalizeCancelled(ctx, task)
	}

	slog.Warn("reaping stuck task",
		"task_id", taskID,
		"worker_id", task.WorkerID,
		"attempt", task.Attempt,
		"max_retries", task.MaxRetries)

	if task.Attempt >= task.MaxRetries {
		return uc.fail(ctx, task, "lease expired, no more retries")
	}
	return uc.requeue(ctx, task)
}

func (uc *ReapTasks) fail(ctx context.Context, task *domain.Task, reason string) error {
	task.Status = domain.StatusFailed
	if err := uc.repo.UpdateStatus(ctx, task.ID, domain.StatusFailed); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if err := uc.repo.UpdateWorkerID(ctx, task.ID, ""); err != nil {
		return fmt.Errorf("clear worker id: %w", err)
	}
	if err := uc.bus.Publish(ctx, task); err != nil {
		slog.Error("publish event failed", "task_id", task.ID, "err", err)
	}
	slog.Info("task marked failed after lease expiry",
		"task_id", task.ID,
		"reason", reason)
	return nil
}

func (uc *ReapTasks) requeue(ctx context.Context, task *domain.Task) error {
	task.Attempt++
	task.Status = domain.StatusPending
	task.WorkerID = ""

	if err := uc.repo.UpdateStatus(ctx, task.ID, domain.StatusPending); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if err := uc.repo.UpdateWorkerID(ctx, task.ID, ""); err != nil {
		return fmt.Errorf("clear worker id: %w", err)
	}
	if err := uc.repo.UpdateAttempt(ctx, task.ID, task.Attempt); err != nil {
		return fmt.Errorf("update attempt: %w", err)
	}
	if err := uc.queue.Enqueue(ctx, task); err != nil {
		return fmt.Errorf("re-enqueue: %w", err)
	}
	if err := uc.bus.Publish(ctx, task); err != nil {
		slog.Error("publish event failed", "task_id", task.ID, "err", err)
	}

	slog.Info("task requeued after lease expiry",
		"task_id", task.ID,
		"attempt", task.Attempt,
		"max_retries", task.MaxRetries)
	return nil
}

func (uc *ReapTasks) finalizeCancelled(ctx context.Context, task *domain.Task) error {
	task.Status = domain.StatusCancelled
	if err := uc.repo.UpdateStatus(ctx, task.ID, domain.StatusCancelled); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if err := uc.repo.UpdateWorkerID(ctx, task.ID, ""); err != nil {
		return fmt.Errorf("clear worker id: %w", err)
	}
	if err := uc.cancels.Clear(ctx, task.ID); err != nil {
		slog.Error("clear cancel flag failed", "task_id", task.ID, "err", err)
	}
	if err := uc.bus.Publish(ctx, task); err != nil {
		slog.Error("publish cancelled event failed", "task_id", task.ID, "err", err)
	}
	slog.Info("task finalized as cancelled by reaper", "task_id", task.ID)
	return nil
}
