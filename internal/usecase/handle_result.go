package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// HandleResultInput — данные о завершении задачи от воркера.
type HandleResultInput struct {
	TaskID string
	Status domain.TaskStatus
	Output []byte
	Error  string
}

// HandleResult обрабатывает результат от воркера:
//   - переводит задачу в финальный статус, если попытки кончились или успех
//   - возвращает в очередь, если есть ещё попытки
//   - публикует событие для подписчиков
type HandleResult struct {
	repo    TaskRepository
	queue   TaskQueue
	bus     TaskEventBus
	cancels TaskCancelStore
}

func NewHandleResult(repo TaskRepository, queue TaskQueue, bus TaskEventBus, cancels TaskCancelStore) *HandleResult {
	return &HandleResult{repo: repo, queue: queue, bus: bus, cancels: cancels}
}

type HandleResultOutcome string

const (
	OutcomeSucceeded HandleResultOutcome = "succeeded"
	OutcomeFailed    HandleResultOutcome = "failed"
	OutcomeRequeued  HandleResultOutcome = "requeued"
	OutcomeIgnored   HandleResultOutcome = "ignored"
)

func (uc *HandleResult) Execute(ctx context.Context, in HandleResultInput) (HandleResultOutcome, error) {
	task, err := uc.repo.Get(ctx, in.TaskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			slog.Warn("handle result: task not found, ignoring",
				"task_id", in.TaskID)
			return OutcomeIgnored, nil
		}
		return OutcomeIgnored, fmt.Errorf("get task: %w", err)
	}

	task.UpdatedAt = nowFunc()

	if in.Status != domain.StatusSucceeded && in.Status != domain.StatusFailed {
		return OutcomeIgnored, fmt.Errorf("unexpected status: %s", in.Status)
	}

	if task.Status != domain.StatusRunning {
		slog.Warn("task result for non-running task, ignoring",
			"task_id", task.ID,
			"current_status", task.Status,
			"reported_status", in.Status,
			"attempt", task.Attempt)
		return OutcomeIgnored, nil
	}

	cancelled, err := uc.cancels.IsCancelled(ctx, task.ID)
	if err != nil {
		slog.Error("check cancelled flag failed", "task_id", task.ID, "err", err)
	}
	if cancelled {
		slog.Info("task was cancelled while running, finalizing as cancelled",
			"task_id", task.ID,
			"worker_reported", in.Status)
		return OutcomeSucceeded, uc.finalize(ctx, task, domain.StatusCancelled, nil, "cancelled by user")
	}

	if in.Status == domain.StatusSucceeded {
		return OutcomeSucceeded, uc.finalize(ctx, task, domain.StatusSucceeded, in.Output, "")
	}

	if task.Attempt >= task.MaxRetries {
		return OutcomeFailed, uc.finalize(ctx, task, domain.StatusFailed, nil, in.Error)
	}

	return OutcomeRequeued, uc.retry(ctx, task, in.Error)
}

// finalize перевод в финальный статус.
func (uc *HandleResult) finalize(ctx context.Context, task *domain.Task, status domain.TaskStatus, output []byte, errMsg string) error {
	if !task.CanTransitionTo(status) {
		slog.Warn("invalid transition, ignoring",
			"task_id", task.ID,
			"from", task.Status,
			"to", status)
		return nil
	}

	task.Status = status
	if err := uc.repo.UpdateStatus(ctx, task.ID, status); err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	if err := uc.cancels.Clear(ctx, task.ID); err != nil {
		slog.Error("clear cancel flag failed", "task_id", task.ID, "err", err)
	}

	_ = uc.queue.RemoveLease(ctx, task.ID)

	if err := uc.bus.Publish(ctx, task); err != nil {
		slog.Error("publish event failed", "task_id", task.ID, "err", err)
	}
	return nil
}

// retry возврат в очередь с увеличенной попыткой.
func (uc *HandleResult) retry(ctx context.Context, task *domain.Task, lastErr string) error {
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

	_ = uc.queue.RemoveLease(ctx, task.ID)

	slog.Info("task requeued",
		"task_id", task.ID,
		"next_attempt", task.Attempt,
		"max_retries", task.MaxRetries,
		"last_error", lastErr)

	if err := uc.bus.Publish(ctx, task); err != nil {
		slog.Error("publish event failed", "task_id", task.ID, "err", err)
	}
	return nil
}
