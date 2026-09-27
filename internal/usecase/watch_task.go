package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// WatchTask — подписка на события задачи.
type WatchTask struct {
	repo TaskRepository
	bus  TaskEventBus
}

func NewWatchTask(repo TaskRepository, bus TaskEventBus) *WatchTask {
	return &WatchTask{repo: repo, bus: bus}
}

// Execute возвращает:
//   - текущее состояние задачи (сразу)
//   - канал событий (может быть nil, если задача уже финальная)
//
// Если задача финальная — канал nil, и клиент должен сразу закрыть стрим.
func (uc *WatchTask) Execute(ctx context.Context, taskID string) (*domain.Task, <-chan *domain.Task, error) {
	if taskID == "" {
		return nil, nil, fmt.Errorf("%w: id is required", domain.ErrInvalidTask)
	}

	task, err := uc.repo.Get(ctx, taskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			return nil, nil, domain.ErrTaskNotFound
		}
		return nil, nil, fmt.Errorf("get task: %w", err)
	}

	if isFinal(task.Status) {
		return task, nil, nil
	}

	events, err := uc.bus.Subscribe(ctx, taskID)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe: %w", err)
	}

	task, err = uc.repo.Get(ctx, taskID)
	if err != nil {
		slog.Warn("re-read task after subscribe failed", "task_id", taskID, "err", err)
	}
	if task != nil && isFinal(task.Status) {
		return task, nil, nil
	}

	return task, events, nil
}

func isFinal(s domain.TaskStatus) bool {
	switch s {
	case domain.StatusSucceeded, domain.StatusFailed, domain.StatusCancelled:
		return true
	default:
		return false
	}
}
