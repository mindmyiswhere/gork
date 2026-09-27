package usecase

import (
	"context"
	"errors"
	"fmt"

	"github.com/mindmyiswhere/gork/internal/domain"
)

type AssignTask struct {
	repo     TaskRepository
	queue    TaskQueue
	registry *WorkerRegistry
}

func NewAssignTask(repo TaskRepository, queue TaskQueue, registry *WorkerRegistry) *AssignTask {
	return &AssignTask{repo: repo, queue: queue, registry: registry}
}

// Execute пытается выдать одну задачу указанному воркеру.
// Возвращает (nil, nil), если задач нет или воркер занят полностью.
func (uc *AssignTask) Execute(ctx context.Context, workerID string) (*domain.Task, error) {
	w, ok := uc.registry.Get(workerID)
	if !ok {
		return nil, fmt.Errorf("worker %s not registered", workerID)
	}
	if int32(len(w.RunningTasks)) >= w.Concurrency {
		return nil, nil
	}

	taskID, err := uc.queue.Pop(ctx, workerID, w.SupportedTypes)
	if err != nil {
		return nil, fmt.Errorf("pop from queue: %w", err)
	}
	if taskID == "" {
		return nil, nil
	}

	task, err := uc.repo.Get(ctx, taskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get task: %w", err)
	}

	task.Status = domain.StatusRunning
	task.WorkerID = workerID
	return task, nil
}
