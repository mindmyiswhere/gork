package usecase

import (
	"context"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// TaskRepository абстракция над хранилищем задач.
// Реализуется в adapter/redis.
type TaskRepository interface {
	// Create сохраняет новую задачу.
	Create(ctx context.Context, task *domain.Task) error

	// Get возвращает задачу по id. Если нет, то domain.ErrTaskNotFound.
	Get(ctx context.Context, id string) (*domain.Task, error)

	// UpdateStatus меняет статус задачи.
	UpdateStatus(ctx context.Context, id string, status domain.TaskStatus) error
}

// TaskQueue абстракция над очередью задач.
type TaskQueue interface {
	Enqueue(ctx context.Context, task *domain.Task) error
	Pop(ctx context.Context, workerID string, supportedTypes []string) (string, error)
}
