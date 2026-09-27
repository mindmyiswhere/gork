package usecase

import (
	"context"
	"time"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// TaskRepository абстракция над хранилищем задач.
// Реализуется в adapter/redis.
type TaskRepository interface {
	Create(ctx context.Context, task *domain.Task) error
	Get(ctx context.Context, id string) (*domain.Task, error)
	UpdateStatus(ctx context.Context, id string, status domain.TaskStatus) error
	UpdateAttempt(ctx context.Context, id string, attempt int32) error
	UpdateWorkerID(ctx context.Context, id, workerID string) error
}

// TaskQueue абстракция над очередью задач.
type TaskQueue interface {
	Enqueue(ctx context.Context, task *domain.Task) error
	Pop(ctx context.Context, workerID string, supportedTypes []string, lease time.Duration) (string, error)
	ExtendLease(ctx context.Context, taskID string, lease time.Duration) error
	ExpiredLeases(ctx context.Context, limit int) ([]string, error)
	RemoveLease(ctx context.Context, taskID string) error
	Remove(ctx context.Context, taskID string) error
}

var nowFunc = time.Now

// TaskEventBus — публикация событий о задачах.
type TaskEventBus interface {
	Publish(ctx context.Context, task *domain.Task) error
	Subscribe(ctx context.Context, taskID string) (<-chan *domain.Task, error)
}

type TaskCancelStore interface {
	MarkCancelled(ctx context.Context, taskID string) error
	IsCancelled(ctx context.Context, taskID string) (bool, error)
	Clear(ctx context.Context, taskID string) error
}
