package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// SubmitTaskInput входные данные use case.
type SubmitTaskInput struct {
	Type       string
	Payload    []byte
	Priority   int32
	MaxRetries int32
	Timeout    time.Duration
}

// SubmitTaskOutput результат use case.
type SubmitTaskOutput struct {
	TaskID string
}

// SubmitTask сценарий создания новой задачи.
type SubmitTask struct {
	repo  TaskRepository
	queue TaskQueue
	now   func() time.Time
}

func NewSubmitTask(repo TaskRepository, queue TaskQueue) *SubmitTask {
	return &SubmitTask{
		repo:  repo,
		queue: queue,
		now:   time.Now,
	}
}

func (uc *SubmitTask) Execute(ctx context.Context, in SubmitTaskInput) (*SubmitTaskOutput, error) {
	task := &domain.Task{
		ID:         uuid.NewString(),
		Type:       in.Type,
		Payload:    in.Payload,
		Priority:   in.Priority,
		MaxRetries: in.MaxRetries,
		Timeout:    in.Timeout,
		Status:     domain.StatusPending,
		Attempt:    1,
		CreatedAt:  uc.now(),
		UpdatedAt:  uc.now(),
	}

	if err := task.Validate(); err != nil {
		return nil, err
	}

	if err := uc.repo.Create(ctx, task); err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}

	if err := uc.queue.Enqueue(ctx, task); err != nil {
		return nil, fmt.Errorf("enqueue task: %w", err)
	}

	return &SubmitTaskOutput{TaskID: task.ID}, nil
}
