package usecase

import (
	"context"
	"fmt"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// GetTaskOutput — результат сценария.
type GetTaskOutput struct {
	Task *domain.Task
}

// GetTask — сценарий чтения задачи по id.
type GetTask struct {
	repo TaskRepository
}

func NewGetTask(repo TaskRepository) *GetTask {
	return &GetTask{repo: repo}
}

func (uc *GetTask) Execute(ctx context.Context, taskID string) (*GetTaskOutput, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: id is required", domain.ErrInvalidTask)
	}

	task, err := uc.repo.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}
	return &GetTaskOutput{Task: task}, nil
}
