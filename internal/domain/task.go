package domain

import (
	"errors"
	"time"
)

// TaskStatus — статус задачи в жизненном цикле.
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"
	StatusRunning   TaskStatus = "RUNNING"
	StatusSucceeded TaskStatus = "SUCCEEDED"
	StatusFailed    TaskStatus = "FAILED"
	StatusCancelled TaskStatus = "CANCELLED"
)

// Task — доменная сущность задачи.
type Task struct {
	ID         string
	Type       string
	Payload    []byte
	Priority   int32
	MaxRetries int32
	Timeout    time.Duration

	Status    TaskStatus
	Attempt   int32
	WorkerID  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Ошибки домена.
var (
	ErrTaskNotFound = errors.New("task not found")
	ErrInvalidTask  = errors.New("invalid task")
)

// Validate проверяет базовые инварианты задачи.
func (t *Task) Validate() error {
	if t.Type == "" {
		return errors.Join(ErrInvalidTask, errors.New("type is required"))
	}
	if t.MaxRetries < 0 {
		return errors.Join(ErrInvalidTask, errors.New("max_retries must be >= 0"))
	}
	return nil
}

// CanTransitionTo описывает правила переходов статусов.
func (t *Task) CanTransitionTo(next TaskStatus) bool {
	switch t.Status {
	case StatusPending:
		return next == StatusRunning || next == StatusCancelled
	case StatusRunning:
		return next == StatusSucceeded || next == StatusFailed || next == StatusPending
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return false
	default:
		return false
	}
}
