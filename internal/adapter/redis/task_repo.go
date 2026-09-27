package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mindmyiswhere/gork/internal/domain"
)

// TaskRepository реализует usecase.TaskRepository поверх Redis.
type TaskRepository struct {
	rdb *goredis.Client
}

func NewTaskRepository(rdb *goredis.Client) *TaskRepository {
	return &TaskRepository{rdb: rdb}
}

// taskKey имя хэша для задачи.
func taskKey(id string) string {
	return "task:" + id
}

func (r *TaskRepository) Create(ctx context.Context, task *domain.Task) error {
	pipe := r.rdb.TxPipeline()

	pipe.HSet(ctx, taskKey(task.ID), map[string]any{
		"id":          task.ID,
		"type":        task.Type,
		"payload":     task.Payload,
		"priority":    task.Priority,
		"max_retries": task.MaxRetries,
		"timeout_ns":  int64(task.Timeout),
		"status":      string(task.Status),
		"attempt":     task.Attempt,
		"worker_id":   task.WorkerID,
		"created_at":  task.CreatedAt.UnixNano(),
		"updated_at":  task.UpdatedAt.UnixNano(),
	})
	pipe.SAdd(ctx, "tasks:pending", task.ID)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis hset: %w", err)
	}
	return nil
}

func (r *TaskRepository) Get(ctx context.Context, id string) (*domain.Task, error) {
	data, err := r.rdb.HGetAll(ctx, taskKey(id)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis hgetall: %w", err)
	}
	if len(data) == 0 {
		return nil, domain.ErrTaskNotFound
	}
	return unmarshalTask(data)
}

func (r *TaskRepository) UpdateStatus(ctx context.Context, id string, status domain.TaskStatus) error {
	pipe := r.rdb.TxPipeline()
	pipe.HSet(ctx, taskKey(id), "status", string(status), "updated_at", time.Now().UnixNano())

	pipe.SRem(ctx, "tasks:pending", id)
	pipe.SRem(ctx, "tasks:running", id)
	pipe.SRem(ctx, "tasks:done", id)
	switch status {
	case domain.StatusPending:
		pipe.SAdd(ctx, "tasks:pending", id)
	case domain.StatusRunning:
		pipe.SAdd(ctx, "tasks:running", id)
	case domain.StatusSucceeded, domain.StatusFailed, domain.StatusCancelled:
		pipe.SAdd(ctx, "tasks:done", id)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis update status: %w", err)
	}
	return nil
}

// unmarshalTask превращает поля из HGetAll в доменную структуру.
func unmarshalTask(data map[string]string) (*domain.Task, error) {
	t := &domain.Task{
		ID:       data["id"],
		Type:     data["type"],
		Payload:  []byte(data["payload"]),
		Status:   domain.TaskStatus(data["status"]),
		WorkerID: data["worker_id"],
	}

	if v, err := parseInt32(data["priority"]); err == nil {
		t.Priority = v
	}
	if v, err := parseInt32(data["max_retries"]); err == nil {
		t.MaxRetries = v
	}
	if v, err := parseInt32(data["attempt"]); err == nil {
		t.Attempt = v
	}
	if v, err := parseInt64(data["timeout_ns"]); err == nil {
		t.Timeout = time.Duration(v)
	}
	if v, err := parseInt64(data["created_at"]); err == nil {
		t.CreatedAt = time.Unix(0, v)
	}
	if v, err := parseInt64(data["updated_at"]); err == nil {
		t.UpdatedAt = time.Unix(0, v)
	}

	if t.ID == "" {
		return nil, domain.ErrTaskNotFound
	}
	return t, nil
}

func parseInt32(s string) (int32, error) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return int32(v), err
}

func parseInt64(s string) (int64, error) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}

func (r *TaskRepository) UpdateAttempt(ctx context.Context, id string, attempt int32) error {
	if err := r.rdb.HSet(ctx, taskKey(id), "attempt", attempt).Err(); err != nil {
		return fmt.Errorf("redis hset attempt: %w", err)
	}
	return nil
}

func (r *TaskRepository) UpdateWorkerID(ctx context.Context, id, workerID string) error {
	if err := r.rdb.HSet(ctx, taskKey(id), "worker_id", workerID).Err(); err != nil {
		return fmt.Errorf("redis hset worker_id: %w", err)
	}
	return nil
}
