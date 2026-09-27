package redis

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

const cancelKeyPrefix = "task:cancelled:"

// TaskCancelStore реализует usecase.TaskCancelStore поверх Redis.
type TaskCancelStore struct {
	rdb *goredis.Client
}

func NewTaskCancelStore(rdb *goredis.Client) *TaskCancelStore {
	return &TaskCancelStore{rdb: rdb}
}

func (s *TaskCancelStore) MarkCancelled(ctx context.Context, taskID string) error {
	if err := s.rdb.Set(ctx, cancelKeyPrefix+taskID, "1", 0).Err(); err != nil {
		return fmt.Errorf("redis set cancelled: %w", err)
	}
	return nil
}

func (s *TaskCancelStore) IsCancelled(ctx context.Context, taskID string) (bool, error) {
	_, err := s.rdb.Get(ctx, cancelKeyPrefix+taskID).Result()
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redis get cancelled: %w", err)
	}
	return true, nil
}

func (s *TaskCancelStore) Clear(ctx context.Context, taskID string) error {
	if err := s.rdb.Del(ctx, cancelKeyPrefix+taskID).Err(); err != nil {
		return fmt.Errorf("redis del cancelled: %w", err)
	}
	return nil
}
