package redis

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mindmyiswhere/gork/internal/domain"
)

const queueKey = "tasks:queue"

//go:embed scripts/pop_task.lua
var popTaskScript string

var popTask = goredis.NewScript(popTaskScript)

type TaskQueue struct {
	rdb *goredis.Client
}

func NewTaskQueue(rdb *goredis.Client) *TaskQueue {
	return &TaskQueue{rdb: rdb}
}

func (q *TaskQueue) Enqueue(ctx context.Context, task *domain.Task) error {
	score := float64(1000-task.Priority)*1e13 + float64(task.CreatedAt.UnixNano())
	if err := q.rdb.ZAdd(ctx, queueKey, goredis.Z{
		Score:  score,
		Member: task.ID,
	}).Err(); err != nil {
		return fmt.Errorf("redis zadd: %w", err)
	}
	return nil
}

// Pop атомарно забирает задачу из очереди для воркера.
// Если подходящей задачи нет — возвращает ("", nil).
func (q *TaskQueue) Pop(ctx context.Context, workerID string, supportedTypes []string) (string, error) {
	res, err := popTask.Run(ctx, q.rdb,
		[]string{queueKey},
		workerID,
		time.Now().UnixNano(),
		strings.Join(supportedTypes, ","),
	).Result()

	if err == goredis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("pop task: %w", err)
	}

	id, ok := res.(string)
	if !ok {
		return "", nil
	}
	return id, nil
}
