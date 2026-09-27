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

const (
	queueKey  = "tasks:queue"
	leasesKey = "leases"
)

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
func (q *TaskQueue) Pop(ctx context.Context, workerID string, supportedTypes []string, lease time.Duration) (string, error) {
	now := time.Now()
	deadline := now.Add(lease).UnixNano()

	res, err := popTask.Run(ctx, q.rdb,
		[]string{queueKey, leasesKey},
		workerID,
		now.UnixNano(),
		deadline,
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

// ExtendLease продлевает аренду задачи. Вызывается по heartbeat от воркера.
func (q *TaskQueue) ExtendLease(ctx context.Context, taskID string, lease time.Duration) error {
	deadline := time.Now().Add(lease).UnixNano()
	if err := q.rdb.ZAdd(ctx, leasesKey, goredis.Z{
		Score:  float64(deadline),
		Member: taskID,
	}).Err(); err != nil {
		return fmt.Errorf("extend lease: %w", err)
	}
	if err := q.rdb.HSet(ctx, "task:"+taskID, "lease_deadline", deadline).Err(); err != nil {
		return fmt.Errorf("update lease deadline: %w", err)
	}
	return nil
}

// ExpiredLeases возвращает task_id, у которых lease истёк.
func (q *TaskQueue) ExpiredLeases(ctx context.Context, limit int) ([]string, error) {
	now := time.Now().UnixNano()
	ids, err := q.rdb.ZRangeByScore(ctx, leasesKey, &goredis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", now),
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore: %w", err)
	}
	return ids, nil
}

// RemoveLease снимает задачу с учёта аренды.
func (q *TaskQueue) RemoveLease(ctx context.Context, taskID string) error {
	pipe := q.rdb.TxPipeline()
	pipe.ZRem(ctx, leasesKey, taskID)
	pipe.HDel(ctx, "task:"+taskID, "lease_deadline")
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("remove lease: %w", err)
	}
	return nil
}

// Remove убирает задачу из очереди. Если её там нет — не ошибка.
func (q *TaskQueue) Remove(ctx context.Context, taskID string) error {
	if err := q.rdb.ZRem(ctx, queueKey, taskID).Err(); err != nil {
		return fmt.Errorf("zrem from queue: %w", err)
	}
	return nil
}
