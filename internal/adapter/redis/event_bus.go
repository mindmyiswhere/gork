package redis

import (
	"context"
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mindmyiswhere/gork/internal/domain"
)

const eventsChannelPrefix = "task:events:"

// TaskEventBus публикует и слушает события задач через Redis Pub/Sub.
type TaskEventBus struct {
	rdb *goredis.Client
}

func NewTaskEventBus(rdb *goredis.Client) *TaskEventBus {
	return &TaskEventBus{rdb: rdb}
}

// eventPayload - то, что летит по каналу.
type eventPayload struct {
	TaskID    string `json:"task_id"`
	Status    string `json:"status"`
	Attempt   int32  `json:"attempt"`
	WorkerID  string `json:"worker_id"`
	UpdatedAt int64  `json:"updated_at"`
}

func (b *TaskEventBus) Publish(ctx context.Context, task *domain.Task) error {
	data, err := json.Marshal(eventPayload{
		TaskID:    task.ID,
		Status:    string(task.Status),
		Attempt:   task.Attempt,
		WorkerID:  task.WorkerID,
		UpdatedAt: task.UpdatedAt.UnixNano(),
	})
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	channel := eventsChannelPrefix + task.ID
	if err := b.rdb.Publish(ctx, channel, data).Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	return nil
}

// Subscribe возвращает канал событий по задаче.
func (b *TaskEventBus) Subscribe(ctx context.Context, taskID string) (<-chan *domain.Task, error) {
	sub := b.rdb.Subscribe(ctx, eventsChannelPrefix+taskID)
	out := make(chan *domain.Task, 8)

	go func() {
		defer close(out)
		defer sub.Close()

		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var p eventPayload
				if err := json.Unmarshal([]byte(msg.Payload), &p); err != nil {
					continue
				}
				select {
				case out <- payloadToTask(&p):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

func payloadToTask(p *eventPayload) *domain.Task {
	return &domain.Task{
		ID:       p.TaskID,
		Status:   domain.TaskStatus(p.Status),
		Attempt:  p.Attempt,
		WorkerID: p.WorkerID,
	}
}
