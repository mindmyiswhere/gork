package worker

import (
	"context"
	"errors"
	"fmt"
)

// Handler — функция выполнения задачи. Принимает payload, возвращает результат.
type Handler func(ctx context.Context, payload []byte) ([]byte, error)

// HandlerRegistry хранит обработчики по типу задачи.
type HandlerRegistry struct {
	handlers map[string]Handler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[string]Handler)}
}

func (r *HandlerRegistry) Register(taskType string, h Handler) {
	r.handlers[taskType] = h
}

// Get возвращает обработчик для типа. Если нет — ошибку.
func (r *HandlerRegistry) Get(taskType string) (Handler, error) {
	h, ok := r.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("no handler for task type %q", taskType)
	}
	return h, nil
}

// Supports проверяет, есть ли обработчик. Используется при регистрации воркера.
func (r *HandlerRegistry) Supports(taskType string) bool {
	_, ok := r.handlers[taskType]
	return ok
}

// Types возвращает список типов, которые умеет воркер.
func (r *HandlerRegistry) Types() []string {
	out := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		out = append(out, t)
	}
	return out
}

// ErrNoHandler — сентинел для тестов.
var ErrNoHandler = errors.New("handler not found")
