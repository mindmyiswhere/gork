package infra

import (
	"context"
	"log/slog"
	"time"

	"github.com/mindmyiswhere/gork/internal/usecase"
)

// RunReaper запускает фоновый цикл обработки зависших задач.
// Блокируется до отмены ctx.
func RunReaper(ctx context.Context, uc *usecase.ReapTasks, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("reaper started", "interval", interval, "batch_size", batchSize)

	for {
		select {
		case <-ctx.Done():
			slog.Info("reaper stopped")
			return
		case <-ticker.C:
			reaped, err := uc.ReapOnce(ctx, batchSize)
			if err != nil {
				slog.Error("reaper iteration failed", "err", err)
				continue
			}
			if reaped > 0 {
				slog.Info("reaper processed tasks", "count", reaped)
			}
		}
	}
}
