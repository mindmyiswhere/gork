package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/mindmyiswhere/gork/internal/config"
	"github.com/mindmyiswhere/gork/internal/worker"
)

func main() {
	if err := godotenv.Load(); err != nil {
		_ = err
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	logger := newLogger(cfg.Log) // тот же хелпер, что в main оркестратора
	slog.SetDefault(logger)

	logger.Info("starting gork worker",
		"orchestrator", cfg.Worker.OrchestratorAddr,
		"concurrency", cfg.Worker.Concurrency,
		"types", cfg.Worker.SupportedTypes,
	)

	registry := worker.NewHandlerRegistry()
	registry.Register("send_email", worker.HandleSendEmail)
	registry.Register("resize_image", worker.HandleResizeImage)
	registry.Register("generate_report", worker.HandleGenerateReport)

	w := worker.New(cfg.Worker, registry, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := w.Run(ctx); err != nil {
		logger.Error("worker stopped with error", "err", err)
		os.Exit(1)
	}
	logger.Info("worker stopped gracefully")
}

func newLogger(cfg config.LogConfig) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}
