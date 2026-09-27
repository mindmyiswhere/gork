package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/mindmyiswhere/gork/internal/config"
	"github.com/mindmyiswhere/gork/internal/infra"
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

	logger := newLogger(cfg.Log)
	slog.SetDefault(logger)

	logger.Info("starting gork orchestrator",
		"grpc_addr", cfg.GRPC.Addr(),
		"redis_addr", cfg.Redis.Addr,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Redis
	rdb, err := infra.NewRedisClient(ctx, cfg.Redis)
	if err != nil {
		logger.Error("failed to connect to redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()
	logger.Info("connected to redis", "addr", cfg.Redis.Addr)

	// gRPC-сервер
	srv := infra.NewGRPCServer(rdb)

	errCh := make(chan error, 1)
	go func() {
		errCh <- infra.Serve(srv, cfg.GRPC.Addr())
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received, stopping gracefully")
		srv.GracefulStop()
		logger.Info("server stopped")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("grpc server error", "err", err)
			os.Exit(1)
		}
	}
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
