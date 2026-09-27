package infra

import (
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	orchestratorv1 "github.com/mindmyiswhere/gork/gen/go/orchestrator/v1"
	grpcadapter "github.com/mindmyiswhere/gork/internal/adapter/grpc"
	redisadapter "github.com/mindmyiswhere/gork/internal/adapter/redis"
	"github.com/mindmyiswhere/gork/internal/usecase"
	"github.com/redis/go-redis/v9"
)

// NewGRPCServer собирает gRPC-сервер со всеми зарегистрированными сервисами.
func NewGRPCServer(rdb *redis.Client) *grpc.Server {
	srv := grpc.NewServer()

	taskRepo := redisadapter.NewTaskRepository(rdb)
	taskQueue := redisadapter.NewTaskQueue(rdb)
	registry := usecase.NewWorkerRegistry()

	submitTaskUC := usecase.NewSubmitTask(taskRepo, taskQueue)
	getTaskUC := usecase.NewGetTask(taskRepo)
	assignTaskUC := usecase.NewAssignTask(taskRepo, taskQueue, registry)

	orchestratorv1.RegisterClientServiceServer(srv,
		grpcadapter.NewClientService(submitTaskUC, getTaskUC))
	orchestratorv1.RegisterWorkerServiceServer(srv,
		grpcadapter.NewWorkerService(assignTaskUC, registry))

	reflection.Register(srv)
	return srv
}

// Serve запускает сервер на указанном адресе и блокируется до остановки.
func Serve(srv *grpc.Server, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	slog.Info("grpc server listening", "addr", addr)
	return srv.Serve(lis)
}
