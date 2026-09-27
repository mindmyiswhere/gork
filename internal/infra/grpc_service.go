package infra

import (
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	orchestratorv1 "github.com/mindmyiswhere/gork/gen/go/orchestrator/v1"
	grpcadapter "github.com/mindmyiswhere/gork/internal/adapter/grpc"
	"github.com/mindmyiswhere/gork/internal/usecase"
)

const DefaultTaskLease = 10 * time.Second

// NewGRPCServer собирает gRPC-сервер из готовых зависимостей.
func NewGRPCServer(
	taskRepo usecase.TaskRepository,
	taskQueue usecase.TaskQueue,
	eventBus usecase.TaskEventBus,
	cancelStore usecase.TaskCancelStore,
) *grpc.Server {
	srv := grpc.NewServer()

	registry := usecase.NewWorkerRegistry()

	submitTaskUC := usecase.NewSubmitTask(taskRepo, taskQueue)
	getTaskUC := usecase.NewGetTask(taskRepo)
	watchTaskUC := usecase.NewWatchTask(taskRepo, eventBus)
	cancelTaskUC := usecase.NewCancelTask(taskRepo, taskQueue, cancelStore, eventBus)
	assignTaskUC := usecase.NewAssignTask(taskRepo, taskQueue, registry, eventBus, DefaultTaskLease)
	handleResultUC := usecase.NewHandleResult(taskRepo, taskQueue, eventBus, cancelStore)

	orchestratorv1.RegisterClientServiceServer(srv,
		grpcadapter.NewClientService(submitTaskUC, getTaskUC, watchTaskUC, cancelTaskUC))
	orchestratorv1.RegisterWorkerServiceServer(srv,
		grpcadapter.NewWorkerService(assignTaskUC, handleResultUC, registry,
			taskQueue, cancelStore, DefaultTaskLease))

	reflection.Register(srv)
	return srv
}

func Serve(srv *grpc.Server, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	slog.Info("grpc server listening", "addr", addr)
	return srv.Serve(lis)
}
