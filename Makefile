.PHONY: proto build lint test \
        run-orch run-worker \
        up-gork down-gork \
        redis-up redis-down

# Кодогенерация
proto:
	protoc \
		--proto_path=proto \
		--go_out=gen/go --go_opt=paths=source_relative \
		--go-grpc_out=gen/go --go-grpc_opt=paths=source_relative \
		proto/orchestrator/v1/orchestrator.proto

# Сборка и проверки
build: proto
	go build -o bin/orchestrator ./cmd/orchestrator
	go build -o bin/worker ./cmd/worker

lint:
	golangci-lint run ./...

test:
	go test ./...

# Локальный запуск (в отдельных терминалах)

run-orch: build
	./bin/orchestrator

run-worker: build
	./bin/worker

redis-up:
	docker compose up -d redis

redis-down:
	docker compose down redis