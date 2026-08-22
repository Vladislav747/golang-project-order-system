# PHONY - тут игнорирует ошибки
.PHONY: migrate dev-up dev-down prod-up prod-down build local-run rebuild-go-app-docker docker-compose-exec-postgres-psql test-integration service-test load-k6 load-k6-smoke

DATABASE_URL ?= postgres://orders:orders@localhost:5432/orders?sslmode=disable

migrate:
	docker compose exec -T postgres psql -U orders -d orders -f /docker-entrypoint-initdb.d/init.sql

check-queue-kafka:
	docker exec -it order-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.create \
  --from-beginning \
  --group debug-viewer

dev-up:
	docker-compose up

dev-down:
	docker-compose down

prod-up:
	docker compose -f docker-compose.prod.yml up -d --build

prod-down:
	docker compose -f docker-compose.prod.yml down

build:
	go build ./cmd/order-service/main.go

local-run:
	go run ./cmd/order-service/main.go

local-run-outbox:
	go run ./cmd/outbox-worker/main.go

rebuild-go-app-docker:
	docker compose up --build go-app

docker-compose-exec-postgres-psql:
	docker compose exec postgres psql -U orders -d orders -c "\dt"


# Integration-тесты (файлы с //go:build integration).
test-integration:
	go test ./... -v -tags=integration

# E2E — black-box против уже запущенного сервиса (http://127.0.0.1:8080).
# Sync: config/local.yaml (mode: sync) + make local-run && make test-e2e
# Async: docker compose up -d --build (prod.yaml mode: async) && make test-e2e-async
E2E_BASE_URL ?= http://127.0.0.1:8080

test-e2e:
	E2E_BASE_URL=$(E2E_BASE_URL) go test ./tests/e2e/ -count=1 -v -tags=e2e

test-e2e-async:
	E2E_BASE_URL=$(E2E_BASE_URL) go test ./tests/e2e/ -count=1 -v -tags=e2e_async

service-test:
	go test ./internal/service/ -v

handler-test:
	go test ./internal/handler/order/ -v

# Load-тест HTTP (нужен запущенный сервис на BASE_URL).
# Пример: make local-run  →  make load-k6-smoke
BASE_URL ?= http://127.0.0.1:8080

load-k6-smoke:
	BASE_URL=$(BASE_URL) k6 run --vus 5 --duration 20s scripts/load/orders.js

load-k6:
	BASE_URL=$(BASE_URL) k6 run --stage 20s:10 --stage 40s:30 --stage 20s:0 scripts/load/orders.js

# Только POST /order (нужен wrk: brew install wrk)
load-wrk-create:
	wrk -t4 -c50 -d30s -s scripts/load/create_order.lua $(BASE_URL)

lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix

migrate-up:
	goose up

migrate-down:
	goose down

migrate-status:
	goose status

generate-mocks:
	go tool mockery

deploy-prod:
	docker compose -f docker-compose.prod.yml up -d --build

deploy-prod-down:
	docker compose -f docker-compose.prod.yml down

deploy-prod-logs:
	docker compose -f docker-compose.prod.yml logs -f go-app

make-grpc-proto:
	PATH="$$(go env GOPATH)/bin:$$PATH" protoc \
		-I internal/api \
		-I "$$(brew --prefix)/include" \
		--go_out=internal/pkg/api --go_opt=paths=source_relative \
		--go-grpc_out=internal/pkg/api --go-grpc_opt=paths=source_relative \
		order/v1/order.proto \
		order_event/v1/order_event.proto