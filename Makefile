# PHONY - тут игнорирует ошибки
.PHONY: migrate dev-up dev-down prod-up prod-down build local-run rebuild-go-app-docker docker-compose-exec-postgres-psql test-integration service-test

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

rebuild-go-app-docker:
	docker compose up --build go-app

docker-compose-exec-postgres-psql:
	docker compose exec postgres psql -U orders -d orders -c "\dt"


# Integration-тесты (файлы с //go:build integration).
test-integration:
	go test ./... -count=1 -v -tags=integration

service-test:
	go test ./internal/service/ -v

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