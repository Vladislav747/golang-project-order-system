# PHONY - тут игнорирует ошибки
.PHONY: migrate dev-up dev-down build local-run rebuild-go-app-docker docker-compose-exec-postgres-psql

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

build:
	go build ./cmd/order-service/main.go

local-run:
	go run ./cmd/order-service/main.go

rebuild-go-app-docker:
	docker compose up --build go-app

docker-compose-exec-postgres-psql:
	docker compose exec postgres psql -U orders -d orders -c "\dt"

service-test:
	go test ./internal/service/ -v

lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix
