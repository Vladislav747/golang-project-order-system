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
