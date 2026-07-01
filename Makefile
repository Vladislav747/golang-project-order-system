dev-up:
	docker compose up

dev-down:
	docker compose down

build:
	go build ./cmd/order-service/main.go

local-run:
	go run ./cmd/order-service/main.go