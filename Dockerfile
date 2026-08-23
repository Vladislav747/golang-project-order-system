FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /order-service ./cmd/order-service
RUN CGO_ENABLED=0 GOOS=linux go build -o /outbox-worker ./cmd/outbox-worker

FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

COPY --from=builder /order-service ./order-service
COPY --from=builder /outbox-worker ./outbox-worker
COPY config/prod.yaml ./config/prod.yaml

EXPOSE 8080 8081

ENV CONFIG_PATH=/app/config/prod.yaml

CMD ["./order-service", "--config", "/app/config/prod.yaml"]
