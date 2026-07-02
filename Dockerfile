FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /order-service ./cmd/order-service

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /order-service ./order-service
COPY config/prod.yaml ./config/prod.yaml

EXPOSE 8082

ENV CONFIG_PATH=/app/config/prod.yaml

CMD ["./order-service", "--config", "/app/config/prod.yaml"]
