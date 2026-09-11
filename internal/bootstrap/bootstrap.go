package bootstrap

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/logger"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

func MustInitConfigAndLogger(appName string) (*config.Config, *zap.Logger) {
	cfg := config.MustLoad()

	logger := logger.MustNew(cfg.Env)

	fields := []zap.Field{
		zap.String("app", appName),
		zap.String("env", cfg.Env),
	}
	if appName == "order-service" {
		fields = append(fields,
			zap.Int("port", cfg.Port),
			zap.Int("grpcPort", cfg.GrpcPort),
		)
	}

	logger.Info("starting app", fields...)

	return cfg, logger
}

func MustInitPool(cfg *config.Config, logger *zap.Logger) *pgxpool.Pool {
	pool, err := pgxpool.New(context.Background(), cfg.Database.URL)
	if err != nil {
		log.Panicf("failed to create pool: %v", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		log.Panicf("failed to ping pool: %v", err)
	}

	return pool
}

func MustInitProducer(cfg *config.Config, logger *zap.Logger) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicOrders,
		logger,
	)
	return producer, err
}
