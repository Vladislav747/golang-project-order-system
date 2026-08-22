package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/logger"
	repositoryOutbox "github.com/Vladislav747/golang-project-order-system/internal/repository/outbox"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
	"github.com/Vladislav747/golang-project-order-system/internal/worker"
)

func main() {
	cfg, logger := mustInitConfigAndLogger()

	pool := mustInitPool(cfg, logger)

	defer pool.Close()

	producer, err := mustInitProducer(cfg, logger)
	if err != nil {
		log.Panicf("failed to create producer %v", zap.Error(err))
	}

	relay, relayCancel, relayWG := mustStartOutboxRelay(cfg, pool, producer, logger)
	cleaner, cleanerCancel, cleanerWG := mustStartOutboxCleaner(cfg, pool, logger)

	provider := config.NewProvider(cfg)
	watchCtx, watchCancel := context.WithCancel(context.Background())

	go func() {
		path := os.Getenv("CONFIG_PATH")
		if path == "" {
			logger.Error("CONFIG_PATH is not set")
			return
		}
		onReload := func(newCfg *config.Config) {
			relay.SetInterval(newCfg.Outbox.RelayInterval)
			relay.SetLimit(newCfg.Outbox.Limit)
			relay.SetMaxAttempts(newCfg.Outbox.MaxAttempts)
			cleaner.SetInterval(newCfg.Outbox.CleanupInterval)
			cleaner.SetRetention(newCfg.Outbox.PublishedRetention)
			cleaner.SetMaxAttempts(newCfg.Outbox.MaxAttempts)
		}
		if err := provider.StartWatch(watchCtx, path, logger, onReload); err != nil {
			logger.Error("config watch stopped", zap.Error(err))
		}
	}()

	// Запускаем graceful shutdown
	gracefulShutdown(
		logger,
		producer,
		provider,
		watchCancel,
		relayCancel,
		cleanerCancel,
		relayWG,
		cleanerWG,
	)
}

func mustStartOutboxRelay(cfg *config.Config, pool *pgxpool.Pool, producer *kafka.Producer, logger *zap.Logger) (*worker.OutboxRelay, context.CancelFunc, *sync.WaitGroup) {
	repositoryOutbox := repositoryOutbox.NewRepository(pool, logger)
	relay := worker.NewOutboxRelay(
		repositoryOutbox,
		producer,
		logger,
		cfg.Outbox.RelayInterval,
		cfg.Outbox.Limit,
		cfg.Outbox.MaxAttempts,
		pool, // TxManager
	)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()
	return relay, cancel, &wg
}

// Дубль - убрать при рефакторинге
func mustInitConfigAndLogger() (*config.Config, *zap.Logger) {
	cfg := config.MustLoad()

	logger := logger.MustNew(cfg.Env)

	logger.Info("starting outbox worker")

	return cfg, logger
}

// Дубль - убрать при рефакторинге
func mustInitPool(cfg *config.Config, logger *zap.Logger) *pgxpool.Pool {
	pool, err := pgxpool.New(context.Background(), cfg.Database.URL)
	if err != nil {
		log.Panicf("failed to create pool: %v", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		log.Panicf("failed to ping pool: %v", err)
	}

	return pool
}

// Дубль - убрать при рефакторинге
func mustInitProducer(cfg *config.Config, logger *zap.Logger) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicOrders,
		logger,
	)
	return producer, err
}

func mustStartOutboxCleaner(cfg *config.Config, pool *pgxpool.Pool, logger *zap.Logger) (*worker.OutboxCleaner, context.CancelFunc, *sync.WaitGroup) {
	repo := repositoryOutbox.NewRepository(pool, logger)
	cleaner := worker.NewOutboxCleaner(
		repo,
		logger,
		cfg.Outbox.CleanupInterval,
		cfg.Outbox.PublishedRetention,
		cfg.Outbox.MaxAttempts,
		pool,
	)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		cleaner.Run(ctx)
	}()
	return cleaner, cancel, &wg
}

func gracefulShutdown(
	logger *zap.Logger,
	producer *kafka.Producer,
	provider *config.Provider,
	watchCancel context.CancelFunc,
	outboxCancel context.CancelFunc,
	outboxCleanerCancel context.CancelFunc,
	outboxWG *sync.WaitGroup,
	outboxCleanerWG *sync.WaitGroup,
) {
	// Ждем сигналы прерывания
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	logger.Info("shutting down outbox relay")
	outboxCancel()
	// горутина точно завершилась - relay.Run завершится только после завершения работы relay
	outboxWG.Wait()

	logger.Info("shutting down outbox cleaner")
	outboxCleanerCancel()
	// горутина точно завершилась - cleaner.Run завершится только после завершения работы cleaner
	outboxCleanerWG.Wait()

	logger.Info("shutting down producer")
	// Закрываем producer
	if err := producer.Close(); err != nil {
		logger.Error("producer close failed", zap.Error(err))
	}

	logger.Info("shutting down reload config")
	watchCancel()

	logger.Info("server stopped")
}
