package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/handler"
	"github.com/Vladislav747/golang-project-order-system/internal/handler/health"
	orderHandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order"
	orderEventHandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order_event"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/logger"
	repositoryOrder "github.com/Vladislav747/golang-project-order-system/internal/repository/order"
	repositoryOrderEvent "github.com/Vladislav747/golang-project-order-system/internal/repository/order_event"
	repositoryOutbox "github.com/Vladislav747/golang-project-order-system/internal/repository/outbox"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
	grpcserver "github.com/Vladislav747/golang-project-order-system/internal/transport/grpcserver"
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
	svc := mustInitService(pool, producer, logger)
	consumer, cancel, consumerWG := mustStartConsumer(cfg, svc, logger)

	relay, relayCancel, relayWG := mustStartOutboxRelay(cfg, pool, producer, logger)
	cleaner, cleanerCancel, cleanerWG := mustStartOutboxCleaner(cfg, pool, logger)

	provider := config.NewProvider(cfg)

	orderHandler := orderHandler.NewHandler(svc, logger, provider)
	orderEventHandler := orderEventHandler.NewHandler(svc, logger, provider)
	healthHandler := health.NewHandler(health.NewDBChecker(pool))

	// Регистрируем маршруты
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, logger, orderHandler, orderEventHandler, healthHandler)

	server := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.Port),
		Handler: mux,
		// ReadTimeout — максимальное время на чтение всего запроса (заголовки + тело)
		ReadTimeout: cfg.HttpServer.ReadTimeout,
		// WriteTimeout — максимальное время на запись ответа клиенту
		WriteTimeout: cfg.HttpServer.WriteTimeout,
		// IdleTimeout — максимальное время ожидания следующего запроса при keep-alive соединении
		IdleTimeout: cfg.HttpServer.IdleTimeout,
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())
	go func() {
		path := os.Getenv("CONFIG_PATH")
		if path == "" {
			// если тут делать панику и если его не перехватил recover, то будет паника в main
			logger.Error("CONFIG_PATH is not set")
			return
		}
		onReload := func(newCfg *config.Config) {
			server.ReadTimeout = newCfg.HttpServer.ReadTimeout
			server.WriteTimeout = newCfg.HttpServer.WriteTimeout
			server.IdleTimeout = newCfg.HttpServer.IdleTimeout
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

	orderGrpc := grpcserver.NewOrderServer(svc, logger, provider)
	orderEventGrpc := grpcserver.NewOrderEventServer(svc, logger, provider)
	grpcSrv, err := grpcserver.NewServer(cfg.GrpcPort, orderGrpc, orderEventGrpc, logger)
	if err != nil {
		log.Panicf("failed to create gRPC server: %v", err)
	}

	go func() {
		logger.Info("gRPC server started on port", zap.Int("port", cfg.GrpcPort))
		if err := grpcSrv.Serve(); err != nil {
			log.Panicf("gRPC server error: %v", err)
		}
	}()

	// Запускаем HTTP сервер в отдельной горутине
	go func() {
		logger.Info("server started")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Panicf("server error: %v", err)
		}
	}()

	// Запускаем graceful shutdown
	gracefulShutdown(
		server,
		grpcSrv,
		logger,
		consumer,
		cancel,
		consumerWG,
		producer,
		provider,
		watchCancel,
		relayCancel,
		cleanerCancel,
		relayWG,
		cleanerWG,
	)
}

func gracefulShutdown(
	server *http.Server,
	grpcSrv *grpcserver.Server,
	logger *zap.Logger,
	consumer *kafka.Consumer,
	consumerCancel context.CancelFunc,
	consumerWG *sync.WaitGroup,
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

	// Останавливаем сервер
	logger.Info("shutting down server")

	// Устанавливаем timeout для graceful shutdown (актуальный из provider)
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		provider.Get().HttpServer.GracefulShutdownTimeout,
	)
	defer cancel()

	// Останавливаем HTTP сервер
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", zap.Error(err))
	}

	// Останавливаем GRPC сервер
	grpcSrv.Shutdown(shutdownCtx)

	logger.Info("shutting down consumer")
	consumerCancel()
	if err := consumer.Close(); err != nil {
		logger.Error("consumer close failed", zap.Error(err))
	}
	// горутина точно завершилась - consumer.Run завершится только после завершения работы консьюмера
	consumerWG.Wait()

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

func mustInitConfigAndLogger() (*config.Config, *zap.Logger) {
	cfg := config.MustLoad()

	logger := logger.MustNew(cfg.Env)

	logger.Info("starting app",
		zap.String("env", cfg.Env),
		zap.Int("port", cfg.Port),
		zap.Int("grpcPort", cfg.GrpcPort),
	)

	return cfg, logger
}

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

func mustInitProducer(cfg *config.Config, logger *zap.Logger) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicOrders,
		logger,
	)
	return producer, err
}

func mustInitService(pool *pgxpool.Pool, producer *kafka.Producer, logger *zap.Logger) *service.Service {
	repositoryOrder := repositoryOrder.NewRepository(pool, logger)
	repositoryOrderEvent := repositoryOrderEvent.NewRepository(pool, logger)
	repositoryOutbox := repositoryOutbox.NewRepository(pool, logger)
	return service.NewService(repositoryOrder, repositoryOrderEvent, repositoryOutbox, pool, producer, logger)
}

func mustStartConsumer(cfg *config.Config, svc *service.Service, logger *zap.Logger) (*kafka.Consumer, context.CancelFunc, *sync.WaitGroup) {
	consumer, err := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicOrders,
		cfg.Kafka.ConsumerGroup,
		svc,
		logger,
	)

	if err != nil {
		log.Panicf("failed to create consumer: %v", err)
	}

	appCtx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Run(appCtx); err != nil {
			logger.Error("consumer stopped", zap.Error(err))
		}
	}()

	return consumer, cancel, &wg
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
