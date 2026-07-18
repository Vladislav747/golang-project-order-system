package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/handler"
	orderHandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order"
	orderEventHandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order_event"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/logger"
	repositoryOrder "github.com/Vladislav747/golang-project-order-system/internal/repository/order"
	repositoryOrderEvent "github.com/Vladislav747/golang-project-order-system/internal/repository/order_event"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
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

	orderHandler := orderHandler.NewHandler(svc, logger, cfg.HttpServer.RequestTimeout, cfg.ProcessingMode)
	orderEventHandler := orderEventHandler.NewHandler(svc, logger, cfg.HttpServer.RequestTimeout)

	// Регистрируем маршруты
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, orderHandler, orderEventHandler)

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
	// Запускаем сервер в отдельной горутине
	go func() {
		logger.Info("server started")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Panicf("server error: %v", err)
		}
	}()

	// Запускаем graceful shutdown
	gracefulShutdown(server, logger, consumer, cancel, consumerWG, producer, cfg)
}

func gracefulShutdown(
	server *http.Server,
	logger *zap.Logger,
	consumer *kafka.Consumer,
	consumerCancel context.CancelFunc,
	consumerWG *sync.WaitGroup,
	producer *kafka.Producer,
	cfg *config.Config,
) {
	// Ждем сигналы прерывания
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	// Останавливаем сервер
	logger.Info("shutting down server")

	// Устанавливаем timeout для graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HttpServer.GracefulShutdownTimeout)
	defer cancel()

	// Останавливаем сервер
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", zap.Error(err))
	}

	logger.Info("shutting down consumer")
	consumerCancel()
	if err := consumer.Close(); err != nil {
		logger.Error("consumer close failed", zap.Error(err))
	}
	// горутина точно завершилась - consumer.Run завершится только после завершения работы консьюмера
	consumerWG.Wait()

	logger.Info("shutting down producer")
	// Закрываем producer
	if err := producer.Close(); err != nil {
		logger.Error("producer close failed", zap.Error(err))
	}

	logger.Info("server stopped")
}

func mustInitConfigAndLogger() (*config.Config, *zap.Logger) {
	cfg := config.MustLoad()

	logger := logger.MustNew(cfg.Env)

	logger.Info("starting app",
		zap.String("env", cfg.Env),
		zap.Int("port", cfg.Port),
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
	return service.NewService(repositoryOrder, repositoryOrderEvent, pool, producer, logger)
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
