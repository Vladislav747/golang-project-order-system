package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/handler"
	"github.com/Vladislav747/golang-project-order-system/internal/repository"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	cfg, logger := mustInitConfigAndLogger()

	pool := mustInitPool(logger)

	defer pool.Close()


	producer := mustInitProducer(logger)
	defer producer.Close()
	svc := mustInitService(pool, producer, logger)
	consumer, cancel := mustStartConsumer(svc, logger)
	defer cancel()

	

	orderHandler := handler.NewHandler(svc, logger, cfg.HttpServer.RequestTimeout)

	// Регистрируем маршруты
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, orderHandler)

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
	gracefulShutdown(server, pool, logger, consumer, producer, cfg)
}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		log = slog.New(
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)

	case envProd:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)

	default: // If env config is invalid, set prod settings by default due to security
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	}

	return log
}

func gracefulShutdown(server *http.Server, pool *pgxpool.Pool, logger *slog.Logger, consumer *kafka.Consumer, producer *kafka.Producer, cfg *config.Config) {
	// Ждем сигналы прерывания
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Ждем сигналы прерывания
	<-ctx.Done()

	// Останавливаем сервер
	logger.Info("shutting down server")

	// Останавливаем consumer
	logger.Info("shutting down consumer")
	consumer.Close()

	// Останавливаем producer
	logger.Info("shutting down producer")
	producer.Close()

	// Устанавливаем timeout для graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HttpServer.GracefulShutdownTimeout)
	defer cancel()

	// Останавливаем сервер
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", slog.String("error", err.Error()))
	}

	logger.Info("server stopped")
}

func mustInitConfigAndLogger() (*config.Config, *slog.Logger) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatal("Error loading .env file: ", err)
	}

	cfg := config.MustLoad()

	logger := setupLogger(cfg.Env)

	logger.Info("starting app",
		slog.String("env", cfg.Env),
		slog.String("port", strconv.Itoa(cfg.Port)),
	)

	return cfg, logger
}

func mustInitPool(logger *slog.Logger) *pgxpool.Pool {
	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		log.Panicf("DATABASE_URL is not set: %v", databaseUrl)
	}

	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		log.Panicf("failed to create pool: %v", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		log.Panicf("failed to ping pool: %v", err)
	}

	return pool
}


func mustInitProducer(logger *slog.Logger) *kafka.Producer {
	producer := kafka.NewProducer(
		strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		os.Getenv("KAFKA_TOPIC_ORDERS"),
		logger,
	)
	return producer
}

func mustInitService(pool *pgxpool.Pool, producer *kafka.Producer, logger *slog.Logger) *service.Service {
	repository := repository.NewRepository(pool, logger)
	return service.NewService(repository, pool, producer, logger)
}


func mustStartConsumer(svc *service.Service, logger *slog.Logger) (*kafka.Consumer, context.CancelFunc) {
	consumer := kafka.NewConsumer(
		strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		os.Getenv("KAFKA_TOPIC_ORDERS"),
		os.Getenv("KAFKA_CONSUMER_GROUP"),
		svc,
		logger,
	)

	appCtx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := consumer.Run(appCtx); err != nil {
			logger.Error("consumer stopped", slog.String("error", err.Error()))
		}
	}()

	return consumer, cancel
}