package main

import (
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"os/signal"
	"syscall"
	"github.com/joho/godotenv"
	"github.com/jackc/pgx/v5/pgxpool"
	"context"
	"strings"

	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
	"github.com/Vladislav747/golang-project-order-system/internal/handler"
	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/repository"
	"github.com/Vladislav747/golang-project-order-system/internal/service"

)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatal("Error loading .env file: ", err)
	}

	cfg := config.MustLoad()

	logger := setupLogger(cfg.Env)

	logger.Info("starting app",
		slog.String("env", cfg.Env),
		slog.String("port", strconv.Itoa(cfg.Port)),
	)

	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		logger.Error("DATABASE_URL is not set")
		os.Exit(1)
	}

	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		logger.Error("failed to create pool", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := pool.Ping(context.Background()); err != nil {
		logger.Error("failed to ping pool", slog.String("error", err.Error()))
		os.Exit(1)
	}

	//@TODO: Remove this after testing
	if err == nil {
		logger.Info("connected to database")
	}

	defer pool.Close()

	ctx := context.Background()

	// Иницилизация kafka
	producer := kafka.NewProducer(
		strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		os.Getenv("KAFKA_TOPIC_ORDERS"),
		logger,
	)
	defer producer.Close()

	repository := repository.NewRepository(pool, logger)
	service := service.NewService(repository, pool, producer, logger)

	consumer := kafka.NewConsumer(
		strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		os.Getenv("KAFKA_TOPIC_ORDERS"),
		os.Getenv("KAFKA_CONSUMER_GROUP"),
	)

	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := consumer.Run(appCtx); err != nil {
			logger.Error("consumer stopped", slog.String("error", err.Error()))
		}
	}()

	orderHandler := handler.NewHandler(ctx, service, logger)

	// Регистрируем маршруты
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, orderHandler)

	server := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.Port),
		Handler: mux,
		// ReadTimeout — максимальное время на чтение всего запроса (заголовки + тело)
		ReadTimeout:  cfg.HttpServer.ReadTimeout,
		// WriteTimeout — максимальное время на запись ответа клиенту
		WriteTimeout: cfg.HttpServer.WriteTimeout,
		// IdleTimeout — максимальное время ожидания следующего запроса при keep-alive соединении
		IdleTimeout: cfg.HttpServer.IdleTimeout,
	}
	// Запускаем сервер в отдельной горутине
	go func() {
		logger.Info("server started")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	// Запускаем graceful shutdown
	gracefulShutdown(server, pool, logger, cfg)
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

func gracefulShutdown(server *http.Server, pool *pgxpool.Pool, logger *slog.Logger, cfg *config.Config) {
	// Ждем сигналы прерывания
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Ждем сигналы прерывания
	<-ctx.Done()

	// Останавливаем сервер
	logger.Info("shutting down server")

	// Устанавливаем timeout для graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HttpServer.GracefulShutdownTimeout)
    defer cancel()

	// Останавливаем сервер
	if err := server.Shutdown(shutdownCtx); err != nil {
        logger.Error("server shutdown failed", slog.String("error", err.Error()))
    }

	logger.Info("server stopped")
}