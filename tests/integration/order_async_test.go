//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	repositoryOrder "github.com/Vladislav747/golang-project-order-system/internal/repository/order"
	repositoryOrderEvent "github.com/Vladislav747/golang-project-order-system/internal/repository/order_event"
	repositoryOutbox "github.com/Vladislav747/golang-project-order-system/internal/repository/outbox"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type AsyncMocks struct {
	pool *pgxpool.Pool
	ctx  context.Context
	svc  *service.Service
}

func TestCreateOrder_AsyncViaKafka(t *testing.T) {
	m := getAsyncMocks(t)

	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status: model.StatusPending,
		TotalAmount: 1500,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}

	require.NoError(t, m.svc.CreateOrderKafka(m.ctx, order))

	require.Eventually(t, func() bool {
		got, err := m.svc.GetOrder(m.ctx, order.ID.String())
		return err == nil && got.ID == order.ID && got.Status == model.StatusPending
	}, 15*time.Second, 200*time.Millisecond, "order was not created by kafka consumer")

	events, err := m.svc.GetOrderEvents(m.ctx)
	require.NoError(t, err)
	requireOrderEvent(t, events, order.ID, model.EventCreated, model.SourceKafka)
}

func TestUpdateOrder_AsyncViaKafka(t *testing.T) {
	m := getAsyncMocks(t)

	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status: model.StatusPending,
		TotalAmount: 1500,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, m.svc.CreateOrder(m.ctx, order))

	updated := order
	updated.Status = model.StatusCompleted
	require.NoError(t, m.svc.UpdateOrderKafka(m.ctx, updated))

	require.Eventually(t, func() bool {
		got, err := m.svc.GetOrder(m.ctx, order.ID.String())
		return err == nil && got.Status == model.StatusCompleted
	}, 15*time.Second, 200*time.Millisecond, "order was not updated by kafka consumer")

	events, err := m.svc.GetOrderEvents(m.ctx)
	require.NoError(t, err)
	requireOrderEvent(t, events, order.ID, model.EventUpdated, model.SourceKafka)
}

func TestDeleteOrder_AsyncViaKafka(t *testing.T) {
	m := getAsyncMocks(t)

	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status: model.StatusPending,
		TotalAmount: 1500,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, m.svc.CreateOrder(m.ctx, order))
	require.NoError(t, m.svc.DeleteOrderKafka(m.ctx, order.ID.String()))

	require.Eventually(t, func() bool {
		got, err := m.svc.GetOrder(m.ctx, order.ID.String())
		return err == nil && got.Status == model.StatusDeleted
	}, 15*time.Second, 200*time.Millisecond, "order was not soft-deleted by kafka consumer")

	events, err := m.svc.GetOrderEvents(m.ctx)
	require.NoError(t, err)
	requireOrderEvent(t, events, order.ID, model.EventDeleted, model.SourceKafka)
}

func getAsyncMocks(t *testing.T) *AsyncMocks {
	t.Helper()

	pool := setupPostgres(t)
	brokers := setupKafka(t)
	logger := zap.NewNop()
	ctx := t.Context()

	producer, err := kafka.NewProducer(brokers, IntegrationOrdersTopic, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = producer.Close() })

	svc := service.NewService(
		repositoryOrder.NewRepository(pool, logger),
		repositoryOrderEvent.NewRepository(pool, logger),
		repositoryOutbox.NewRepository(pool, logger),
		pool,
		producer,
		logger,
	)

	consumer, err := kafka.NewConsumer(
		brokers,
		IntegrationOrdersTopic,
		"integration-order-service",
		svc,
		logger,
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
		wg.Wait()
	})

	time.Sleep(2 * time.Second)

	return &AsyncMocks{
		pool: pool,
		ctx:  ctx,
		svc:  svc,
	}
}
