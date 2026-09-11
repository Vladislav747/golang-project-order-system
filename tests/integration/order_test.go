//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	repositoryOrder "github.com/Vladislav747/golang-project-order-system/internal/repository/order"
	repositoryOrderEvent "github.com/Vladislav747/golang-project-order-system/internal/repository/order_event"
	repositoryOutbox "github.com/Vladislav747/golang-project-order-system/internal/repository/outbox"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
)

type Mocks struct {
	pool *pgxpool.Pool
	ctx  context.Context
	svc  *service.Service
}

func TestCreateOrder_CheckEventsInDatabase(t *testing.T) {
	mockHelper := getMocks(t)
	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      model.StatusPending,
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	got, err := mockHelper.svc.GetOrder(mockHelper.ctx, order.ID.String())
	require.NoError(t, err)
	require.Equal(t, order.ID, got.ID)
	require.Equal(t, model.StatusPending, got.Status)

	events, err := mockHelper.svc.GetOrderEvents(mockHelper.ctx)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	requireOrderEvent(t, events, order.ID, model.EventCreated, model.SourceHTTPSync)
}

func TestUpdateOrder_CheckEventsInDatabase(t *testing.T) {
	mockHelper := getMocks(t)
	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      model.StatusPending,
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	updateOrder := model.Order{
		ID:          order.ID,
		CustomerID:  order.CustomerID,
		Status:      model.StatusCompleted,
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.UpdateOrder(mockHelper.ctx, updateOrder))
	got, err := mockHelper.svc.GetOrder(mockHelper.ctx, order.ID.String())
	require.NoError(t, err)
	require.Equal(t, order.ID, got.ID)
	require.Equal(t, model.StatusCompleted, got.Status)

	events, err := mockHelper.svc.GetOrderEvents(mockHelper.ctx)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	requireOrderEvents(t, events, order.ID, model.SourceHTTPSync, model.EventCreated, model.EventUpdated)
}

func TestSoftDeleteOrder_CheckEventsInDatabase(t *testing.T) {
	mockHelper := getMocks(t)
	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      model.StatusPending,
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	require.NoError(t, mockHelper.svc.DeleteSoftOrder(mockHelper.ctx, order.ID.String()))
	got, err := mockHelper.svc.GetOrder(mockHelper.ctx, order.ID.String())
	require.NoError(t, err)
	require.Equal(t, order.ID, got.ID)
	require.Equal(t, model.StatusDeleted, got.Status)

	events, err := mockHelper.svc.GetOrderEvents(mockHelper.ctx)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	requireOrderEvents(t, events, order.ID, model.SourceHTTPSync, model.EventCreated, model.EventDeleted)
}

func TestGetOrderNotFound_CheckInDatabase(t *testing.T) {
	mockHelper := getMocks(t)

	_, err := mockHelper.svc.GetOrder(mockHelper.ctx, uuid.New().String())
	require.ErrorIs(t, err, model.ErrOrderNotFound)
	events, err := mockHelper.svc.GetOrderEvents(mockHelper.ctx)
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestDuplicateCreateOrder_CheckInDatabase(t *testing.T) {
	mockHelper := getMocks(t)

	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      model.StatusPending,
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	err := mockHelper.svc.CreateOrder(mockHelper.ctx, order)
	require.Contains(t, err.Error(), "duplicate key value violates unique constraint \"orders_pkey\"")
}

func getMocks(t *testing.T) *Mocks {
	t.Helper()
	pool := setupPostgres(t)
	ctx := t.Context()
	logger := zap.NewNop()
	svc := service.NewService(
		repositoryOrder.NewRepository(pool, logger),
		repositoryOrderEvent.NewRepository(pool, logger),
		repositoryOutbox.NewRepository(pool, logger),
		pool, // TxManager: у *pgxpool.Pool есть Begin
		nil,
		logger,
	)
	return &Mocks{
		pool: pool,
		ctx:  ctx,
		svc:  svc,
	}
}

func requireOrderEvent(
	t *testing.T,
	events []model.OrderEvent,
	orderID uuid.UUID,
	eventType model.EventType,
	source model.EventSource,
) {
	t.Helper()
	for _, e := range events {
		if e.OrderID == orderID && e.EventType == eventType {
			require.Equal(t, source, e.Source)
			return
		}
	}
	require.Failf(t, "event not found", "order_id=%s event_type=%s source=%s", orderID, eventType, source)
}

func requireOrderEvents(
	t *testing.T,
	events []model.OrderEvent,
	orderID uuid.UUID,
	source model.EventSource,
	types ...model.EventType,
) {
	t.Helper()
	for _, typ := range types {
		requireOrderEvent(t, events, orderID, typ, source)
	}
}