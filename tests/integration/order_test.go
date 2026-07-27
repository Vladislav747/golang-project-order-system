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
	"github.com/Vladislav747/golang-project-order-system/internal/service"
)

type Mocks struct {
	pool *pgxpool.Pool
	ctx context.Context
	svc *service.Service
}

func TestCreateOrder_CheckEventsInDatabase(t *testing.T) {
	mockHelper := getMocks(t)
	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      "pending",
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	got, err := mockHelper.svc.GetOrder(mockHelper.ctx, order.ID.String())
	require.NoError(t, err)
	require.Equal(t, order.ID, got.ID)
	require.Equal(t, "pending", got.Status)
	events, err := mockHelper.svc.GetOrderEvents(mockHelper.ctx)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	found := false
	for _, e := range events {
		if e.OrderID == order.ID && e.EventType == model.EventCreated {
			found = true
			require.Equal(t, model.SourceHTTPSync, e.Source)
			break
		}
	}
	require.True(t, found, "created event not found")
}

func TestUpdateOrder_CheckEventsInDatabase(t *testing.T) {
	mockHelper := getMocks(t)
	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      "pending",
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	updateOrder := model.Order{
		ID:          order.ID,
		CustomerID:  order.CustomerID,
		Status:      "shipped",
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.UpdateOrder(mockHelper.ctx, updateOrder))
	got, err := mockHelper.svc.GetOrder(mockHelper.ctx, order.ID.String())
	require.NoError(t, err)
	require.Equal(t, order.ID, got.ID)
	require.Equal(t, "shipped", got.Status)
	events, err := mockHelper.svc.GetOrderEvents(mockHelper.ctx)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	var created, updated bool
	for _, e := range events {
		if e.OrderID != order.ID {
			continue
		}
		switch e.EventType {
		case model.EventCreated:
			created = true
			require.Equal(t, model.SourceHTTPSync, e.Source)
		case model.EventUpdated:
			updated = true
			require.Equal(t, model.SourceHTTPSync, e.Source)
		}
	}
	require.True(t, created, "created event not found")
	require.True(t, updated, "updated event not found")
}


func TestSoftDeleteOrder_CheckEventsInDatabase(t *testing.T) {
	mockHelper := getMocks(t)
	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      "pending",
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	require.NoError(t, mockHelper.svc.DeleteSoftOrder(mockHelper.ctx, order.ID.String()))
	got, err := mockHelper.svc.GetOrder(mockHelper.ctx, order.ID.String())
	require.NoError(t, err)
	require.Equal(t, order.ID, got.ID)
	require.Equal(t, "deleted", got.Status)
	events, err := mockHelper.svc.GetOrderEvents(mockHelper.ctx)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	var created, deleted bool
	for _, e := range events {
		if e.OrderID != order.ID {
			continue
		}
		switch e.EventType {
		case model.EventCreated:
			created = true
			require.Equal(t, model.SourceHTTPSync, e.Source)
		case model.EventDeleted:
			deleted = true
			require.Equal(t, model.SourceHTTPSync, e.Source)
		}
	}
	require.True(t, created, "created event not found")
	require.True(t, deleted, "updated event not found")
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
		Status:      "pending",
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, mockHelper.svc.CreateOrder(mockHelper.ctx, order))
	err := mockHelper.svc.CreateOrder(mockHelper.ctx, order)
	require.Contains(t, err.Error(), "duplicate key value violates unique constraint \"orders_pkey\"")
}

func getMocks (t *testing.T) *Mocks {
	t.Helper()
	pool := setupPostgres(t)
	ctx := t.Context()
	logger := zap.NewNop()
	svc := service.NewService(
		repositoryOrder.NewRepository(pool, logger),
		repositoryOrderEvent.NewRepository(pool, logger),
		pool, // TxManager: у *pgxpool.Pool есть Begin
		nil,
		logger,
	)
	return &Mocks{
		pool: pool,
		ctx: ctx,
		svc: svc,
	}
}