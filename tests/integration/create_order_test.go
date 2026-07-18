//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"github.com/Vladislav747/golang-project-order-system/internal/model"
	repositoryOrder "github.com/Vladislav747/golang-project-order-system/internal/repository/order"
	repositoryOrderEvent "github.com/Vladislav747/golang-project-order-system/internal/repository/order_event"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
)

func TestCreateOrder_WithCheckDatabase(t *testing.T) {
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
	order := model.Order{
		ID:          uuid.New(),
		CustomerID:  uuid.New(),
		Status:      "pending",
		TotalAmount: 1000,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	require.NoError(t, svc.CreateOrder(ctx, order))
	got, err := svc.GetOrder(ctx, order.ID.String())
	require.NoError(t, err)
	require.Equal(t, order.ID, got.ID)
	require.Equal(t, "pending", got.Status)
	events, err := svc.GetOrderEvents(ctx)
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

func TestUpdateOrder_WithCheckDatabase(t *testing.T) {
	pool := setupPostgres(t)
	ctx := t.Context()
	logger := zap.NewNop()
	svc := service.NewService(
		repositoryOrder.NewRepository(pool, logger),
		repositoryOrderEvent.NewRepository(pool, logger),
		pool, // TxManager: у *pgxpool.Pool есть Begin
		nil,
		logger,
	}