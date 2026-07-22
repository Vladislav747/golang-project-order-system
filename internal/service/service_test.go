//go:build !integration

package service

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/service/mocks"
)

func TestNewService(t *testing.T) {
	t.Parallel()
	_, _, _, _, svc := createMocks(t)

	if svc == nil {
		t.Fatal("expected service instance")
	}
}

func TestCreateOrder_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, repoEvent, txManager, mockTx, svc := createMocks(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	order := model.Order{Status: "pending"}

	repoOrder.EXPECT().
		CreateOrder(mock.Anything, mockTx, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == "pending"
		})).
		Return(nil)

	repoEvent.EXPECT().
		CreateOrderEvent(mock.Anything, mockTx, mock.MatchedBy(func(e model.OrderEvent) bool {
			return e.EventType == model.EventCreated && e.Source == model.SourceHTTPSync
		})).
		Return(nil)

	err := svc.CreateOrder(ctx, order)
	require.NoError(t, err)
}

func TestGetOrders_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, _, _, _, svc := createMocks(t)

	repoOrder.EXPECT().
		GetOrders(mock.Anything).
		Return([]model.Order{{Status: "pending"}}, nil)

	orders, err := svc.GetOrders(ctx)
	require.NoError(t, err)
	require.Len(t, orders, 1)
	require.Equal(t, "pending", orders[0].Status)
}

func TestDeleteOrder_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, repoEvent, txManager, mockTx, svc := createMocks(t)

	orderID := "6ba7b810-9dad-11d1-80b4-00c04fd43023"

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	repoOrder.EXPECT().
		DeleteOrder(mock.Anything, mockTx, orderID).
		Return(nil)

	repoEvent.EXPECT().
		CreateOrderEvent(mock.Anything, mockTx, mock.MatchedBy(func(e model.OrderEvent) bool {
			return e.EventType == model.EventDeleted &&
				e.Source == model.SourceHTTPSync &&
				e.OrderID == uuid.MustParse(orderID)
		})).
		Return(nil)

	err := svc.DeleteOrder(ctx, orderID)
	require.NoError(t, err)
}

func TestUpdateOrder_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, repoEvent, txManager, mockTx, svc := createMocks(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	order := model.Order{Status: "completed"}

	repoOrder.EXPECT().
		UpdateOrder(mock.Anything, mockTx, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == "completed"
		})).
		Return(nil)

	repoEvent.EXPECT().
		CreateOrderEvent(mock.Anything, mockTx, mock.MatchedBy(func(e model.OrderEvent) bool {
			return e.EventType == model.EventUpdated && e.Source == model.SourceHTTPSync
		})).
		Return(nil)

	err := svc.UpdateOrder(ctx, order)
	require.NoError(t, err)
}

func TestGetOrder_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, _, _, _, svc := createMocks(t)

	expected := model.Order{Status: "pending"}

	repoOrder.EXPECT().
		GetOrder(mock.Anything, "123").
		Return(expected, nil)

	order, err := svc.GetOrder(ctx, "123")
	require.NoError(t, err)
	require.Equal(t, expected, order)
}

func createMocks(t *testing.T) (
	*mocks.MockRepositoryOrder,
	*mocks.MockRepositoryOrderEvent,
	*mocks.MockTxManager,
	*mocks.MockTx,
	*Service,
) {
	t.Helper()
	repoOrder := mocks.NewMockRepositoryOrder(t)
	repoEvent := mocks.NewMockRepositoryOrderEvent(t)
	txManager := mocks.NewMockTxManager(t)
	mockTx := mocks.NewMockTx(t)

	svc := NewService(repoOrder, repoEvent, txManager, nil, zap.NewNop())

	return repoOrder, repoEvent, txManager, mockTx, svc
}
