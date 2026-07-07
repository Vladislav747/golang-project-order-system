package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/service/mocks"
)

func TestNewService(t *testing.T) {
	repo := mocks.NewMockRepository(t)
	svc := NewService(repo, nil, nil, zap.NewNop())

	if svc == nil {
		t.Fatal("expected service instance")
	}
}

func TestCreateOrder_RepositoryCalled(t *testing.T) {
	ctx := context.Background()

	repo := mocks.NewMockRepository(t)
	//Мок говорит: «когда вызовут Begin, верну фейковую транзакцию».
	txManager := mocks.NewMockTxManager(t)
	//Мок говорит: «когда вызовут Rollback, верни nil».
	mockTx := mocks.NewMockTx(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	//Мок говорит: «когда вызовут CreateOrder, верни nil».
	repo.EXPECT().
		CreateOrder(mock.Anything, mockTx, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == "pending"
		})).
		Return(nil)

	svc := NewService(repo, txManager, nil, zap.NewNop())

	err := svc.CreateOrder(ctx, model.Order{Status: "pending"})
	require.NoError(t, err)
}

func TestGetOrders_RepositoryCalled(t *testing.T) {
	ctx := context.Background()

	repo := mocks.NewMockRepository(t)

	//Мок говорит: «когда вызовут GetOrders, верни массив заказов».
	repo.EXPECT().
		GetOrders(mock.Anything).
		Return([]model.Order{{Status: "pending"}}, nil)

	svc := NewService(repo, nil, nil, zap.NewNop())

	orders, err := svc.GetOrders(ctx)
	require.NoError(t, err)
	require.Len(t, orders, 1)
	require.Equal(t, orders[0].Status, "pending")
}

func TestDeleteOrder_RepositoryCalled(t *testing.T) {
	ctx := context.Background()

	repo := mocks.NewMockRepository(t)
	txManager := mocks.NewMockTxManager(t)
	mockTx := mocks.NewMockTx(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	repo.EXPECT().
		DeleteOrder(mock.Anything, mockTx, "123").
		Return(nil)

	svc := NewService(repo, txManager, nil, zap.NewNop())

	err := svc.DeleteOrder(ctx, "123")
	require.NoError(t, err)
}

func TestUpdateOrder_RepositoryCalled(t *testing.T) {
	ctx := context.Background()

	repo := mocks.NewMockRepository(t)
	txManager := mocks.NewMockTxManager(t)
	mockTx := mocks.NewMockTx(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	order := model.Order{Status: "completed"}

	repo.EXPECT().
		UpdateOrder(mock.Anything, mockTx, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == "completed"
		})).
		Return(nil)

	svc := NewService(repo, txManager, nil, zap.NewNop())

	err := svc.UpdateOrder(ctx, order)
	require.NoError(t, err)
}

func TestGetOrder_RepositoryCalled(t *testing.T) {
	ctx := context.Background()

	repo := mocks.NewMockRepository(t)

	expected := model.Order{Status: "pending"}

	repo.EXPECT().
		GetOrder(mock.Anything, "123").
		Return(expected, nil)

	svc := NewService(repo, nil, nil, zap.NewNop())

	order, err := svc.GetOrder(ctx, "123")
	require.NoError(t, err)
	require.Equal(t, expected, order)
}
