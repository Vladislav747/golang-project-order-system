package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type mockRepository struct {
	createOrderFn func(ctx context.Context, tx pgx.Tx, order model.Order) error
	getOrdersFn   func(ctx context.Context, tx pgx.Tx) ([]model.Order, error)
	getOrderFn    func(ctx context.Context, tx pgx.Tx, id string) (model.Order, error)
	updateOrderFn func(ctx context.Context, tx pgx.Tx, order model.Order) error
	deleteOrderFn func(ctx context.Context, tx pgx.Tx, id string) error
}

func (m *mockRepository) CreateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error {
	if m.createOrderFn != nil {
		return m.createOrderFn(ctx, tx, order)
	}
	return nil
}

func (m *mockRepository) GetOrders(ctx context.Context, tx pgx.Tx) ([]model.Order, error) {
	if m.getOrdersFn != nil {
		return m.getOrdersFn(ctx, tx)
	}
	return nil, nil
}

func (m *mockRepository) GetOrder(ctx context.Context, tx pgx.Tx, id string) (model.Order, error) {
	if m.getOrderFn != nil {
		return m.getOrderFn(ctx, tx, id)
	}
	return model.Order{}, nil
}

func (m *mockRepository) UpdateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error {
	if m.updateOrderFn != nil {
		return m.updateOrderFn(ctx, tx, order)
	}
	return nil
}

func (m *mockRepository) DeleteOrder(ctx context.Context, tx pgx.Tx, id string) error {
	if m.deleteOrderFn != nil {
		return m.deleteOrderFn(ctx, tx, id)
	}
	return nil
}

func newTestLogger() (*zap.Logger, error) {
	return zap.NewDevelopment()
}

func newTestService(repo Repository) *Service {
	logger, _ := newTestLogger()
	return NewService(repo, nil, nil, logger)
}

func TestNewService(t *testing.T) {
	svc := newTestService(&mockRepository{})
	if svc == nil {
		t.Fatal("expected service instance")
	}
}

func TestCreateOrderKafka_BuildsMessage(t *testing.T) {
	t.Skip("TODO: inject OrderPublisher interface to mock producer")

	order := model.Order{
		CustomerID:  uuid.New(),
		Status:      "pending",
		TotalAmount: 1000,
		Currency:    "RUB",
		Items:       []byte("[]"),
	}

	_ = kafka.CreateOrderMessage{
		OrderID:     order.ID,
		CustomerID:  order.CustomerID,
		Status:      order.Status,
		TotalAmount: order.TotalAmount,
		Currency:    order.Currency,
		Items:       order.Items,
	}
}

func TestCreateOrder_RepositoryCalled(t *testing.T) {
	t.Skip("TODO: mock pgxpool or extract TxManager for unit tests")

	ctx := context.Background()
	called := false

	repo := &mockRepository{
		createOrderFn: func(ctx context.Context, tx pgx.Tx, order model.Order) error {
			called = true
			return nil
		},
	}

	svc := newTestService(repo)
	_ = svc.CreateOrder(ctx, model.Order{Status: "pending"})
	_ = called
}
