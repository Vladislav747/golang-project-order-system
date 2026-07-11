package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type RepositoryOrder interface {
	CreateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	GetOrders(ctx context.Context) ([]model.Order, error)
	GetOrder(ctx context.Context, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	DeleteSoftOrder(ctx context.Context, tx pgx.Tx, id string) error
	DeleteOrder(ctx context.Context, tx pgx.Tx, id string) error
}

type RepositoryOrderEvent interface {
	CreateOrderEvent(ctx context.Context, tx pgx.Tx, order model.OrderEvent) error
	GetOrderEvents(ctx context.Context) ([]model.OrderEvent, error)
}

type TxManager interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Service struct {
	repositoryOrder      RepositoryOrder
	repositoryOrderEvent RepositoryOrderEvent
	txManager            TxManager
	producer             *kafka.Producer
	logger               *zap.Logger
}

func NewService(repositoryOrder RepositoryOrder, repositoryOrderEvent RepositoryOrderEvent, txManager TxManager, producer *kafka.Producer, logger *zap.Logger) *Service {
	return &Service{
		repositoryOrder:      repositoryOrder,
		repositoryOrderEvent: repositoryOrderEvent,
		txManager:            txManager,
		producer:             producer,
		logger:               logger,
	}
}

func (s *Service) CreateOrder(ctx context.Context, order model.Order) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {

		if err := s.repositoryOrder.CreateOrder(ctx, tx, order); err != nil {
			return err
		}

		event, err := buildOrderEvent(order.ID, model.EventCreated, model.SourceHTTPSync, order)
		if err != nil {
			s.logger.Error("failed to build order event", zap.Error(err))
			return err
		}

		return s.repositoryOrderEvent.CreateOrderEvent(ctx, tx, event)
	})
}

func (s *Service) GetOrders(ctx context.Context) ([]model.Order, error) {
	// read операция не должна быть в транзакции
	return s.repositoryOrder.GetOrders(ctx)
}

func (s *Service) GetOrder(ctx context.Context, id string) (model.Order, error) {
	// read операция не должна быть в транзакции
	return s.repositoryOrder.GetOrder(ctx, id)
}

func (s *Service) UpdateOrder(ctx context.Context, order model.Order) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {

		if err := s.repositoryOrder.UpdateOrder(ctx, tx, order); err != nil {
			return err
		}

		event, err := buildOrderEvent(order.ID, model.EventUpdated, model.SourceHTTPSync, order)
		if err != nil {
			s.logger.Error("failed to build order event", zap.Error(err))
			return err
		}

		return s.repositoryOrderEvent.CreateOrderEvent(ctx, tx, event)
	})
}

func (s *Service) DeleteOrder(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {

		if err := s.repositoryOrder.DeleteOrder(ctx, tx, id); err != nil {
			s.logger.Error("failed to delete order in repository", zap.Error(err))
			return err
		}

		orderIDUUID, err := uuid.Parse(id)
		if err != nil {
			s.logger.Error("failed to parse order ID", zap.Error(err))
			return err
		}

		event, err := buildOrderEvent(orderIDUUID, model.EventDeleted, model.SourceHTTPSync, nil)
		if err != nil {
			s.logger.Error("failed to build order event", zap.Error(err))
		}
		return s.repositoryOrderEvent.CreateOrderEvent(ctx, tx, event)
	})
}

func (s *Service) DeleteSoftOrder(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repositoryOrder.DeleteSoftOrder(ctx, tx, id)
	})
}
