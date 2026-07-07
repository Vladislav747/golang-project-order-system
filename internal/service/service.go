package service

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type Repository interface {
	CreateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	GetOrders(ctx context.Context, tx pgx.Tx) ([]model.Order, error)
	GetOrder(ctx context.Context, tx pgx.Tx, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	DeleteOrder(ctx context.Context, tx pgx.Tx, id string) error
}

type Service struct {
	repository Repository
	pool       *pgxpool.Pool
	producer   *kafka.Producer
	logger     *zap.Logger
}

func NewService(repository Repository, pool *pgxpool.Pool, producer *kafka.Producer, logger *zap.Logger) *Service {
	return &Service{
		repository: repository,
		pool:       pool,
		producer:   producer,
		logger:     logger,
	}
}

func (s *Service) CreateOrder(ctx context.Context, order model.Order) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = s.repository.CreateOrder(ctx, tx, order)
	if err != nil {
		return err
	}
	tx.Commit(ctx)
	return nil
}

func (s *Service) GetOrders(ctx context.Context) ([]model.Order, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	res, err := s.repository.GetOrders(ctx, tx)
	if err != nil {
		return nil, err
	}
	tx.Commit(ctx)
	return res, nil
}

func (s *Service) GetOrder(ctx context.Context, id string) (model.Order, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Order{}, err
	}
	defer tx.Rollback(ctx)

	res, err := s.repository.GetOrder(ctx, tx, id)
	if err != nil {
		return model.Order{}, err
	}
	tx.Commit(ctx)
	return res, nil
}

func (s *Service) UpdateOrder(ctx context.Context, order model.Order) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = s.repository.UpdateOrder(ctx, tx, order)
	if err != nil {
		return err
	}
	tx.Commit(ctx)
	return nil
}

func (s *Service) DeleteOrder(ctx context.Context, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = s.repository.DeleteOrder(ctx, tx, id)
	if err != nil {
		return err
	}
	tx.Commit(ctx)
	return nil
}

func (s *Service) HandleCreateOrder(ctx context.Context, msg kafka.CreateOrderMessage) error {
	order := model.Order{
		ID:          msg.OrderID,
		CustomerID:  msg.CustomerID,
		Status:      msg.Status,
		TotalAmount: msg.TotalAmount,
		Currency:    msg.Currency,
		Items:       msg.Items,
	}
	return s.CreateOrderFromKafka(ctx, order)
}

func (s *Service) CreateOrderKafka(ctx context.Context, order model.Order) error {
	message := kafka.CreateOrderMessage{
		OrderID:     order.ID,
		CustomerID:  order.CustomerID,
		Status:      order.Status,
		TotalAmount: order.TotalAmount,
		Currency:    order.Currency,
		Items:       order.Items,
	}
	s.logger.Info("sending message to kafka", zap.Any("message", message))
	return s.producer.SendMessage(message)
}

func (s *Service) CreateOrderFromKafka(ctx context.Context, order model.Order) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := s.repository.CreateOrder(ctx, tx, order); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
