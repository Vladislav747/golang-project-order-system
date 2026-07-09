package service

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type Repository interface {
	CreateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	GetOrders(ctx context.Context) ([]model.Order, error)
	GetOrder(ctx context.Context, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	DeleteSoftOrder(ctx context.Context, tx pgx.Tx, id string) error
	DeleteOrder(ctx context.Context, tx pgx.Tx, id string) error
}

type TxManager interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Service struct {
	repository Repository
	txManager  TxManager
	producer   *kafka.Producer
	logger     *zap.Logger
}

func NewService(repository Repository, txManager TxManager, producer *kafka.Producer, logger *zap.Logger) *Service {
	return &Service{
		repository: repository,
		txManager:  txManager,
		producer:   producer,
		logger:     logger,
	}
}

func (s *Service) CreateOrder(ctx context.Context, order model.Order) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repository.CreateOrder(ctx, tx, order)
	})
}

func (s *Service) GetOrders(ctx context.Context) ([]model.Order, error) {
	// read операция не должна быть в транзакции
	return s.repository.GetOrders(ctx)
}

func (s *Service) GetOrder(ctx context.Context, id string) (model.Order, error) {
	// read операция не должна быть в транзакции
	return s.repository.GetOrder(ctx, id)
}

func (s *Service) UpdateOrder(ctx context.Context, order model.Order) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repository.UpdateOrder(ctx, tx, order)
	})
}

func (s *Service) DeleteOrder(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repository.DeleteOrder(ctx, tx, id)
	})
}

func (s *Service) DeleteSoftOrder(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repository.DeleteSoftOrder(ctx, tx, id)
	})
}
