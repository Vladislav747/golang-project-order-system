package service

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type Repository interface {
	CreateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	GetOrders(ctx context.Context, tx pgx.Tx) ([]model.Order, error)
	GetOrder(ctx context.Context, tx pgx.Tx, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error
	DeleteOrder(id int64) error
}


type service struct {
	repository Repository
	pool *pgxpool.Pool
	ctx context.Context
}

func NewService(ctx context.Context, repository Repository, pool *pgxpool.Pool) *service {
	return &service{
		repository: repository,
		pool: pool,
		ctx: ctx,
	}
}

func (s *service) CreateOrder(ctx context.Context, order model.Order) error {
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

func (s *service) GetOrders(ctx context.Context) ([]model.Order, error) {
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

func (s *service) GetOrder(ctx context.Context, id string) (model.Order, error) {
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

func (s *service) UpdateOrder(ctx context.Context, order model.Order ) error {
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

func (s *service) DeleteOrder(id int64) error {
	return s.repository.DeleteOrder(id)
}