package repository

import (
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *repository {
	return &repository{pool: pool}
}

func (r *repository) CreateOrder(order model.Order) error {
	return errors.New("not implemented")
}

func (r *repository) GetOrders() []model.Order {
	return nil
}

func (r *repository) GetOrder(id int64) (model.Order, error) {
	return model.Order{}, errors.New("not implemented")
}

func (r *repository) UpdateOrder(order model.Order) error {
	return errors.New("not implemented")
}

func (r *repository) DeleteOrder(id int64) error {
	return errors.New("not implemented")
}
