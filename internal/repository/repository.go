package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type repository struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

func NewRepository(pool *pgxpool.Pool, logger *zap.Logger) *repository {
	return &repository{pool: pool, logger: logger}
}

func (r *repository) CreateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error {
	sqlQuery := `
		INSERT INTO orders (id, customer_id, status, total_amount, currency, items)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := tx.Exec(ctx, sqlQuery, order.ID, order.CustomerID, order.Status, order.TotalAmount, order.Currency, order.Items)
	if err != nil {
		r.logger.Error("failed to create order in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *repository) GetOrders(ctx context.Context, tx pgx.Tx) ([]model.Order, error) {
	sqlQuery := `SELECT * from orders`

	rows, err := tx.Query(ctx, sqlQuery)
	if err != nil {
		r.logger.Error("failed to get orders in repository", zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var orders []model.Order
	for rows.Next() {
		var order model.Order
		var deletedAt *time.Time

		err := rows.Scan(
			&order.ID,
			&order.CustomerID,
			&order.Status,
			&order.TotalAmount,
			&order.Currency,
			&order.Items,
			&order.CreatedAt,
			&order.UpdatedAt,
			&deletedAt,
		)
		if err != nil {
			return nil, err
		}

		if deletedAt != nil {
			order.DeletedAt = *deletedAt
		}
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return orders, nil

}

func (r *repository) GetOrder(ctx context.Context, tx pgx.Tx, id string) (model.Order, error) {
	sqlQuery := `
        SELECT id, customer_id, status, total_amount, currency, items,
               created_at, updated_at, deleted_at
        FROM orders
        WHERE id = $1 AND deleted_at IS NULL
    `

	var order model.Order
	var deletedAt *time.Time
	err := tx.QueryRow(ctx, sqlQuery, id).Scan(
		&order.ID,
		&order.CustomerID,
		&order.Status,
		&order.TotalAmount,
		&order.Currency,
		&order.Items,
		&order.CreatedAt,
		&order.UpdatedAt,
		&deletedAt,
	)
	if err != nil {
		r.logger.Error("failed to get order in repository", zap.Error(err))
		return model.Order{}, err
	}
	if deletedAt != nil {
		order.DeletedAt = *deletedAt
	}
	return order, nil
}

func (r *repository) UpdateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error {
	sqlQuery := `
        UPDATE orders
		SET status = $2,
			updated_at = NOW()
		WHERE id = $1;
    `
	_, err := tx.Exec(ctx, sqlQuery, order.ID, order.Status)
	if err != nil {
		r.logger.Error("failed to update order in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *repository) DeleteOrder(ctx context.Context, tx pgx.Tx, id string) error {
	sqlQuery := `DELETE FROM orders WHERE id = $1;`
	_, err := tx.Exec(ctx, sqlQuery, id)
	if err != nil {
		r.logger.Error("failed to delete order in repository", zap.Error(err))
		return err
	}
	return nil
}
