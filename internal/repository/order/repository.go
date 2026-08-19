package order

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type Repository struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

func NewRepository(pool *pgxpool.Pool, logger *zap.Logger) *Repository {
	return &Repository{pool: pool, logger: logger}
}

func (r *Repository) CreateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error {

	sqlQuery := sqlx.Rebind(sqlx.DOLLAR, `
		INSERT INTO orders (id, customer_id, status, total_amount, currency, items)
		VALUES (?, ?, ?, ?, ?, ?)
	`)

	_, err := tx.Exec(ctx, sqlQuery, order.ID, order.CustomerID, order.Status, order.TotalAmount, order.Currency, order.Items)
	if err != nil {
		r.logger.Error("failed to create order in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *Repository) GetOrders(ctx context.Context) ([]model.Order, error) {
	sqlQuery := `SELECT * from orders`
	rows, err := r.pool.Query(ctx, sqlQuery)
	if err != nil {
		r.logger.Error("failed to get orders in repository", zap.Error(err))
		return nil, err
	}

	orders, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.Order])
	if err != nil {
		r.logger.Error("failed to collect orders from rows", zap.Error(err))
		return nil, err
	}
	return orders, nil

}

func (r *Repository) GetOrder(ctx context.Context, id string) (model.Order, error) {
	sqlQuery := `
        SELECT id, customer_id, status, total_amount, currency, items,
               created_at, updated_at, deleted_at
        FROM orders
        WHERE id = $1
    `

	rows, err := r.pool.Query(ctx, sqlQuery, id)
	if err != nil {
		r.logger.Error("failed to get order in repository", zap.Error(err))
		return model.Order{}, err
	}

	order, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[model.Order])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Order{}, model.ErrOrderNotFound
		}
		r.logger.Error("failed to collect order from row", zap.Error(err))
		return model.Order{}, err
	}
	return order, nil
}

func (r *Repository) UpdateOrder(ctx context.Context, tx pgx.Tx, order model.Order) error {
	sqlQuery := `
        UPDATE orders
		SET status = $1,
			updated_at = NOW()
		WHERE id = $2;
    `
	_, err := tx.Exec(ctx, sqlQuery, order.Status, order.ID)
	if err != nil {
		r.logger.Error("failed to update order in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *Repository) DeleteOrder(ctx context.Context, tx pgx.Tx, id string) error {
	sqlQuery := `DELETE FROM orders WHERE id = $1;`
	_, err := tx.Exec(ctx, sqlQuery, id)
	if err != nil {
		r.logger.Error("failed to delete order in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *Repository) DeleteSoftOrder(ctx context.Context, tx pgx.Tx, id string) error {
	sqlQuery := `
        UPDATE orders
		SET status = 'deleted',
			updated_at = NOW(),
			deleted_at = NOW()
		WHERE id = $1;
    `
	_, err := tx.Exec(ctx, sqlQuery, id)
	if err != nil {
		r.logger.Error("failed to delete soft order in repository", zap.Error(err))
		return err
	}
	return nil
}
