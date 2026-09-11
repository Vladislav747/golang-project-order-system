package order_event

import (
	"context"
	"time"

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

func (r *Repository) CreateOrderEvent(ctx context.Context, tx pgx.Tx, order model.OrderEvent) error {

	sqlQuery := sqlx.Rebind(sqlx.DOLLAR, `
		INSERT INTO order_events (id, order_id, event_type, source, payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`)

	_, err := tx.Exec(ctx, sqlQuery, order.ID, order.OrderID, order.EventType, order.Source, order.Payload, time.Now())
	if err != nil {
		r.logger.Error("failed to create order in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *Repository) GetOrderEvents(ctx context.Context) ([]model.OrderEvent, error) {
	sqlQuery := `SELECT * from order_events`
	rows, err := r.pool.Query(ctx, sqlQuery)
	if err != nil {
		r.logger.Error("failed to get order events in repository", zap.Error(err))
		return nil, err
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.OrderEvent])
	if err != nil {
		r.logger.Error("failed to collect events from rows", zap.Error(err))
		return nil, err
	}
	return events, nil
}
