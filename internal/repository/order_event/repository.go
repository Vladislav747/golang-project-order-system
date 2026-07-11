package order_event

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

func (r *repository) CreateOrderEvent(ctx context.Context, tx pgx.Tx, order model.OrderEvent) error {

	sqlQuery := `
		INSERT INTO order_events (id, order_id, event_type, source, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := tx.Exec(ctx, sqlQuery, order.ID, order.OrderID, order.EventType, order.Source, order.Payload, time.Now())
	if err != nil {
		r.logger.Error("failed to create order in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *repository) GetOrderEvents(ctx context.Context) ([]model.OrderEvent, error) {
	sqlQuery := `SELECT * from order_events`
	rows, err := r.pool.Query(ctx, sqlQuery)
	if err != nil {
		r.logger.Error("failed to get order events in repository", zap.Error(err))
		return nil, err
	}
	events, err := pgx.CollectRows(rows, r.scanOrderEvent)
	if err != nil {
		r.logger.Error("failed to collect events from rows", zap.Error(err))
		return nil, err
	}
	return events, nil
}

func (r *repository) scanOrderEvent(row pgx.CollectableRow) (model.OrderEvent, error) {
	var event model.OrderEvent
	err := row.Scan(
		&event.ID,
		&event.OrderID,
		&event.EventType,
		&event.Source,
		&event.Payload,
		&event.CreatedAt,
	)
	if err != nil {
		r.logger.Error("failed to scan event from row", zap.Error(err))
		return model.OrderEvent{}, err
	}
	return event, nil
}
