package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
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

func (r *repository) CreateOutboxMessage(ctx context.Context, tx pgx.Tx, message model.OutboxMessage) error {

	sqlQuery := sqlx.Rebind(sqlx.DOLLAR, `
		INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
		VALUES (?, ?, ?, ?, ?, ?)
	`)

	_, err := tx.Exec(ctx, sqlQuery, message.ID, message.AggregateType, message.AggregateID, message.EventType, message.Topic, message.Payload)
	if err != nil {
		r.logger.Error("failed to create outbox message in repository", zap.Error(err))
		return err
	}
	return nil
}
