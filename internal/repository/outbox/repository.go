package outbox

import (
	"context"

	"github.com/google/uuid"
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

func (r *repository) GetOutboxMessagesUnpublished(ctx context.Context, limit int) ([]model.OutboxMessage, error) {

	sqlQuery := `
        SELECT *
        FROM outbox
        WHERE published_at is null
		ORDER BY created_at ASC
		LIMIT $1
    `

	rows, err := r.pool.Query(ctx, sqlQuery, limit)
	if err != nil {
		r.logger.Error("failed to get unpublished outbox messages in repository", zap.Error(err))
		return nil, err
	}
	outboxMessages, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.OutboxMessage])
	if err != nil {
		r.logger.Error("failed to collect outboxMessages from rows", zap.Error(err))
		return nil, err
	}
	return outboxMessages, nil
}

func (r *repository) MarkOutboxMessagePublished(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {

	sqlQuery := `
        UPDATE outbox
		SET published_at = NOW(),
			last_error = NULL
		WHERE id = $1;
    `

	_, err := tx.Exec(ctx, sqlQuery, id)
	if err != nil {
		r.logger.Error("failed to update outbox message published in repository", zap.Error(err))
		return err
	}
	return nil
}

func (r *repository) MarkOutboxMessageFailed(ctx context.Context, tx pgx.Tx, id uuid.UUID, lastError string) error {

	sqlQuery := `
        UPDATE outbox
		SET attempts = attempts + 1,
			last_error = $2
		WHERE id = $1;
    `

	_, err := tx.Exec(ctx, sqlQuery, id, lastError)
	if err != nil {
		r.logger.Error("failed to update outbox message failed in repository", zap.Error(err))
		return err
	}
	return nil
}
