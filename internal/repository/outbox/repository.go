package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type Pool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Repository struct {
	pool   Pool
	logger *zap.Logger
}

func NewRepository(pool Pool, logger *zap.Logger) *Repository {
	return &Repository{pool: pool, logger: logger}
}

func (r *Repository) CreateOutboxMessage(ctx context.Context, tx pgx.Tx, message model.OutboxMessage) error {

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

func (r *Repository) GetOutboxMessagesUnpublished(ctx context.Context, limit int, maxAttempts int) ([]model.OutboxMessage, error) {

	sqlQuery := `
        SELECT *
        FROM outbox
        WHERE published_at is null
		AND attempts < $2
		ORDER BY created_at ASC
		LIMIT $1
    `

	rows, err := r.pool.Query(ctx, sqlQuery, limit, maxAttempts)
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

func (r *Repository) MarkOutboxMessagePublished(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {

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

func (r *Repository) MarkOutboxMessageFailed(ctx context.Context, tx pgx.Tx, id uuid.UUID, lastError string) error {

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

func (r *Repository) CleanOutboxMessages(ctx context.Context, tx pgx.Tx, retention time.Duration, maxAttempts int) error {

	sqlQuery := `
        DELETE FROM outbox
		WHERE (published_at IS NOT NULL AND published_at < NOW() - $1::interval)
   			OR (published_at IS NULL AND attempts >= $2)
    `

	tag, err := tx.Exec(ctx, sqlQuery, retention, maxAttempts)
	if err != nil {
		r.logger.Error("failed to clean published messages in repository", zap.Error(err))
		return err
	}
	if tag.RowsAffected() > 0 {
		r.logger.Info("outbox messages cleaned", zap.Int64("deleted", tag.RowsAffected()))
	}
	return nil
}
