package deadletter

import (
	"context"

	"github.com/jackc/pgx/v5"
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

func (r *Repository) Insert(ctx context.Context, tx pgx.Tx, letter model.DeadLetter) error {
	sqlQuery := `
		INSERT INTO dead_letters (
			source, payload, original_topic,
			original_outbox_id, aggregate_type, aggregate_id, event_type,
			original_partition, original_offset,
			error_text, error_class, attempts, first_failed_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6, $7,
			$8, $9,
			$10, $11, $12, $13
		)
		ON CONFLICT DO NOTHING
	`
	_, err := tx.Exec(ctx, sqlQuery,
		letter.Source,
		letter.Payload,
		letter.OriginalTopic,
		letter.OriginalOutboxID,
		letter.AggregateType,
		letter.AggregateID,
		letter.EventType,
		letter.OriginalPartition,
		letter.OriginalOffset,
		letter.ErrorText,
		letter.ErrorClass,
		letter.Attempts,
		letter.FirstFailedAt,
	)
	if err != nil {
		r.logger.Error("failed to insert dead letter", zap.Error(err))
		return err
	}
	return nil
}
