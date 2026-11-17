package deadletter

import (
	"context"
	"fmt"

	"github.com/google/uuid"
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

type ListFilter struct {
	Limit   int
	Offset  int
	Source  *model.DeadLetterSource // nil = любой
	Retried *bool                   // nil = all; false = pending; true = уже retried
}

const deadLetterColumns = `
	id, source, payload, original_topic, original_outbox_id,
	aggregate_type, aggregate_id, event_type,
	original_partition, original_offset,
	error_text, error_class, attempts, first_failed_at,
	created_at, retried_at, retry_error`

func (r *Repository) List(ctx context.Context, f ListFilter) ([]model.DeadLetter, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 200 {
		f.Limit = 200
	}
	if f.Offset < 0 {
		f.Offset = 0
	}

	q := `SELECT ` + deadLetterColumns + ` FROM dead_letters WHERE 1=1`
	// args — значения для $1..$N; n — следующий свободный номер плейсхолдера
	args := make([]any, 0, 4)
	n := 1
	if f.Source != nil {
		q += fmt.Sprintf(` AND source = $%d`, n)
		args = append(args, *f.Source)
		n++
	}
	if f.Retried != nil {
		if *f.Retried {
			q += ` AND retried_at IS NOT NULL`
		} else {
			q += ` AND retried_at IS NULL`
		}
	}
	q += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, n, n+1)
	args = append(args, f.Limit, f.Offset)
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		r.logger.Error("failed to list dead letters", zap.Error(err))
		return nil, err
	}
	items, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.DeadLetter])
	if err != nil {
		r.logger.Error("failed to collect dead letters", zap.Error(err))
		return nil, err
	}
	return items, nil
}

func (r *Repository) GetByID(ctx context.Context, id uuid.UUID) (model.DeadLetter, error) {
	q := `SELECT ` + deadLetterColumns + ` FROM dead_letters WHERE id = $1`
	rows, err := r.pool.Query(ctx, q, id)
	if err != nil {
		r.logger.Error("failed to get dead letter", zap.Error(err))
		return model.DeadLetter{}, err
	}
	letter, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[model.DeadLetter])
	if err != nil {
		return model.DeadLetter{}, err
	}
	return letter, nil
}

func (r *Repository) GetByIDForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (model.DeadLetter, error) {
	q := `SELECT ` + deadLetterColumns + ` FROM dead_letters WHERE id = $1 FOR UPDATE`
	rows, err := tx.Query(ctx, q, id)
	if err != nil {
		r.logger.Error("failed to get dead letter for update", zap.Error(err))
		return model.DeadLetter{}, err
	}
	letter, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[model.DeadLetter])
	if err != nil {
		return model.DeadLetter{}, err
	}
	return letter, nil
}

func (r *Repository) MarkRetried(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		UPDATE dead_letters
		SET retried_at = NOW(),
		    retry_error = NULL
		WHERE id = $1
	`, id)
	if err != nil {
		r.logger.Error("failed to mark dead letter retried", zap.Error(err))
		return err
	}
	return nil
}

func (r *Repository) MarkRetryError(ctx context.Context, tx pgx.Tx, id uuid.UUID, retryErr string) error {
	_, err := tx.Exec(ctx, `
		UPDATE dead_letters
		SET retry_error = $2
		WHERE id = $1
	`, id, retryErr)
	if err != nil {
		r.logger.Error("failed to mark dead letter retry error", zap.Error(err))
		return err
	}
	return nil
}
