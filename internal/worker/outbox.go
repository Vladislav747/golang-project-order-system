package worker

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type OutboxRelay struct {
	repo      OutboxRepository
	producer  *kafka.Producer
	logger    *zap.Logger
	interval  time.Duration
	limit     int
	txManager TxManager
}

type TxManager interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type OutboxRepository interface {
	GetOutboxMessagesUnpublished(ctx context.Context, limit int) ([]model.OutboxMessage, error)
	MarkOutboxMessagePublished(ctx context.Context, tx pgx.Tx, id uuid.UUID) error
	MarkOutboxMessageFailed(ctx context.Context, tx pgx.Tx, id uuid.UUID, lastError string) error
}

func NewOutboxRelay(repo OutboxRepository, producer *kafka.Producer, logger *zap.Logger, interval time.Duration, limit int, txManager TxManager) *OutboxRelay {
	return &OutboxRelay{
		repo:      repo,
		producer:  producer,
		logger:    logger,
		interval:  interval,
		limit:     limit,
		txManager: txManager,
	}
}

func (r *OutboxRelay) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.interval):
			r.relayMessages(ctx)
		}
	}
}

func (r *OutboxRelay) relayMessages(ctx context.Context) {
	// Получаем не опубликованные сообщения из репозитория
	msgs, err := r.repo.GetOutboxMessagesUnpublished(ctx, r.limit)
	if err != nil {
		r.logger.Error("failed to get outbox messages", zap.Error(err))
		return
	}
	// Перебираем все сообщения и публикуем их в Kafka
	for _, msg := range msgs {
		publishErr := r.producer.PublishEvent(string(msg.Topic), msg.Payload)
		if publishErr != nil {
			r.logger.Error("relayMessages. failed to publish outbox event", zap.Error(publishErr))
			err = pgx.BeginFunc(ctx, r.txManager, func(tx pgx.Tx) error {
				return r.repo.MarkOutboxMessageFailed(ctx, tx, msg.ID, publishErr.Error())
			})
			if err != nil {
				r.logger.Error("relayMessages. failed to mark outbox message failed", zap.Error(err))
				continue
			}
			continue
		}

		// Отмечаем сообщение как опубликованное в базе данных
		err = pgx.BeginFunc(ctx, r.txManager, func(tx pgx.Tx) error {
			return r.repo.MarkOutboxMessagePublished(ctx, tx, msg.ID)
		})

		if err != nil {
			r.logger.Error("relayMessages. failed to mark outbox message published", zap.Error(err))
			continue
		}

	}

}
