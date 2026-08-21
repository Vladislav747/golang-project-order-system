package worker

import (
	"context"
	"errors"
	"sync/atomic"
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
	interval  atomic.Int64
	limit     atomic.Int64
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
	r := &OutboxRelay{
		repo:      repo,
		producer:  producer,
		logger:    logger,
		txManager: txManager,
	}
	r.interval.Store(int64(interval))
	r.limit.Store(int64(limit))
	return r
}

func (r *OutboxRelay) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(r.interval.Load())):
			r.relayMessages(ctx)
		}
	}
}

func (r *OutboxRelay) relayMessages(ctx context.Context) {
	// Получаем не опубликованные сообщения из репозитория
	msgs, err := r.repo.GetOutboxMessagesUnpublished(ctx, int(r.limit.Load()))
	if err != nil {
		r.logger.Error("failed to get outbox messages", zap.Error(err))
		return
	}
	if len(msgs) == 0 {
		return
	}

	// собираем батч сообщений для публикации

	events := make([]kafka.PublishMessage, 0, len(msgs))

	for _, msg := range msgs {
		events = append(events, kafka.PublishMessage{
			Topic:    string(msg.Topic),
			Payload:  msg.Payload,
			Metadata: msg.ID,
		})
	}

	err = r.producer.PublishEvents(events)

	failed := make(map[uuid.UUID]error)
	if err != nil {
		var batchErr kafka.PublishErrors
		if errors.As(err, &batchErr) {
			// pe - 'publish error'
			for _, pe := range batchErr {
				id, ok := pe.Metadata.(uuid.UUID)
				if !ok {
					continue
				}
				failed[id] = pe.Err
			}
		} else {
			// упал весь батч
			for _, msg := range msgs {
				failed[msg.ID] = err
			}
		}
	}

	// 4) один BeginFunc — mark всех
	err = pgx.BeginFunc(ctx, r.txManager, func(tx pgx.Tx) error {
		for _, msg := range msgs {
			if pubErr, ok := failed[msg.ID]; ok {
				if markErr := r.repo.MarkOutboxMessageFailed(ctx, tx, msg.ID, pubErr.Error()); markErr != nil {
					return markErr
				}
				continue
			}
			if markErr := r.repo.MarkOutboxMessagePublished(ctx, tx, msg.ID); markErr != nil {
				return markErr
			}
		}
		return nil
	})
	if err != nil {
		r.logger.Error("relayMessages. failed to mark outbox messages", zap.Error(err))
	}

}

func (r *OutboxRelay) SetLimit(n int) {
	r.limit.Store(int64(n))
}

func (r *OutboxRelay) SetInterval(d time.Duration) {
	r.interval.Store(int64(d))
}
