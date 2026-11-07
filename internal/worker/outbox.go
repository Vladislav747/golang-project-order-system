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
	repo           OutboxRepository
	producer       EventPublisher
	logger         *zap.Logger
	interval       atomic.Int64
	limit          atomic.Int64
	maxAttempts    atomic.Int64
	txManager      TxManager
	deadLetterRepo DeadLetterRepository
	backoffInitial atomic.Int64
	backoffMax     atomic.Int64
}

type TxManager interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type EventPublisher interface {
	PublishEvents(events []kafka.PublishMessage) error
}

type OutboxRepository interface {
	GetOutboxMessagesUnpublished(ctx context.Context, limit int, maxAttempts int) ([]model.OutboxMessage, error)
	MarkOutboxMessagePublished(ctx context.Context, tx pgx.Tx, id uuid.UUID) error
	MarkOutboxMessageFailed(ctx context.Context, tx pgx.Tx, id uuid.UUID, lastError string, nextRetryAt time.Time) error
	DeleteOutboxMessage(ctx context.Context, tx pgx.Tx, id uuid.UUID) error
	GetExhaustedOutboxMessages(ctx context.Context, maxAttempts int) ([]model.OutboxMessage, error)
}

type DeadLetterRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, letter model.DeadLetter) error
}

func NewOutboxRelay(
	repo OutboxRepository,
	producer EventPublisher,
	logger *zap.Logger,
	interval time.Duration,
	limit int,
	maxAttempts int,
	txManager TxManager,
	deadLetterRepo DeadLetterRepository,
	backoffInitial time.Duration,
	backoffMax time.Duration,
) *OutboxRelay {
	r := &OutboxRelay{
		repo:           repo,
		producer:       producer,
		logger:         logger,
		txManager:      txManager,
		deadLetterRepo: deadLetterRepo,
	}
	r.interval.Store(int64(interval))
	r.limit.Store(int64(limit))
	r.maxAttempts.Store(int64(maxAttempts))
	r.backoffInitial.Store(int64(backoffInitial))
	r.backoffMax.Store(int64(backoffMax))
	return r
}

func (r *OutboxRelay) Run(ctx context.Context) {
	timer := time.NewTimer(time.Duration(r.interval.Load()))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			r.relayMessages(ctx)
			timer.Reset(time.Duration(r.interval.Load()))
		}
	}
}

func (r *OutboxRelay) relayMessages(ctx context.Context) {
	exhausted, err := r.repo.GetExhaustedOutboxMessages(ctx, int(r.maxAttempts.Load()))
	if err != nil {
		r.logger.Error("failed to get exhausted outbox messages", zap.Error(err))

	} else if len(exhausted) > 0 {
		err = pgx.BeginFunc(ctx, r.txManager, func(tx pgx.Tx) error {
			for _, msg := range exhausted {
				errorText := "exhausted attempts"
				if msg.LastError != nil {
					errorText = *msg.LastError
				}
				letter := model.DeadLetter{
					Source:           model.DeadLetterSourceOutbox,
					Payload:          msg.Payload,
					OriginalTopic:    string(msg.Topic),
					OriginalOutboxID: &msg.ID,
					AggregateType:    &msg.AggregateType,
					AggregateID:      &msg.AggregateID,
					EventType:        &msg.EventType,
					ErrorText:        errorText,
					ErrorClass:       model.ErrorClassRetryable,
					Attempts:         msg.Attempts,
					FirstFailedAt:    time.Now(),
				}
				if err := r.deadLetterRepo.Insert(ctx, tx, letter); err != nil {
					return err
				}
				if err := r.repo.DeleteOutboxMessage(ctx, tx, msg.ID); err != nil {
					return err
				}
				r.logger.Warn("exhausted outbox message moved to DLQ",
					zap.String("id", msg.ID.String()),
				)
			}
			return nil
		})
		if err != nil {
			r.logger.Error("failed to move exhausted outbox messages to DLQ", zap.Error(err))
		}
	}

	// Получаем неопубликованные сообщения из репозитория
	msgs, err := r.repo.GetOutboxMessagesUnpublished(ctx, int(r.limit.Load()), int(r.maxAttempts.Load()))
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
				attemptsAfter := msg.Attempts + 1
				// попытки исчерпаны — уводим в DLQ, а не ретраим
				if attemptsAfter >= int(r.maxAttempts.Load()) {
					letter := model.DeadLetter{
						Source:           model.DeadLetterSourceOutbox,
						Payload:          msg.Payload,
						OriginalTopic:    string(msg.Topic),
						OriginalOutboxID: &msg.ID,
						AggregateType:    &msg.AggregateType,
						AggregateID:      &msg.AggregateID,
						EventType:        &msg.EventType,
						ErrorText:        pubErr.Error(),
						ErrorClass:       model.ErrorClassRetryable,
						Attempts:         attemptsAfter,
						FirstFailedAt:    time.Now(),
					}

					if err := r.deadLetterRepo.Insert(ctx, tx, letter); err != nil {
						return err
					}
					if err := r.repo.DeleteOutboxMessage(ctx, tx, msg.ID); err != nil {
						return err
					}
					r.logger.Warn("outbox message moved to DLQ",
						zap.String("id", msg.ID.String()),
						zap.Error(pubErr),
					)
					continue
				}
				next := nextRetryAt(
					time.Now(),
					attemptsAfter,
					time.Duration(r.backoffInitial.Load()),
					time.Duration(r.backoffMax.Load()),
				)
				if markErr := r.repo.MarkOutboxMessageFailed(ctx, tx, msg.ID, pubErr.Error(), next); markErr != nil {
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

func (r *OutboxRelay) SetMaxAttempts(n int) {
	r.maxAttempts.Store(int64(n))
}

func (r *OutboxRelay) SetBackoffInitial(d time.Duration) {
	r.backoffInitial.Store(int64(d))
}

func (r *OutboxRelay) SetBackoffMax(d time.Duration) {
	r.backoffMax.Store(int64(d))
}
