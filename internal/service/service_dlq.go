package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	deadletter "github.com/Vladislav747/golang-project-order-system/internal/repository/deadletter"
)

var ErrDeadLetterNotFound = errors.New("dead letter not found")

type RepositoryDeadLetter interface {
	List(ctx context.Context, f deadletter.ListFilter) ([]model.DeadLetter, error)
	GetByID(ctx context.Context, id uuid.UUID) (model.DeadLetter, error)
	GetByIDForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (model.DeadLetter, error)
	MarkRetried(ctx context.Context, tx pgx.Tx, id uuid.UUID) error
	MarkRetryError(ctx context.Context, tx pgx.Tx, id uuid.UUID, retryErr string) error
}

func (s *Service) ListDeadLetters(ctx context.Context, f deadletter.ListFilter) ([]model.DeadLetter, error) {
	return s.repositoryDeadLetter.List(ctx, f)
}

func (s *Service) GetDeadLetter(ctx context.Context, id uuid.UUID) (model.DeadLetter, error) {
	letter, err := s.repositoryDeadLetter.GetByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.DeadLetter{}, ErrDeadLetterNotFound
	}
	return letter, err
}

func (s *Service) RetryDeadLetter(ctx context.Context, id uuid.UUID) (model.DeadLetter, error) {
	letter, err := s.GetDeadLetter(ctx, id)
	if err != nil {
		return model.DeadLetter{}, err
	}

	switch letter.Source {
	case model.DeadLetterSourceOutbox:
		err = s.retryOutboxDeadLetter(ctx, letter)
	case model.DeadLetterSourceConsumer:
		err = s.retryKafkaDeadLetter(ctx, letter)
	default:
		return model.DeadLetter{}, fmt.Errorf("unknown dead letter source: %s", letter.Source)
	}

	if err != nil {
		return model.DeadLetter{}, err
	}
	return s.GetDeadLetter(ctx, id)
}

func (s *Service) retryOutboxDeadLetter(ctx context.Context, letter model.DeadLetter) error {
	if letter.AggregateType == nil || letter.AggregateID == nil || letter.EventType == nil {
		return fmt.Errorf("outbox dead letter %s missing aggregate fields", letter.ID)
	}

	// выполняется в рамках одной транзакции
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		locked, err := s.repositoryDeadLetter.GetByIDForUpdate(ctx, tx, letter.ID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrDeadLetterNotFound
			}
			return err
		}

		msg := model.OutboxMessage{
			ID:            uuid.New(),
			AggregateType: *locked.AggregateType,
			AggregateID:   *locked.AggregateID,
			EventType:     *locked.EventType,
			Payload:       locked.Payload,
			Topic:         model.TopicOutbox(locked.OriginalTopic),
		}
		if err := s.repositoryOutbox.CreateOutboxMessage(ctx, tx, msg); err != nil {
			return err
		}
		return s.repositoryDeadLetter.MarkRetried(ctx, tx, locked.ID)
	})
}

func (s *Service) retryKafkaDeadLetter(ctx context.Context, letter model.DeadLetter) error {
	if err := s.producer.PublishEvent(letter.OriginalTopic, letter.Payload); err != nil {
		_ = pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
			return s.repositoryDeadLetter.MarkRetryError(ctx, tx, letter.ID, err.Error())
		})
		return err
	}

	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repositoryDeadLetter.MarkRetried(ctx, tx, letter.ID)
	})
}
