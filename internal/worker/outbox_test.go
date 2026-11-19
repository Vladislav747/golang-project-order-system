//go:build !integration

package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	svcmocks "github.com/Vladislav747/golang-project-order-system/internal/service/mocks"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
	"github.com/Vladislav747/golang-project-order-system/internal/worker/mocks"
)

type stubPublisher struct {
	err error
}

func (s stubPublisher) PublishEvents([]kafka.PublishMessage) error {
	return s.err
}

func createRelayMocks(t *testing.T) (
	*mocks.MockOutboxRepository,
	*mocks.MockDeadLetterRepository,
	*mocks.MockTxManager,
	*svcmocks.MockTx,
) {
	t.Helper()
	return mocks.NewMockOutboxRepository(t),
		mocks.NewMockDeadLetterRepository(t),
		mocks.NewMockTxManager(t),
		svcmocks.NewMockTx(t)
}

func TestOutboxRelay_Fail_Backoff(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repo, deadLetters, txManager, mockTx := createRelayMocks(t)
	id := uuid.New()
	msg := model.OutboxMessage{
		ID:            id,
		AggregateType: model.AggregateOrder,
		AggregateID:   uuid.New(),
		EventType:     model.EventTypeOrderCreated,
		Payload:       []byte(`{"id":1}`),
		Topic:         model.TopicOrderEvents,
		Attempts:      0,
	}

	repo.EXPECT().
		GetExhaustedOutboxMessages(mock.Anything, 3).
		Return(nil, nil)
	repo.EXPECT().
		GetOutboxMessagesUnpublished(mock.Anything, 10, 3).
		Return([]model.OutboxMessage{msg}, nil)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	repo.EXPECT().
		MarkOutboxMessageFailed(mock.Anything, mockTx, id, "broker down", mock.AnythingOfType("time.Time")).
		Return(nil)

	relay := NewOutboxRelay(
		repo,
		stubPublisher{err: kafka.PublishErrors{{Metadata: id, Err: errors.New("broker down")}}},
		zap.NewNop(),
		time.Second,
		10,
		3,
		txManager,
		deadLetters,
		time.Second,
		8*time.Second,
	)
	relay.relayMessages(ctx)
}

func TestOutboxRelay_Fail_MoveToDLQ(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repo, deadLetters, txManager, mockTx := createRelayMocks(t)
	id := uuid.New()
	aggregateID := uuid.New()
	msg := model.OutboxMessage{
		ID:            id,
		AggregateType: model.AggregateOrder,
		AggregateID:   aggregateID,
		EventType:     model.EventTypeOrderCreated,
		Payload:       []byte(`{"id":1}`),
		Topic:         model.TopicOrderEvents,
		Attempts:      2,
	}

	repo.EXPECT().
		GetExhaustedOutboxMessages(mock.Anything, 3).
		Return(nil, nil)
	repo.EXPECT().
		GetOutboxMessagesUnpublished(mock.Anything, 10, 3).
		Return([]model.OutboxMessage{msg}, nil)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	deadLetters.EXPECT().
		Insert(mock.Anything, mockTx, mock.MatchedBy(func(letter model.DeadLetter) bool {
			return letter.Source == model.DeadLetterSourceOutbox &&
				letter.OriginalOutboxID != nil &&
				*letter.OriginalOutboxID == id &&
				letter.Attempts == 3 &&
				letter.ErrorClass == model.ErrorClassRetryable &&
				letter.ErrorText == "broker down"
		})).
		Return(nil)
	repo.EXPECT().
		DeleteOutboxMessage(mock.Anything, mockTx, id).
		Return(nil)

	relay := NewOutboxRelay(
		repo,
		stubPublisher{err: kafka.PublishErrors{{Metadata: id, Err: errors.New("broker down")}}},
		zap.NewNop(),
		time.Second,
		10,
		3,
		txManager,
		deadLetters,
		time.Second,
		8*time.Second,
	)
	relay.relayMessages(ctx)
}
