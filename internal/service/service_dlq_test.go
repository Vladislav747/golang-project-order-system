package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	deadletter "github.com/Vladislav747/golang-project-order-system/internal/repository/deadletter"
	"github.com/Vladislav747/golang-project-order-system/internal/service/mocks"
)

type mockDeadLetterRepo struct {
	mock.Mock
}

func (m *mockDeadLetterRepo) List(ctx context.Context, f deadletter.ListFilter) ([]model.DeadLetter, error) {
	args := m.Called(ctx, f)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.DeadLetter), args.Error(1)
}

func (m *mockDeadLetterRepo) GetByID(ctx context.Context, id uuid.UUID) (model.DeadLetter, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(model.DeadLetter), args.Error(1)
}

func (m *mockDeadLetterRepo) GetByIDForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (model.DeadLetter, error) {
	args := m.Called(ctx, tx, id)
	return args.Get(0).(model.DeadLetter), args.Error(1)
}

func (m *mockDeadLetterRepo) MarkRetried(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	return m.Called(ctx, tx, id).Error(0)
}

func (m *mockDeadLetterRepo) MarkRetryError(ctx context.Context, tx pgx.Tx, id uuid.UUID, retryErr string) error {
	return m.Called(ctx, tx, id, retryErr).Error(0)
}

func newDLQService(t *testing.T, dlRepo *mockDeadLetterRepo, outbox *mocks.MockRepositoryOutbox, tx *mocks.MockTxManager) *Service {
	t.Helper()
	return NewService(
		mocks.NewMockRepositoryOrder(t),
		mocks.NewMockRepositoryOrderEvent(t),
		outbox,
		dlRepo,
		tx,
		nil,
		zap.NewNop(),
	)
}

func TestGetDeadLetter_NotFound(t *testing.T) {
	t.Parallel()
	dlRepo := &mockDeadLetterRepo{}
	dlRepo.Test(t)
	t.Cleanup(func() { dlRepo.AssertExpectations(t) })

	id := uuid.New()
	dlRepo.On("GetByID", mock.Anything, id).Return(model.DeadLetter{}, pgx.ErrNoRows).Once()

	svc := newDLQService(t, dlRepo, mocks.NewMockRepositoryOutbox(t), mocks.NewMockTxManager(t))
	_, err := svc.GetDeadLetter(context.Background(), id)
	require.ErrorIs(t, err, ErrDeadLetterNotFound)
}

func TestRetryDeadLetter_Outbox(t *testing.T) {
	t.Parallel()
	dlRepo := &mockDeadLetterRepo{}
	dlRepo.Test(t)
	t.Cleanup(func() { dlRepo.AssertExpectations(t) })

	outbox := mocks.NewMockRepositoryOutbox(t)
	txManager := mocks.NewMockTxManager(t)
	mockTx := mocks.NewMockTx(t)

	aggType := model.AggregateOrder
	aggID := uuid.New()
	eventType := model.EventTypeOrderCreated
	id := uuid.New()
	letter := model.DeadLetter{
		ID:            id,
		Source:        model.DeadLetterSourceOutbox,
		Payload:       json.RawMessage(`{"ok":true}`),
		OriginalTopic: string(model.TopicOrderEvents),
		AggregateType: &aggType,
		AggregateID:   &aggID,
		EventType:     &eventType,
		ErrorText:     "publish failed",
		ErrorClass:    model.ErrorClassRetryable,
		Attempts:      3,
		FirstFailedAt: time.Now().Add(-time.Minute),
	}

	dlRepo.On("GetByID", mock.Anything, id).Return(letter, nil).Once()
	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	dlRepo.On("GetByIDForUpdate", mock.Anything, mockTx, id).Return(letter, nil).Once()
	outbox.EXPECT().CreateOutboxMessage(mock.Anything, mockTx, mock.MatchedBy(func(msg model.OutboxMessage) bool {
		return msg.ID != id &&
			msg.AggregateType == aggType &&
			msg.AggregateID == aggID &&
			msg.EventType == eventType &&
			msg.Topic == model.TopicOrderEvents &&
			string(msg.Payload) == `{"ok":true}`
	})).Return(nil)
	dlRepo.On("MarkRetried", mock.Anything, mockTx, id).Return(nil).Once()

	retriedAt := time.Now()
	retried := letter
	retried.RetriedAt = &retriedAt
	dlRepo.On("GetByID", mock.Anything, id).Return(retried, nil).Once()

	svc := newDLQService(t, dlRepo, outbox, txManager)
	got, err := svc.RetryDeadLetter(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, got.RetriedAt)
}

func TestRetryDeadLetter_UnknownSource(t *testing.T) {
	t.Parallel()
	dlRepo := &mockDeadLetterRepo{}
	dlRepo.Test(t)
	t.Cleanup(func() { dlRepo.AssertExpectations(t) })

	id := uuid.New()
	dlRepo.On("GetByID", mock.Anything, id).Return(model.DeadLetter{
		ID:     id,
		Source: model.DeadLetterSource("weird"),
	}, nil).Once()

	svc := newDLQService(t, dlRepo, mocks.NewMockRepositoryOutbox(t), mocks.NewMockTxManager(t))
	_, err := svc.RetryDeadLetter(context.Background(), id)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown dead letter source")
}

func TestRetryDeadLetter_OutboxMissingAggregate(t *testing.T) {
	t.Parallel()
	dlRepo := &mockDeadLetterRepo{}
	dlRepo.Test(t)
	t.Cleanup(func() { dlRepo.AssertExpectations(t) })

	id := uuid.New()
	dlRepo.On("GetByID", mock.Anything, id).Return(model.DeadLetter{
		ID:     id,
		Source: model.DeadLetterSourceOutbox,
	}, nil).Once()

	svc := newDLQService(t, dlRepo, mocks.NewMockRepositoryOutbox(t), mocks.NewMockTxManager(t))
	_, err := svc.RetryDeadLetter(context.Background(), id)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing aggregate fields")
}
