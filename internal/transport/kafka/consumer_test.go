//go:build !integration

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type mockOrderHandler struct {
	mock.Mock
}

func (m *mockOrderHandler) HandleCreateOrder(ctx context.Context, msg OrderCommandMessage) error {
	return m.Called(ctx, msg).Error(0)
}

func (m *mockOrderHandler) HandleUpdateOrder(ctx context.Context, msg OrderCommandMessage) error {
	return m.Called(ctx, msg).Error(0)
}

func (m *mockOrderHandler) HandleDeleteOrder(ctx context.Context, msg OrderCommandMessage) error {
	return m.Called(ctx, msg).Error(0)
}

type mockDLQSink struct {
	mock.Mock
}

func (m *mockDLQSink) Send(ctx context.Context, rec DeadLetterRecord) error {
	return m.Called(ctx, rec).Error(0)
}

func newTestConsumerHandler(t *testing.T, maxRetries int, backoff time.Duration) (*consumerGroupHandler, *mockOrderHandler, *mockDLQSink) {
	t.Helper()
	h := &mockOrderHandler{}
	h.Test(t)
	t.Cleanup(func() { h.AssertExpectations(t) })

	sink := &mockDLQSink{}
	sink.Test(t)
	t.Cleanup(func() { sink.AssertExpectations(t) })

	return &consumerGroupHandler{
		handler:      h,
		dlqSink:      sink,
		maxRetries:   maxRetries,
		retryBackoff: backoff,
		logger:       zap.NewNop(),
	}, h, sink
}

func sampleCreatePayload(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(OrderCommandMessage{
		Action:      OrderActionCreated,
		OrderID:     uuid.New(),
		CustomerID:  uuid.New(),
		Status:      model.StatusPending,
		TotalAmount: 100,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	})
	require.NoError(t, err)
	return b
}

func TestProcessMessage_UnmarshalFail_SendsDLQ(t *testing.T) {
	t.Parallel()
	h, orderHandler, sink := newTestConsumerHandler(t, 3, time.Millisecond)

	msg := &sarama.ConsumerMessage{
		Topic:     "orders.create",
		Partition: 1,
		Offset:    42,
		Value:     []byte("{"),
	}

	sink.On("Send", mock.Anything, mock.MatchedBy(func(rec DeadLetterRecord) bool {
		return rec.Topic == "orders.create" &&
			rec.Partition == 1 &&
			rec.Offset == 42 &&
			rec.ErrorClass == model.ErrorClassNonRetryable &&
			rec.Attempts == 0 &&
			len(rec.Payload) > 0
	})).Return(nil).Once()

	err := h.processMessage(context.Background(), msg)
	require.NoError(t, err)
	orderHandler.AssertNotCalled(t, "HandleCreateOrder", mock.Anything, mock.Anything)
}

func TestProcessMessage_Success_NoDLQ(t *testing.T) {
	t.Parallel()
	h, orderHandler, sink := newTestConsumerHandler(t, 3, time.Millisecond)

	payload := sampleCreatePayload(t)
	msg := &sarama.ConsumerMessage{
		Topic: "orders.create",
		Value: payload,
	}

	orderHandler.On("HandleCreateOrder", mock.Anything, mock.Anything).Return(nil).Once()

	err := h.processMessage(context.Background(), msg)
	require.NoError(t, err)
	sink.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
}

func TestProcessMessage_NonRetryable_SendsDLQImmediately(t *testing.T) {
	t.Parallel()
	h, orderHandler, sink := newTestConsumerHandler(t, 3, time.Millisecond)

	payload := sampleCreatePayload(t)
	msg := &sarama.ConsumerMessage{
		Topic:     "orders.create",
		Partition: 0,
		Offset:    7,
		Value:     payload,
	}

	orderHandler.On("HandleCreateOrder", mock.Anything, mock.Anything).
		Return(model.NonRetryable(errors.New("duplicate"))).Once()

	sink.On("Send", mock.Anything, mock.MatchedBy(func(rec DeadLetterRecord) bool {
		return rec.ErrorClass == model.ErrorClassNonRetryable && rec.Attempts == 1
	})).Return(nil).Once()

	err := h.processMessage(context.Background(), msg)
	require.NoError(t, err)
}

func TestProcessMessage_Retryable_ExhaustsThenDLQ(t *testing.T) {
	t.Parallel()
	const maxRetries = 3
	h, orderHandler, sink := newTestConsumerHandler(t, maxRetries, time.Millisecond)

	payload := sampleCreatePayload(t)
	msg := &sarama.ConsumerMessage{
		Topic: "orders.create",
		Value: payload,
	}

	orderHandler.On("HandleCreateOrder", mock.Anything, mock.Anything).
		Return(errors.New("i/o timeout")).Times(maxRetries)

	sink.On("Send", mock.Anything, mock.MatchedBy(func(rec DeadLetterRecord) bool {
		return rec.ErrorClass == model.ErrorClassRetryable && rec.Attempts == maxRetries
	})).Return(nil).Once()

	err := h.processMessage(context.Background(), msg)
	require.NoError(t, err)
}

func TestProcessMessage_SendFails_ReturnsError(t *testing.T) {
	t.Parallel()
	h, _, sink := newTestConsumerHandler(t, 3, time.Millisecond)

	msg := &sarama.ConsumerMessage{
		Topic: "orders.create",
		Value: []byte("{"),
	}

	sinkErr := errors.New("kafka dlq unavailable")
	sink.On("Send", mock.Anything, mock.Anything).Return(sinkErr).Once()

	err := h.processMessage(context.Background(), msg)
	require.ErrorIs(t, err, sinkErr)
}

func TestProcessMessage_UnknownAction_SendsDLQ(t *testing.T) {
	t.Parallel()
	h, orderHandler, sink := newTestConsumerHandler(t, 3, time.Millisecond)

	payload, err := json.Marshal(map[string]any{
		"action":   "explode",
		"order_id": uuid.New().String(),
	})
	require.NoError(t, err)

	msg := &sarama.ConsumerMessage{Topic: "orders.create", Value: payload}

	sink.On("Send", mock.Anything, mock.MatchedBy(func(rec DeadLetterRecord) bool {
		return rec.ErrorClass == model.ErrorClassNonRetryable && rec.Attempts == 0
	})).Return(nil).Once()

	err = h.processMessage(context.Background(), msg)
	require.NoError(t, err)
	orderHandler.AssertNotCalled(t, "HandleCreateOrder", mock.Anything, mock.Anything)
}
