//go:build !integration

package service

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

func TestCreateOrderFromKafka_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, repoEvent, repoOutbox, txManager, mockTx, svc := createMocks(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	expected := model.Order{Status: model.StatusPending}

	repoOrder.EXPECT().
		CreateOrder(mock.Anything, mockTx, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == model.StatusPending
		})).
		Return(nil)

	repoEvent.EXPECT().
		CreateOrderEvent(mock.Anything, mockTx, mock.MatchedBy(func(e model.OrderEvent) bool {
			return e.EventType == model.EventCreated && e.Source == model.SourceKafka
		})).
		Return(nil)

	repoOutbox.EXPECT().
		CreateOutboxMessage(mock.Anything, mockTx, mock.MatchedBy(func(m model.OutboxMessage) bool {
			return m.EventType == model.EventTypeOrderCreated &&
				m.AggregateType == model.AggregateOrder &&
				m.Topic == model.TopicOrderEvents
		})).
		Return(nil)

	err := svc.CreateOrderFromKafka(ctx, expected)
	require.NoError(t, err)
}

func TestUpdateOrderFromKafka_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, repoEvent, repoOutbox, txManager, mockTx, svc := createMocks(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	order := model.Order{Status: model.StatusCompleted}

	repoOrder.EXPECT().
		UpdateOrder(mock.Anything, mockTx, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == model.StatusCompleted
		})).
		Return(nil)

	repoEvent.EXPECT().
		CreateOrderEvent(mock.Anything, mockTx, mock.MatchedBy(func(e model.OrderEvent) bool {
			return e.EventType == model.EventUpdated && e.Source == model.SourceKafka
		})).
		Return(nil)

	repoOutbox.EXPECT().
		CreateOutboxMessage(mock.Anything, mockTx, mock.MatchedBy(func(m model.OutboxMessage) bool {
			return m.EventType == model.EventTypeOrderUpdated &&
				m.AggregateType == model.AggregateOrder &&
				m.Topic == model.TopicOrderEvents
		})).
		Return(nil)

	err := svc.UpdateOrderFromKafka(ctx, order)
	require.NoError(t, err)
}

func TestDeleteOrderFromKafka_RepositoryCalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOrder, repoEvent, repoOutbox, txManager, mockTx, svc := createMocks(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	orderID := "6ba7b810-9dad-11d1-80b4-00c04fd43023"

	repoOrder.EXPECT().
		DeleteSoftOrder(mock.Anything, mockTx, orderID).
		Return(nil)

	repoEvent.EXPECT().
		CreateOrderEvent(mock.Anything, mockTx, mock.MatchedBy(func(e model.OrderEvent) bool {
			return e.EventType == model.EventDeleted &&
				e.Source == model.SourceKafka &&
				e.OrderID == uuid.MustParse(orderID)
		})).
		Return(nil)

	repoOutbox.EXPECT().
		CreateOutboxMessage(mock.Anything, mockTx, mock.MatchedBy(func(m model.OutboxMessage) bool {
			return m.EventType == model.EventTypeOrderDeleted &&
				m.AggregateType == model.AggregateOrder &&
				m.Topic == model.TopicOrderEvents &&
				m.AggregateID == uuid.MustParse(orderID)
		})).
		Return(nil)

	err := svc.DeleteOrderFromKafka(ctx, orderID)
	require.NoError(t, err)
}
