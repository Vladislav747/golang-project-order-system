package kafka_test

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

func TestNewProducer_OK(t *testing.T) {
	t.Parallel()

	p := newTestProducer(t, nil, "orders.create")
	require.NotNil(t, p)
}

func TestSendMessage_OK(t *testing.T) {
	t.Parallel()

	p := newTestProducer(t, nil, "orders.create")

	err := p.SendMessage(kafka.OrderCommandMessage{
		Action:     kafka.OrderActionCreated,
		OrderID:    uuid.New(),
		CustomerID: uuid.New(),
	})
	require.NoError(t, err)
}

func TestPublishEvent_OK(t *testing.T) {
	t.Parallel()

	p := newTestProducer(t, nil, "orders.events")

	err := p.PublishEvent("orders.events", []byte(`{"id":1}`))
	require.NoError(t, err)
}

func TestPublishEvent_BrokerError(t *testing.T) {
	t.Parallel()

	produce := sarama.NewMockProduceResponse(t).
		SetError("orders.events", 0, sarama.ErrNotEnoughReplicas)
	p := newTestProducer(t, produce, "orders.events")

	err := p.PublishEvent("orders.events", []byte(`{"id":1}`))
	require.Error(t, err)
	require.ErrorIs(t, err, sarama.ErrNotEnoughReplicas)
}

func TestPublishEvents_Empty(t *testing.T) {
	t.Parallel()

	p := newTestProducer(t, nil, "orders.events")

	require.NoError(t, p.PublishEvents(nil))
	require.NoError(t, p.PublishEvents([]kafka.PublishMessage{}))
}

func TestPublishEvents_OK(t *testing.T) {
	t.Parallel()

	p := newTestProducer(t, nil, "orders.events")

	err := p.PublishEvents([]kafka.PublishMessage{
		{Topic: "orders.events", Payload: []byte(`{"n":1}`), Metadata: uuid.New()},
		{Topic: "orders.events", Payload: []byte(`{"n":2}`), Metadata: uuid.New()},
	})
	require.NoError(t, err)
}

func TestPublishEvents_MapsProducerErrors(t *testing.T) {
	t.Parallel()

	okTopic := "orders.ok"
	failTopic := "orders.fail"
	produce := sarama.NewMockProduceResponse(t).
		SetError(okTopic, 0, sarama.ErrNoError).
		SetError(failTopic, 0, sarama.ErrNotEnoughReplicas)
	p := newTestProducer(t, produce, okTopic, failTopic)

	okID := uuid.New()
	failID := uuid.New()

	err := p.PublishEvents([]kafka.PublishMessage{
		{Topic: okTopic, Payload: []byte(`ok`), Metadata: okID},
		{Topic: failTopic, Payload: []byte(`fail`), Metadata: failID},
	})
	require.Error(t, err)

	var pubErrs kafka.PublishErrors
	require.ErrorAs(t, err, &pubErrs)
	require.Equal(t, "1 publish errors", pubErrs.Error())
	require.Len(t, pubErrs, 1)
	require.Equal(t, failID, pubErrs[0].Metadata)
	require.ErrorIs(t, pubErrs[0].Err, sarama.ErrNotEnoughReplicas)
}

func TestClose_OK(t *testing.T) {
	t.Parallel()

	broker := newMockBroker(t, sarama.NewMockProduceResponse(t), "orders.create")
	p, err := kafka.NewProducer([]string{broker.Addr()}, "orders.create", zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, p.Close())
}

func newTestProducer(t *testing.T, produce sarama.MockResponse, topics ...string) *kafka.Producer {
	t.Helper()

	broker := newMockBroker(t, produce, topics...)
	p, err := kafka.NewProducer([]string{broker.Addr()}, topics[0], zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func newMockBroker(t *testing.T, produce sarama.MockResponse, topics ...string) *sarama.MockBroker {
	t.Helper()
	if len(topics) == 0 {
		topics = []string{"orders"}
	}

	broker := sarama.NewMockBroker(t, 1)
	t.Cleanup(func() { broker.Close() })

	meta := sarama.NewMockMetadataResponse(t).
		SetBroker(broker.Addr(), broker.BrokerID())
	for _, topic := range topics {
		meta.SetLeader(topic, 0, broker.BrokerID())
	}

	if produce == nil {
		ok := sarama.NewMockProduceResponse(t)
		for _, topic := range topics {
			ok.SetError(topic, 0, sarama.ErrNoError)
		}
		produce = ok
	}

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest":    meta,
		"ProduceRequest":     produce,
	})
	return broker
}
