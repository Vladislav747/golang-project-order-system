package service

import (
	"encoding/json"

	"github.com/google/uuid"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

func buildOutboxMessage(orderID uuid.UUID, aggregateType model.AggregateType, topic model.TopicOutbox, eventType model.EventTypeOutbox, payload any) (model.OutboxMessage, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return model.OutboxMessage{}, err
	}

	return model.OutboxMessage{
		ID:            uuid.New(),
		AggregateType: aggregateType,
		AggregateID:   orderID,
		EventType:     eventType,
		Topic:         topic,
		Payload:       data,
	}, nil
}
