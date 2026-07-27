package service

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

func buildOrderEvent(orderID uuid.UUID, eventType model.EventType, source model.EventSource, payload any) (model.OrderEvent, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return model.OrderEvent{}, err
	}

	return model.OrderEvent{
		ID:        uuid.New(),
		OrderID:   orderID,
		EventType: eventType,
		Source:    source,
		Payload:   data,
	}, nil
}

func (s *Service) GetOrderEvents(ctx context.Context) ([]model.OrderEvent, error) {
	events, err := s.repositoryOrderEvent.GetOrderEvents(ctx)
	if err != nil {
		return nil, err
	}
	return events, nil
}
