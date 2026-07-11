package model

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type EventType string
type EventSource string

const (
	EventCreated EventType = "created"
	EventUpdated EventType = "updated"
	EventDeleted EventType = "deleted"
)

const (
	SourceHTTPSync EventSource = "http_sync"
	SourceKafka    EventSource = "kafka"
)

type OrderEvent struct {
	ID        uuid.UUID       `json:"id"`
	OrderID   uuid.UUID       `json:"order_id"`
	EventType EventType       `json:"event_type"`
	Source    EventSource     `json:"source"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

func (t EventType) IsValid() bool {
	switch t {
	case EventCreated, EventUpdated, EventDeleted:
		return true
	default:
		return false
	}
}

func (t EventSource) IsValid() bool {
	switch t {
	case SourceHTTPSync, SourceKafka:
		return true
	default:
		return false
	}
}

func (e OrderEvent) Validate() error {
	if !e.EventType.IsValid() {
		return fmt.Errorf("invalid event_type: %q", e.EventType)
	}
	if !e.Source.IsValid() {
		return fmt.Errorf("invalid source: %q", e.Source)
	}
	return nil
}
