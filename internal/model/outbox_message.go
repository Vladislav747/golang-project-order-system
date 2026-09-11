package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EventTypeOutbox string

type AggregateType string

type TopicOutbox string

const AggregateOrder AggregateType = "order"

const (
	EventTypeOrderCreated EventTypeOutbox = "order.created"
	EventTypeOrderUpdated EventTypeOutbox = "order.updated"
	EventTypeOrderDeleted EventTypeOutbox = "order.deleted"
)

const TopicOrderEvents TopicOutbox = "orders.events"

type OutboxMessage struct {
	ID            uuid.UUID       `json:"id"`
	AggregateType AggregateType   `json:"aggregate_type"`
	AggregateID   uuid.UUID       `json:"aggregate_id"`
	EventType     EventTypeOutbox `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	Topic         TopicOutbox     `json:"topic"`
	CreatedAt     time.Time       `json:"created_at"`
	PublishedAt   *time.Time      `json:"published_at,omitempty"`
	Attempts      int             `json:"attempts"`
	LastError     *string         `json:"last_error,omitempty"`
}
