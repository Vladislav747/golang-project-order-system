package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type DeadLetterSource string

const (
	DeadLetterSourceOutbox   DeadLetterSource = "outbox_relay"
	DeadLetterSourceConsumer DeadLetterSource = "kafka_consumer"
)

type DeadLetterErrorClass string

const (
	ErrorClassRetryable    DeadLetterErrorClass = "retryable"
	ErrorClassNonRetryable DeadLetterErrorClass = "non_retryable"
)

type DeadLetter struct {
	ID                uuid.UUID            `json:"id"`
	Source            DeadLetterSource     `json:"source"`
	Payload           json.RawMessage      `json:"payload"`
	OriginalTopic     string               `json:"original_topic"`
	OriginalOutboxID  *uuid.UUID           `json:"original_outbox_id,omitempty"`
	AggregateType     *AggregateType       `json:"aggregate_type,omitempty"`
	AggregateID       *uuid.UUID           `json:"aggregate_id,omitempty"`
	EventType         *EventTypeOutbox     `json:"event_type,omitempty"`
	OriginalPartition *int32               `json:"original_partition,omitempty"`
	OriginalOffset    *int64               `json:"original_offset,omitempty"`
	ErrorText         string               `json:"error_text"`
	ErrorClass        DeadLetterErrorClass `json:"error_class"`
	Attempts          int                  `json:"attempts"`
	FirstFailedAt     time.Time            `json:"first_failed_at"`
	CreatedAt         time.Time            `json:"created_at"`
	RetriedAt         *time.Time           `json:"retried_at,omitempty"`
	RetryError        *string              `json:"retry_error,omitempty"`
}
