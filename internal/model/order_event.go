package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type OrderEvent struct {
	ID uuid.UUID `json:"id"`
	OrderID int64 `json:"order_id"`
	EventType string `json:"event_type"`
	Source string `json:"source"`
	Payload json.RawMessage `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}