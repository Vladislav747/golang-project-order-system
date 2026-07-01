package model

import (
	"encoding/json"
	"time"
)

type OrderEvent struct {
	ID int64 `json:"id"`
	OrderID int64 `json:"order_id"`
	EventType string `json:"event_type"`
	Source string `json:"source"`
	Payload json.RawMessage `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}