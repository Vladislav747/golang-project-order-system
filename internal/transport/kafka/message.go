package kafka

import (
	"encoding/json"

	"github.com/google/uuid"
)

type OrderCommandMessage struct {
	Action      string          `json:"action"` // created | updated | deleted
	OrderID     uuid.UUID       `json:"order_id"`
	CustomerID  uuid.UUID       `json:"customer_id,omitempty"`
	Status      string          `json:"status,omitempty"`
	TotalAmount int64           `json:"total_amount,omitempty"`
	Currency    string          `json:"currency,omitempty"`
	Items       json.RawMessage `json:"items,omitempty"`
}
