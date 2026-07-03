package kafka

import (
	"encoding/json"

	"github.com/google/uuid"
)

type CreateOrderMessage struct {
	OrderID     uuid.UUID       `json:"order_id"`
	CustomerID  uuid.UUID       `json:"customer_id"`
	Status      string          `json:"status"`
	TotalAmount int64           `json:"total_amount"`
	Currency    string          `json:"currency"`
	Items       json.RawMessage `json:"items"`
}
