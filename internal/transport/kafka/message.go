package kafka

import (
	"encoding/json"

	"github.com/google/uuid"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type orderAction string

const (
	OrderActionCreated orderAction = "created"
	OrderActionUpdated orderAction = "updated"
	OrderActionDeleted orderAction = "deleted"

	HeaderOriginalTopic     = "x-original-topic"
	HeaderOriginalPartition = "x-original-partition"
	HeaderOriginalOffset    = "x-original-offset"
	HeaderError             = "x-error"
	HeaderErrorClass        = "x-error-class"
	HeaderAttempts          = "x-attempts"
	HeaderFirstErrorAt      = "x-first-error-at"
	HeaderOriginalKey       = "x-original-key"
)

type OrderCommandMessage struct {
	Action      orderAction     `json:"action"` // created | updated | deleted
	OrderID     uuid.UUID       `json:"order_id"`
	CustomerID  uuid.UUID       `json:"customer_id,omitempty"`
	Status      model.Status    `json:"status,omitempty"`
	TotalAmount int64           `json:"total_amount,omitempty"`
	Currency    string          `json:"currency,omitempty"`
	Items       json.RawMessage `json:"items,omitempty"`
}
