package model

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrOrderNotFound = errors.New("order not found")

type Order struct {
	ID          uuid.UUID       `json:"id"`
	CustomerID  uuid.UUID       `json:"customer_id"`
	Status      string          `json:"status"`
	TotalAmount int64           `json:"total_amount"`
	Currency    string          `json:"currency"`
	Items       json.RawMessage `json:"items"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	DeletedAt   *time.Time      `json:"deleted_at,omitempty"`
}
