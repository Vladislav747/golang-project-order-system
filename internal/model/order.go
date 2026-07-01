package model

import (
	"encoding/json"
	"time"
)

type Order struct {
	ID int64 `json:"id"`
	CustomerID int64 `json:"customer_id"`
	Status string `json:"status"`
	TotalAmount int64 `json:"total_amount"`
	Currency string `json:"currency"`
	Items json.RawMessage `json:"items"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	DeletedAt time.Time `json:"deleted_at"`
}