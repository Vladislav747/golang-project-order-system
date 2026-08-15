package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var ErrOrderNotFound = errors.New("order not found")

// Status mirrors order.v1.StatusType numeric values.
type Status int32

const (
	StatusUnspecified Status = 0
	StatusCreated     Status = 1
	StatusPending     Status = 2
	StatusCompleted   Status = 3
	StatusFailed      Status = 4
	StatusDeleted     Status = 5
)

func (s Status) IsValid() bool {
	switch s {
	case StatusCreated, StatusPending, StatusCompleted, StatusFailed, StatusDeleted:
		return true
	default:
		return false
	}
}

func (s Status) String() string {
	switch s {
	case StatusCreated:
		return "created"
	case StatusPending:
		return "pending"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusDeleted:
		return "deleted"
	default:
		return "unspecified"
	}
}

func ParseStatus(s string) (Status, error) {
	switch s {
	case "created":
		return StatusCreated, nil
	case "pending":
		return StatusPending, nil
	case "completed":
		return StatusCompleted, nil
	case "failed":
		return StatusFailed, nil
	case "deleted":
		return StatusDeleted, nil
	default:
		return StatusUnspecified, fmt.Errorf("invalid status: %q", s)
	}
}

func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *Status) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := ParseStatus(raw)
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

func (s Status) Value() (driver.Value, error) {
	if s == StatusUnspecified {
		return nil, errors.New("status is required")
	}
	return s.String(), nil
}

func (s *Status) Scan(src any) error {
	if src == nil {
		*s = StatusUnspecified
		return nil
	}
	switch v := src.(type) {
	case string:
		parsed, err := ParseStatus(v)
		if err != nil {
			return err
		}
		*s = parsed
		return nil
	case []byte:
		parsed, err := ParseStatus(string(v))
		if err != nil {
			return err
		}
		*s = parsed
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Status", src)
	}
}

type Order struct {
	ID          uuid.UUID       `json:"id"`
	CustomerID  uuid.UUID       `json:"customer_id"`
	Status      Status          `json:"status"`
	TotalAmount int64           `json:"total_amount"`
	Currency    string          `json:"currency"`
	Items       json.RawMessage `json:"items"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	DeletedAt   *time.Time      `json:"deleted_at,omitempty"`
}
