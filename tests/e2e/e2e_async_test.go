//go:build e2e_async

package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

// OrderAsyncE2ESuite — black-box e2e против уже запущенного сервиса в async-режиме.
// Требует: docker compose up (go-app + postgres + kafka), processing_mode.mode: async.
type OrderAsyncE2ESuite struct {
	suite.Suite
	client *http.Client
}

func TestOrderAsyncE2ESuite(t *testing.T) {
	suite.Run(t, new(OrderAsyncE2ESuite))
}

func (s *OrderAsyncE2ESuite) SetupSuite() {
	s.client = &http.Client{Timeout: 10 * time.Second}
	waitReady(s.T(), s.client)
}

func (s *OrderAsyncE2ESuite) TestCreateOrder_AsyncViaKafka() {
	orderID := uuid.New()
	payload := model.Order{
		ID:          orderID,
		CustomerID:  uuid.New(),
		Status:      "pending",
		TotalAmount: 1500,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}

	code, body := doJSON(s.T(), s.client, http.MethodPost, "/order", payload)
	s.Require().Equal(http.StatusAccepted, code, "body=%s (сервис должен быть в async)", body)
	s.Require().Equal(orderID.String(), string(body))

	s.Require().Eventually(func() bool {
		code, body := doJSON(s.T(), s.client, http.MethodGet, "/orders/"+orderID.String(), nil)
		if code != http.StatusOK {
			return false
		}
		var got model.Order
		if err := json.Unmarshal(body, &got); err != nil {
			return false
		}
		return got.ID == orderID && got.Status == "pending"
	}, 15*time.Second, 200*time.Millisecond, "order was not created by kafka consumer")

	code, body = doJSON(s.T(), s.client, http.MethodGet, "/order-events", nil)
	s.Require().Equal(http.StatusOK, code, "body=%s", body)

	var events []model.OrderEvent
	s.Require().NoError(json.Unmarshal(body, &events))

	var found bool
	for _, e := range events {
		if e.OrderID == orderID && e.EventType == model.EventCreated {
			found = true
			s.Equal(model.SourceKafka, e.Source)
			break
		}
	}
	s.True(found, "kafka created event not found")
}
