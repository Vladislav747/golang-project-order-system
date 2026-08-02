//go:build e2e

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

// OrderE2ESuite — black-box e2e против уже запущенного сервиса в sync-режиме.
// Требует: стек поднят с processing_mode.mode: sync (например config/local.yaml).
type OrderE2ESuite struct {
	suite.Suite
	client *http.Client
}

func TestOrderE2ESuite(t *testing.T) {
	suite.Run(t, new(OrderE2ESuite))
}

func (s *OrderE2ESuite) SetupSuite() {
	s.client = &http.Client{Timeout: 10 * time.Second}
	waitReady(s.T(), s.client)
}

func (s *OrderE2ESuite) TestCreateOrder_SyncViaHTTP() {
	orderID := uuid.New()
	payload := model.Order{
		ID:          orderID,
		CustomerID:  uuid.New(),
		Status: model.StatusPending,
		TotalAmount: 1500,
		Currency:    "USD",
		Items:       json.RawMessage(`[{"sku":"A1","qty":1,"price":500},{"sku":"B2","qty":1,"price":500}]`),
	}

	code, body := doJSON(s.T(), s.client, http.MethodPost, "/order", payload)
	s.Require().Equal(http.StatusCreated, code, "body=%s (сервис должен быть в sync)", body)
	s.Require().Equal(orderID.String(), string(body))

	code, body = doJSON(s.T(), s.client, http.MethodGet, "/orders/"+orderID.String(), nil)
	s.Require().Equal(http.StatusOK, code, "body=%s", body)

	var got model.Order
	s.Require().NoError(json.Unmarshal(body, &got))
	s.Equal(orderID, got.ID)
	s.Equal(model.StatusPending, got.Status)

	code, body = doJSON(s.T(), s.client, http.MethodGet, "/order-events", nil)
	s.Require().Equal(http.StatusOK, code, "body=%s", body)

	var events []model.OrderEvent
	s.Require().NoError(json.Unmarshal(body, &events))

	requireOrderEvent(s.T(), events, orderID, model.EventCreated, model.SourceHTTPSync)
}
