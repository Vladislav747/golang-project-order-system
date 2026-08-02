//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ozontech/testo"
	"github.com/stretchr/testify/require"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type T = *testo.T

// OrderE2ESuite — black-box e2e против уже запущенного сервиса в sync-режиме.
// Требует: стек поднят с processing_mode.mode: sync (например config/local.yaml).
type OrderE2ESuite struct {
	testo.Suite[T]
	client *http.Client
}

func TestOrderE2ESuite(t *testing.T) {
	testo.RunSuite(t, new(OrderE2ESuite))
}

func (s *OrderE2ESuite) BeforeAll(t T) {
	s.client = &http.Client{Timeout: 10 * time.Second}
	waitReady(t, s.client)
}

func (s *OrderE2ESuite) AfterAll(t T) {
	if s.client != nil {
		s.client.CloseIdleConnections()
	}
}

func (s *OrderE2ESuite) TestCreateOrder_SyncViaHTTP(t T) {
	orderID := uuid.New()
	payload := model.Order{
		ID:          orderID,
		CustomerID:  uuid.New(),
		Status:      model.StatusPending,
		TotalAmount: 1500,
		Currency:    "USD",
		Items:       json.RawMessage(`[{"sku":"A1","qty":1,"price":500},{"sku":"B2","qty":1,"price":500}]`),
	}

	code, body := doJSON(t, s.client, http.MethodPost, "/order", payload)
	require.Equal(t, http.StatusCreated, code, "body=%s (сервис должен быть в sync)", body)
	require.Equal(t, orderID.String(), string(body))

	code, body = doJSON(t, s.client, http.MethodGet, "/orders/"+orderID.String(), nil)
	require.Equal(t, http.StatusOK, code, "body=%s", body)

	var got model.Order
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, orderID, got.ID)
	require.Equal(t, model.StatusPending, got.Status)

	code, body = doJSON(t, s.client, http.MethodGet, "/order-events", nil)
	require.Equal(t, http.StatusOK, code, "body=%s", body)

	var events []model.OrderEvent
	require.NoError(t, json.Unmarshal(body, &events))

	requireOrderEvent(t, events, orderID, model.EventCreated, model.SourceHTTPSync)
}
