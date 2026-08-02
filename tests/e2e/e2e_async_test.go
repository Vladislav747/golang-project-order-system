//go:build e2e_async

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

// OrderAsyncE2ESuite — black-box e2e против уже запущенного сервиса в async-режиме.
// Требует: docker compose up (go-app + postgres + kafka), processing_mode.mode: async.
type OrderAsyncE2ESuite struct {
	testo.Suite[T]
	client *http.Client
}

func TestOrderAsyncE2ESuite(t *testing.T) {
	testo.RunSuite(t, new(OrderAsyncE2ESuite))
}

func (s *OrderAsyncE2ESuite) BeforeAll(t T) {
	s.client = &http.Client{Timeout: 10 * time.Second}
	waitReady(t, s.client)
}

func (s *OrderAsyncE2ESuite) AfterAll(t T) {
	if s.client != nil {
		s.client.CloseIdleConnections()
	}
}

func (s *OrderAsyncE2ESuite) TestCreateOrder_AsyncViaKafka(t T) {
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
	require.Equal(t, http.StatusAccepted, code, "body=%s (сервис должен быть в async)", body)
	require.Equal(t, orderID.String(), string(body))

	require.Eventually(t, func() bool {
		code, body := doJSON(t, s.client, http.MethodGet, "/orders/"+orderID.String(), nil)
		if code != http.StatusOK {
			return false
		}
		var got model.Order
		if err := json.Unmarshal(body, &got); err != nil {
			return false
		}
		return got.ID == orderID && got.Status == model.StatusPending
	}, 15*time.Second, 200*time.Millisecond, "order was not created by kafka consumer")

	code, body = doJSON(t, s.client, http.MethodGet, "/order-events", nil)
	require.Equal(t, http.StatusOK, code, "body=%s", body)

	var events []model.OrderEvent
	require.NoError(t, json.Unmarshal(body, &events))

	requireOrderEvent(t, events, orderID, model.EventCreated, model.SourceKafka)
}
