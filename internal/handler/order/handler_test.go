package Handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/handler/order/mocks"
	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

func TestNewHandler(t *testing.T) {
	t.Parallel()
	_, handler := createMocks(t)

	if handler == nil {
		t.Fatal("expected handler instance")
	}
}

func TestCreateOrder(t *testing.T) {
	t.Parallel()
	mockSvc, mockHandler := createMocks(t)

	mockSvc.EXPECT().
		CreateOrder(mock.Anything, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == model.StatusPending && o.TotalAmount == 1500 && len(o.Items) > 0
		})).
		Return(nil)

	body := `{
		"customer_id": "6ba7b810-9dad-11d1-80b4-00c04fd43023",
		"status": "pending",
		"total_amount": 1500,
		"currency": "USD",
		"items": [{"sku":"A1","qty":1}]
	}`

	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mockHandler.CreateOrder(rr, req)
	require.Equal(t, http.StatusCreated, rr.Code)
	require.NotEmpty(t, rr.Body.String())

}

func TestDeleteOrderHard(t *testing.T) {
	t.Parallel()
	mockSvc, mockHandler := createMocks(t)

	orderID := "695d6407-ecaa-4e1b-a943-5f90e019e615"

	mockSvc.EXPECT().
		DeleteOrder(mock.Anything, orderID).
		Return(nil)

	req := httptest.NewRequest(http.MethodDelete, "/orders/hard/"+orderID, nil)
	req.SetPathValue("id", orderID)
	rr := httptest.NewRecorder()

	mockHandler.DeleteOrder(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "Order deleted", rr.Body.String())

}

func TestGetOrder(t *testing.T) {
	t.Parallel()
	mockSvc, mockHandler := createMocks(t)

	orderID := "695d6407-ecaa-4e1b-a943-5f90e019e615"
	id := uuid.MustParse(orderID)

	mockSvc.EXPECT().
		GetOrder(mock.Anything, orderID).
		Return(model.Order{
			ID:          id,
			CustomerID:  uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd43023"),
			Status:      model.StatusPending,
			TotalAmount: 1500,
			Currency:    "USD",
			Items:       json.RawMessage(`[{"sku":"A1","qty":1}]`),
		}, nil)

	req := httptest.NewRequest(http.MethodGet, "/orders/"+orderID, nil)
	req.SetPathValue("id", orderID)
	rr := httptest.NewRecorder()

	mockHandler.GetOrder(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), orderID)
}

func TestGetOrders(t *testing.T) {
	t.Parallel()
	mockSvc, mockHandler := createMocks(t)

	orderID := "695d6407-ecaa-4e1b-a943-5f90e019e615"
	id := uuid.MustParse(orderID)

	mockSvc.EXPECT().
		GetOrders(mock.Anything).
		Return([]model.Order{{
			ID:          id,
			CustomerID:  uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd43023"),
			Status:      model.StatusPending,
			TotalAmount: 1500,
			Currency:    "USD",
			Items:       json.RawMessage(`[{"sku":"A1","qty":1}]`),
		}}, nil)

	req := httptest.NewRequest(http.MethodGet, "/orders", nil)
	rr := httptest.NewRecorder()

	mockHandler.GetOrders(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.NotEmpty(t, rr.Body.String())
}

func TestUpdateOrder(t *testing.T) {
	t.Parallel()
	mockSvc, mockHandler := createMocks(t)

	orderID := "695d6407-ecaa-4e1b-a943-5f90e019e615"

	mockSvc.EXPECT().
		UpdateOrder(mock.Anything, mock.MatchedBy(func(o model.Order) bool {
			return o.Status == model.StatusPending && o.TotalAmount == 1500 && len(o.Items) > 0
		})).
		Return(nil)

	body := `{
		"id": "` + orderID + `",
		"customer_id": "6ba7b810-9dad-11d1-80b4-00c04fd43023",
		"status": "pending",
		"total_amount": 1500,
		"currency": "USD",
		"items": [{"sku":"A1","qty":1}]
	}`

	req := httptest.NewRequest(http.MethodPut, "/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mockHandler.UpdateOrder(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.NotEmpty(t, rr.Body.String())
}

func createMocks(t *testing.T) (*mocks.MockService, *Handler) {
	t.Helper()

	mockSvc := mocks.NewMockService(t)

	cfg := &config.Config{
		ProcessingMode: config.ProcessingMode{Mode: config.OrderModeSync},
		HttpServer: config.HttpServer{
			RequestTimeout: 2 * time.Second,
		},
	}

	provider := config.NewProvider(cfg)

	mockHandler := NewHandler(mockSvc, zap.NewNop(), provider)

	return mockSvc, mockHandler
}
