//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/handler"
	orderhandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order"
	ordereventhandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order_event"
	"github.com/Vladislav747/golang-project-order-system/internal/model"
	repositoryOrder "github.com/Vladislav747/golang-project-order-system/internal/repository/order"
	repositoryOrderEvent "github.com/Vladislav747/golang-project-order-system/internal/repository/order_event"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
)

// OrderE2ESuite — sync e2e без моков: реальный Postgres (testcontainers) + HTTP.
type OrderE2ESuite struct {
	suite.Suite

	pool *pgxpool.Pool
	mux  *http.ServeMux
	svc  *service.Service
}

func TestOrderE2ESuite(t *testing.T) {
	suite.Run(t, new(OrderE2ESuite))
}

func (s *OrderE2ESuite) SetupSuite() {
	t := s.T()
	logger := zap.NewNop()

	s.pool = setupPostgres(t)

	s.svc = service.NewService(
		repositoryOrder.NewRepository(s.pool, logger),
		repositoryOrderEvent.NewRepository(s.pool, logger),
		s.pool,
		nil,
		logger,
	)

	orderHandler := orderhandler.NewHandler(
		s.svc,
		logger,
		5*time.Second,
		config.ProcessingMode{Mode: config.OrderModeSync},
	)
	orderEventHandler := ordereventhandler.NewHandler(s.svc, logger, 5*time.Second)

	s.mux = http.NewServeMux()
	handler.RegisterRoutes(s.mux, orderHandler, orderEventHandler)
}

func (s *OrderE2ESuite) TestCreateOrder_SyncViaHTTP() {
	orderID := uuid.New()
	payload := model.Order{
		ID:          orderID,
		CustomerID:  uuid.New(),
		Status:      "pending",
		TotalAmount: 1500,
		Currency:    "USD",
		Items:       json.RawMessage(`[]`),
	}
	body, err := json.Marshal(payload)
	s.Require().NoError(err)

	createReq := httptest.NewRequest(http.MethodPost, "/order", bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	s.mux.ServeHTTP(createRR, createReq)

	s.Require().Equal(http.StatusCreated, createRR.Code)
	s.Require().Equal(orderID.String(), createRR.Body.String())

	getReq := httptest.NewRequest(http.MethodGet, "/orders/"+orderID.String(), nil)
	getRR := httptest.NewRecorder()
	s.mux.ServeHTTP(getRR, getReq)
	s.Require().Equal(http.StatusOK, getRR.Code)

	var got model.Order
	s.Require().NoError(json.Unmarshal(getRR.Body.Bytes(), &got))
	s.Equal(orderID, got.ID)
	s.Equal("pending", got.Status)

	events, err := s.svc.GetOrderEvents(context.Background())
	s.Require().NoError(err)

	var found bool
	for _, e := range events {
		if e.OrderID == orderID && e.EventType == model.EventCreated {
			found = true
			s.Equal(model.SourceHTTPSync, e.Source)
			break
		}
	}
	s.True(found, "created event not found")
}
