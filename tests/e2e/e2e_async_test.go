//go:build e2e_async
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
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
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type OrderAsyncE2ESuite struct {
	suite.Suite

	pool     *pgxpool.Pool
	mux      *http.ServeMux
	svc      *service.Service
	producer *kafka.Producer
	consumer *kafka.Consumer

	consumerCancel context.CancelFunc
	consumerWG     sync.WaitGroup
}

func TestOrderAsyncE2ESuite(t *testing.T) {
	suite.Run(t, new(OrderAsyncE2ESuite))
}

func (s *OrderAsyncE2ESuite) SetupSuite() {
	t := s.T()
	logger := zap.NewNop()

	s.pool = setupPostgres(t)
	brokers := setupKafka(t)

	producer, err := kafka.NewProducer(brokers, e2eOrdersTopic, logger)
	s.Require().NoError(err)
	s.producer = producer
	t.Cleanup(func() { _ = producer.Close() })

	s.svc = service.NewService(
		repositoryOrder.NewRepository(s.pool, logger),
		repositoryOrderEvent.NewRepository(s.pool, logger),
		s.pool,
		producer,
		logger,
	)

	consumer, err := kafka.NewConsumer(
		brokers,
		e2eOrdersTopic,
		"e2e-order-service",
		s.svc,
		logger,
	)
	s.Require().NoError(err)
	s.consumer = consumer

	ctx, cancel := context.WithCancel(context.Background())
	s.consumerCancel = cancel
	s.consumerWG.Add(1)
	go func() {
		defer s.consumerWG.Done()
		_ = consumer.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
		s.consumerWG.Wait()
	})

	// даём consumer group присоединиться к топику до publish (OffsetNewest)
	time.Sleep(2 * time.Second)

	orderHandler := orderhandler.NewHandler(
		s.svc,
		logger,
		5*time.Second,
		config.ProcessingMode{Mode: config.OrderModeAsync},
	)
	orderEventHandler := ordereventhandler.NewHandler(s.svc, logger, 5*time.Second)

	s.mux = http.NewServeMux()
	handler.RegisterRoutes(s.mux, orderHandler, orderEventHandler)
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
	body, err := json.Marshal(payload)
	s.Require().NoError(err)

	createReq := httptest.NewRequest(http.MethodPost, "/order", bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	s.mux.ServeHTTP(createRR, createReq)

	s.Require().Equal(http.StatusAccepted, createRR.Code)
	s.Require().Equal(orderID.String(), createRR.Body.String())

	s.Require().Eventually(func() bool {
		getReq := httptest.NewRequest(http.MethodGet, "/orders/"+orderID.String(), nil)
		getRR := httptest.NewRecorder()
		s.mux.ServeHTTP(getRR, getReq)
		if getRR.Code != http.StatusOK {
			return false
		}
		var got model.Order
		if err := json.Unmarshal(getRR.Body.Bytes(), &got); err != nil {
			return false
		}
		return got.ID == orderID && got.Status == "pending"
	}, 15*time.Second, 200*time.Millisecond, "order was not created by kafka consumer")

	events, err := s.svc.GetOrderEvents(context.Background())
	s.Require().NoError(err)

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
