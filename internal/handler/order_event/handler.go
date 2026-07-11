package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type Service interface {
	CreateOrder(ctx context.Context, order model.Order) error
	CreateOrderKafka(ctx context.Context, order model.Order) error
	GetOrders(ctx context.Context) ([]model.Order, error)
	GetOrder(ctx context.Context, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, order model.Order) error
	DeleteOrder(ctx context.Context, id string) error
	DeleteSoftOrder(ctx context.Context, id string) error

	GetOrderEvents(ctx context.Context) ([]model.OrderEvent, error)
}

type Handler struct {
	service        Service
	logger         *zap.Logger
	requestTimeout time.Duration
}

func NewHandler(service Service, logger *zap.Logger, requestTimeout time.Duration) *Handler {
	return &Handler{service: service, logger: logger, requestTimeout: requestTimeout}
}

func (h *Handler) GetOrderEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()
	res, err := h.service.GetOrderEvents(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	orders, err := json.Marshal(res)
	if err != nil {
		h.logger.Error("failed to marshal order", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(orders)
}

func requestContext(r *http.Request, requestTimeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	return ctx, cancel
}
