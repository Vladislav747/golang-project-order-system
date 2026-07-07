package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type Service interface {
	CreateOrder(ctx context.Context, order model.Order) error
	CreateOrderKafka(ctx context.Context, order model.Order) error
	GetOrders(ctx context.Context) ([]model.Order, error)
	GetOrder(ctx context.Context, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, order model.Order) error
	DeleteOrder(ctx context.Context, id string) error
}

type handler struct {
	service        Service
	logger         *slog.Logger
	requestTimeout time.Duration
}

func NewHandler(service Service, logger *slog.Logger, requestTimeout time.Duration) *handler {
	return &handler{service: service, logger: logger, requestTimeout: requestTimeout}
}

func (h *handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var input model.Order

	if err := decodeRequest(r, &input, h.logger); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if input.ID == uuid.Nil {
		ID := uuid.New()
		input.ID = ID
	}

	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()

	err := h.service.CreateOrder(ctx, input)
	if err != nil {
		h.logger.Error("failed to create order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(input.ID.String()))
}

func (h *handler) GetOrders(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()
	res, err := h.service.GetOrders(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	orders, err := json.Marshal(res)
	if err != nil {
		h.logger.Error("failed to marshal order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(orders)
}

func (h *handler) GetOrder(w http.ResponseWriter, r *http.Request) {
	idParam := r.PathValue("id")
	if idParam == "" {
		h.logger.Error("id is required")
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()
	res, err := h.service.GetOrder(ctx, idParam)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	order, err := json.Marshal(res)
	if err != nil {
		h.logger.Error("failed to marshal order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(order)
}

func (h *handler) UpdateOrder(w http.ResponseWriter, r *http.Request) {
	var input model.Order

	if err := decodeRequest(r, &input, h.logger); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()

	err := h.service.UpdateOrder(ctx, input)
	if err != nil {
		h.logger.Error("failed to update order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Order updated"))
}

func (h *handler) DeleteOrder(w http.ResponseWriter, r *http.Request) {
	idParam := r.PathValue("id")
	if idParam == "" {
		h.logger.Error("id is required")
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()

	err := h.service.DeleteOrder(ctx, idParam)
	if err != nil {
		h.logger.Error("failed to delete order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Order deleted"))
}

func (h *handler) CreateOrderKafka(w http.ResponseWriter, r *http.Request) {
	var input model.Order

	if err := decodeRequest(r, &input, h.logger); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if input.ID == uuid.Nil {
		ID := uuid.New()
		input.ID = ID
	}

	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()

	err := h.service.CreateOrderKafka(ctx, input)
	if err != nil {
		h.logger.Error("failed to create order kafka", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Order created kafka"))
}

func requestContext(r *http.Request, requestTimeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	return ctx, cancel
}

func decodeRequest(r *http.Request, input *model.Order, logger *slog.Logger) error {
	if err := json.NewDecoder(r.Body).Decode(input); err != nil {
		logger.Error("failed to decode request body", "error", err)
		return err
	}
	return nil
}
