package handler

import (
	"context"
	"log/slog"
	"net/http"
	"encoding/json"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type Service interface {
	CreateOrder(ctx context.Context, order model.Order) error
	GetOrders(ctx context.Context) ([]model.Order, error)
	GetOrder(ctx context.Context, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, order model.Order) error
	DeleteOrder(ctx context.Context, id string) error
}

type handler struct {
	service Service
	logger *slog.Logger
	ctx context.Context
}

func NewHandler( ctx context.Context, service Service, logger *slog.Logger) *handler {
	return &handler{service: service, logger: logger, ctx: ctx}
}

func (h *handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var input model.Order

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.logger.Error("failed to decode request body", "error", err)
		http.Error(w, "некорректный JSON", http.StatusBadRequest)
		return
	}

	err := h.service.CreateOrder(r.Context(), input)
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
	res, err := h.service.GetOrders(r.Context())
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
	res, err := h.service.GetOrder(r.Context(), idParam)
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

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.logger.Error("failed to decode request body", "error", err)
		http.Error(w, "некорректный JSON", http.StatusBadRequest)
		return
	}

	err := h.service.UpdateOrder(r.Context(), input)
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
	err := h.service.DeleteOrder(r.Context(), idParam)
	if err != nil {
		h.logger.Error("failed to delete order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Order deleted"))
}