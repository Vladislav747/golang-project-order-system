package Handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
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
	UpdateOrderKafka(ctx context.Context, order model.Order) error
	DeleteOrderKafka(ctx context.Context, id string) error
}

type Handler struct {
	service        Service
	logger         *zap.Logger
	requestTimeout time.Duration
	processingMode config.ProcessingMode
}

func NewHandler(
	service Service,
	logger *zap.Logger,
	requestTimeout time.Duration,
	processingMode config.ProcessingMode,
) *Handler {
	return &Handler{
		service:        service,
		logger:         logger,
		requestTimeout: requestTimeout,
		processingMode: processingMode,
	}
}

func (h *Handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
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

	var err error
	if h.processingMode.Mode == config.OrderModeAsync {
		err = h.service.CreateOrderKafka(ctx, input)
		w.WriteHeader(http.StatusAccepted)
	} else {
		err = h.service.CreateOrder(ctx, input)
		w.WriteHeader(http.StatusCreated)
	}

	if err != nil {
		h.logger.Error("failed to create order", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(input.ID.String()))
}

func (h *Handler) GetOrders(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()
	res, err := h.service.GetOrders(ctx)
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

func (h *Handler) GetOrder(w http.ResponseWriter, r *http.Request) {
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
		h.logger.Error("failed to marshal order", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if h.processingMode.Mode == config.OrderModeAsync {
		w.Write([]byte("Order send to create queue"))
	} else {
		w.Write(order)
	}

}

func (h *Handler) UpdateOrder(w http.ResponseWriter, r *http.Request) {
	var input model.Order

	if err := decodeRequest(r, &input, h.logger); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if input.ID == uuid.Nil {
		h.logger.Error("id is required")
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()

	var err error
	if h.processingMode.Mode == config.OrderModeAsync {
		err = h.service.UpdateOrderKafka(ctx, input)
	} else {
		err = h.service.UpdateOrder(ctx, input)
	}

	if err != nil {
		h.logger.Error("failed to update order", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)

	if h.processingMode.Mode == config.OrderModeAsync {
		w.Write([]byte("Order send to updated queue"))
	} else {
		w.Write([]byte("Order updated"))
	}
}

func (h *Handler) DeleteSoftOrder(w http.ResponseWriter, r *http.Request) {
	idParam := r.PathValue("id")
	if idParam == "" {
		h.logger.Error("id is required")
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := requestContext(r, h.requestTimeout)
	defer cancel()

	var err error
	if h.processingMode.Mode == config.OrderModeAsync {
		err = h.service.DeleteOrderKafka(ctx, idParam)
	} else {
		err = h.service.DeleteSoftOrder(ctx, idParam)
	}

	if err != nil {
		h.logger.Error("failed to delete order", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if h.processingMode.Mode == config.OrderModeAsync {
		w.Write([]byte("Order send to delete queue"))
	} else {
		w.Write([]byte("Order marked as deleted"))
	}
}

func (h *Handler) DeleteOrder(w http.ResponseWriter, r *http.Request) {
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
		h.logger.Error("failed to delete soft order", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Order deleted"))
}

func requestContext(r *http.Request, requestTimeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	return ctx, cancel
}

func decodeRequest(r *http.Request, input *model.Order, logger *zap.Logger) error {
	if err := json.NewDecoder(r.Body).Decode(input); err != nil {
		logger.Error("failed to decode request body", zap.Error(err))
		return err
	}
	return nil
}
