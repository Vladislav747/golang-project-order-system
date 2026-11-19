package dlq

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/httpx"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/utils"
	deadletter "github.com/Vladislav747/golang-project-order-system/internal/repository/deadletter"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

type Service interface {
	ListDeadLetters(ctx context.Context, f deadletter.ListFilter) ([]model.DeadLetter, error)
	GetDeadLetter(ctx context.Context, id uuid.UUID) (model.DeadLetter, error)
	RetryDeadLetter(ctx context.Context, id uuid.UUID) (model.DeadLetter, error)
}

type Handler struct {
	service  Service
	logger   *zap.Logger
	provider *config.Provider
}

func NewHandler(service Service, logger *zap.Logger, provider *config.Provider) *Handler {
	return &Handler{service: service, logger: logger, provider: provider}
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	cfg := h.provider.Get()

	ctx, cancel := utils.RequestContext(r, cfg.HttpServer.RequestTimeout)
	defer cancel()

	f, err := parseListFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	items, err := h.service.ListDeadLetters(ctx, f)
	if err != nil {
		h.logger.Error("failed to list dead letters", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	limit := f.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": f.Offset,
	})
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	cfg := h.provider.Get()
	ctx, cancel := utils.RequestContext(r, cfg.HttpServer.RequestTimeout)
	defer cancel()

	letter, err := h.service.GetDeadLetter(ctx, id)
	if errors.Is(err, service.ErrDeadLetterNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("failed to get dead letter", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, letter)

}

func (h *Handler) Retry(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	cfg := h.provider.Get()
	ctx, cancel := utils.RequestContext(r, cfg.HttpServer.RequestTimeout)
	defer cancel()

	letter, err := h.service.RetryDeadLetter(ctx, id)
	if errors.Is(err, service.ErrDeadLetterNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("failed to retry dead letter", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"id":      letter.ID,
		"source":  letter.Source,
		"retried": true,
	})
}

func parseListFilter(r *http.Request) (deadletter.ListFilter, error) {
	q := r.URL.Query()
	var f deadletter.ListFilter
	if limitStr := q.Get("limit"); limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil {
			return f, fmt.Errorf("invalid limit")
		}
		f.Limit = n
	}

	if offsetStr := q.Get("offset"); offsetStr != "" {
		n, err := strconv.Atoi(offsetStr)
		if err != nil {
			return f, fmt.Errorf("invalid offset")
		}
		f.Offset = n
	}
	if source := q.Get("source"); source != "" {
		switch model.DeadLetterSource(source) {
		case model.DeadLetterSourceOutbox, model.DeadLetterSourceConsumer:
			s := model.DeadLetterSource(source)
			f.Source = &s
		default:
			return f, fmt.Errorf("invalid source")
		}
	}

	// default: только pending
	retried := false
	f.Retried = &retried
	switch q.Get("retried") {
	case "", "false":
		// already pending
	case "true":
		t := true
		f.Retried = &t
	case "all":
		f.Retried = nil
	default:
		return f, fmt.Errorf("invalid retried")
	}

	return f, nil
}
