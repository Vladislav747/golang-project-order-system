package health

import (
	"context"
	"net/http"
	"time"

	"github.com/Vladislav747/golang-project-order-system/internal/pkg/httpx"
)

// Checker проверяет готовность зависимостей принимать трафик.
type Checker interface {
	Ready(ctx context.Context) error
}

type Handler struct {
	checker Checker
}

func NewHandler(checker Checker) *Handler {
	return &Handler{checker: checker}
}

func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.checker.Ready(ctx); err != nil {
		httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"error":  err.Error(),
		})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
