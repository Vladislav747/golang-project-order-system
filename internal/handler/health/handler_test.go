package health_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Vladislav747/golang-project-order-system/internal/handler/health"
	"github.com/Vladislav747/golang-project-order-system/internal/handler/health/mocks"
)

func TestLive_AlwaysOK(t *testing.T) {
	t.Parallel()
	checker := mocks.NewMockChecker(t)

	h := health.NewHandler(checker)
	rr := httptest.NewRecorder()
	h.Live(rr, httptest.NewRequest(http.MethodGet, "/livez", nil))
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestReady_OK(t *testing.T) {
	t.Parallel()
	checker := mocks.NewMockChecker(t)
	checker.EXPECT().Ready(mock.Anything).Return(nil)

	h := health.NewHandler(checker)
	rr := httptest.NewRecorder()
	h.Ready(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rr.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Equal(t, "ready", body["status"])
}

func TestReady_NotReady(t *testing.T) {
	t.Parallel()
	checker := mocks.NewMockChecker(t)
	checker.EXPECT().Ready(mock.Anything).Return(errors.New("db down"))

	h := health.NewHandler(checker)
	rr := httptest.NewRecorder()
	h.Ready(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Equal(t, "not_ready", body["status"])
	require.Equal(t, "db down", body["error"])
}
