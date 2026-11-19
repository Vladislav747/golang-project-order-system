package dlq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/model"
	deadletter "github.com/Vladislav747/golang-project-order-system/internal/repository/deadletter"
	"github.com/Vladislav747/golang-project-order-system/internal/service"
)

type mockService struct {
	mock.Mock
}

func (m *mockService) ListDeadLetters(ctx context.Context, f deadletter.ListFilter) ([]model.DeadLetter, error) {
	args := m.Called(ctx, f)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.DeadLetter), args.Error(1)
}

func (m *mockService) GetDeadLetter(ctx context.Context, id uuid.UUID) (model.DeadLetter, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(model.DeadLetter), args.Error(1)
}

func (m *mockService) RetryDeadLetter(ctx context.Context, id uuid.UUID) (model.DeadLetter, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(model.DeadLetter), args.Error(1)
}

func newTestHandler(t *testing.T) (*mockService, *Handler) {
	t.Helper()
	svc := &mockService{}
	svc.Test(t)
	t.Cleanup(func() { svc.AssertExpectations(t) })

	cfg := &config.Config{
		HttpServer: config.HttpServer{RequestTimeout: 2 * time.Second},
	}
	return svc, NewHandler(svc, zap.NewNop(), config.NewProvider(cfg))
}

func TestList_DefaultPendingFilter(t *testing.T) {
	t.Parallel()
	svc, h := newTestHandler(t)

	id := uuid.New()
	svc.On("ListDeadLetters", mock.Anything, mock.MatchedBy(func(f deadletter.ListFilter) bool {
		return f.Retried != nil && !*f.Retried
	})).Return([]model.DeadLetter{{
		ID:            id,
		Source:        model.DeadLetterSourceOutbox,
		OriginalTopic: "orders.events",
		ErrorText:     "boom",
		ErrorClass:    model.ErrorClassRetryable,
		Attempts:      3,
	}}, nil).Once()

	req := httptest.NewRequest(http.MethodGet, "/admin/dlq", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.EqualValues(t, 50, body["limit"])
	require.EqualValues(t, 0, body["offset"])
	items, ok := body["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
}

func TestList_InvalidSource(t *testing.T) {
	t.Parallel()
	_, h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/admin/dlq?source=nope", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGet_NotFound(t *testing.T) {
	t.Parallel()
	svc, h := newTestHandler(t)

	id := uuid.New()
	svc.On("GetDeadLetter", mock.Anything, id).Return(model.DeadLetter{}, service.ErrDeadLetterNotFound).Once()

	req := httptest.NewRequest(http.MethodGet, "/admin/dlq/"+id.String(), nil)
	req.SetPathValue("id", id.String())
	rr := httptest.NewRecorder()
	h.Get(rr, req)

	require.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGet_OK(t *testing.T) {
	t.Parallel()
	svc, h := newTestHandler(t)

	id := uuid.New()
	svc.On("GetDeadLetter", mock.Anything, id).Return(model.DeadLetter{
		ID:            id,
		Source:        model.DeadLetterSourceConsumer,
		OriginalTopic: "orders.create",
		ErrorText:     "bad json",
		ErrorClass:    model.ErrorClassNonRetryable,
	}, nil).Once()

	req := httptest.NewRequest(http.MethodGet, "/admin/dlq/"+id.String(), nil)
	req.SetPathValue("id", id.String())
	rr := httptest.NewRecorder()
	h.Get(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), id.String())
}

func TestRetry_OK(t *testing.T) {
	t.Parallel()
	svc, h := newTestHandler(t)

	id := uuid.New()
	now := time.Now()
	svc.On("RetryDeadLetter", mock.Anything, id).Return(model.DeadLetter{
		ID:        id,
		Source:    model.DeadLetterSourceOutbox,
		RetriedAt: &now,
	}, nil).Once()

	req := httptest.NewRequest(http.MethodPost, "/admin/dlq/"+id.String()+"/retry", nil)
	req.SetPathValue("id", id.String())
	rr := httptest.NewRecorder()
	h.Retry(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Equal(t, id.String(), body["id"])
	require.Equal(t, string(model.DeadLetterSourceOutbox), body["source"])
	require.Equal(t, true, body["retried"])
}

func TestRetry_InvalidID(t *testing.T) {
	t.Parallel()
	_, h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/admin/dlq/not-a-uuid/retry", nil)
	req.SetPathValue("id", "not-a-uuid")
	rr := httptest.NewRecorder()
	h.Retry(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
}
