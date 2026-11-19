package handler

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/handler/health"
	orderhandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order"
	ordereventhandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order_event"
	dlqhandler "github.com/Vladislav747/golang-project-order-system/internal/transport/dlq"
)

func RegisterRoutes(
	mux *http.ServeMux,
	logger *zap.Logger,
	orderHandler *orderhandler.Handler,
	orderEventHandler *ordereventhandler.Handler,
	healthHandler *health.Handler,
	dlqHandler *dlqhandler.Handler,
) {
	mux.HandleFunc("GET /orders", InstrumentMetricsHandler(logger, "GET", "/orders", orderHandler.GetOrders))
	mux.HandleFunc("POST /order", InstrumentMetricsHandler(logger, "POST", "/order", orderHandler.CreateOrder))

	mux.HandleFunc("GET /orders/{id}", InstrumentMetricsHandler(logger, "GET", "/orders/{id}", orderHandler.GetOrder))
	mux.HandleFunc("PATCH /orders", InstrumentMetricsHandler(logger, "PATCH", "/orders", orderHandler.UpdateOrder))
	mux.HandleFunc("DELETE /orders/{id}", InstrumentMetricsHandler(logger, "DELETE", "/orders/{id}", orderHandler.DeleteSoftOrder))
	mux.HandleFunc("DELETE /orders/hard/{id}", InstrumentMetricsHandler(logger, "DELETE", "/orders/hard/{id}", orderHandler.DeleteOrder))

	mux.HandleFunc("GET /order-events", InstrumentMetricsHandler(logger, "GET", "/order-events", orderEventHandler.GetOrderEvents))

	mux.HandleFunc("GET /admin/dlq", InstrumentMetricsHandler(logger, "GET", "/admin/dlq", dlqHandler.List))
	mux.HandleFunc("GET /admin/dlq/{id}", InstrumentMetricsHandler(logger, "GET", "/admin/dlq/{id}", dlqHandler.Get))
	mux.HandleFunc("POST /admin/dlq/{id}/retry", InstrumentMetricsHandler(logger, "POST", "/admin/dlq/{id}/retry", dlqHandler.Retry))

	// для readiness и liveness probes
	mux.HandleFunc("GET /livez", healthHandler.Live)
	mux.HandleFunc("GET /readyz", healthHandler.Ready)

	mux.Handle("/metrics", promhttp.Handler())
}
