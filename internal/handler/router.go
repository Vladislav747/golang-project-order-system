package handler

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	orderhandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order"
	ordereventhandler "github.com/Vladislav747/golang-project-order-system/internal/handler/order_event"
)

func RegisterRoutes(mux *http.ServeMux, orderHandler *orderhandler.Handler, orderEventHandler *ordereventhandler.Handler) {
	mux.HandleFunc("GET /orders", InstrumentMetricsHandler("GET", "/orders", orderHandler.GetOrders))
	mux.HandleFunc("POST /order", InstrumentMetricsHandler("POST", "/order", orderHandler.CreateOrder))
	mux.HandleFunc("POST /order/async", InstrumentMetricsHandler("POST", "/order/async", orderHandler.CreateOrderKafka))

	mux.HandleFunc("GET /orders/{id}", InstrumentMetricsHandler("GET", "/orders/{id}", orderHandler.GetOrder))
	mux.HandleFunc("PATCH /orders/{id}", InstrumentMetricsHandler("PATCH", "/orders/{id}", orderHandler.UpdateOrder))
	mux.HandleFunc("DELETE /orders/{id}", InstrumentMetricsHandler("DELETE", "/orders/{id}", orderHandler.DeleteSoftOrder))
	mux.HandleFunc("DELETE /orders/hard/{id}", InstrumentMetricsHandler("DELETE", "/orders/hard/{id}", orderHandler.DeleteOrder))

	mux.HandleFunc("GET /order-events", InstrumentMetricsHandler("GET", "/order-events", orderEventHandler.GetOrderEvents))

	mux.Handle("/metrics", promhttp.Handler())
}
