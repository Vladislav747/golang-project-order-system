package handler

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RegisterRoutes(mux *http.ServeMux, handler *handler) {
	mux.HandleFunc("GET /orders", InstrumentMetricsHandler("GET", "/orders", handler.GetOrders))
	mux.HandleFunc("POST /order", InstrumentMetricsHandler("POST", "/order", handler.CreateOrder))
	mux.HandleFunc("POST /order/async", InstrumentMetricsHandler("POST", "/order/async", handler.CreateOrderKafka))

	mux.HandleFunc("GET /orders/{id}", InstrumentMetricsHandler("GET", "/orders/{id}", handler.GetOrder))
	mux.HandleFunc("PATCH /orders/{id}", InstrumentMetricsHandler("PATCH", "/orders/{id}", handler.UpdateOrder))
	mux.HandleFunc("DELETE /orders/{id}", InstrumentMetricsHandler("DELETE", "/orders/{id}", handler.DeleteSoftOrder))
	mux.HandleFunc("DELETE /orders/hard/{id}", InstrumentMetricsHandler("DELETE", "/orders/hard/{id}", handler.DeleteOrder))

	mux.Handle("/metrics", promhttp.Handler())
}
