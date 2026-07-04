package handler

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RegisterRoutes(mux *http.ServeMux, handler *handler) {
	mux.HandleFunc("GET /orders", InstrumentGetOrders(handler.GetOrders))
	mux.HandleFunc("POST /order", InstrumentCreateOrder(handler.CreateOrder))
	mux.HandleFunc("POST /order/async", InstrumentCreateOrderAsync(handler.CreateOrderKafka))

	mux.HandleFunc("GET /orders/{id}", InstrumentGetOrder(handler.GetOrder))
	mux.HandleFunc("PATCH /orders/{id}", InstrumentUpdateOrder(handler.UpdateOrder))
	mux.HandleFunc("DELETE /orders/{id}", InstrumentDeleteOrder(handler.DeleteOrder))

	mux.Handle("/metrics", promhttp.Handler())
}