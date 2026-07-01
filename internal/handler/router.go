package handler

import (
	"net/http"
)

func RegisterRoutes(mux *http.ServeMux, handler *handler) {
	mux.HandleFunc("GET /orders", handler.GetOrders)
	mux.HandleFunc("POST /order", handler.CreateOrder)

	mux.HandleFunc("GET /orders/{id}", handler.GetOrder)
	mux.HandleFunc("UPDATE /orders/{id}", handler.UpdateOrder)
	mux.HandleFunc("DELETE /orders/{id}", handler.DeleteOrder)
}