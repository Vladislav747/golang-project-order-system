package handler

import (
	"fmt"
	"net/http"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type Service interface {
	CreateOrder() error
	GetOrders() []model.Order
	GetOrder(id int64) (model.Order, error)
	UpdateOrder(id int64) error
	DeleteOrder(id int64) error
}

type handler struct {
	service Service
}

func NewHandler(service Service) *handler {
	return &handler{service: service}
}

func (h *handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	_ = h.service.CreateOrder()
	fmt.Println("CreateOrder")
}

func (h *handler) GetOrders(w http.ResponseWriter, r *http.Request) {
	_ = h.service.GetOrders()
	fmt.Println("GetOrders")
}

func (h *handler) GetOrder(w http.ResponseWriter, r *http.Request) {
	_, err := h.service.GetOrder(1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Println("GetOrder")
}

func (h *handler) UpdateOrder(w http.ResponseWriter, r *http.Request) {
	_ = h.service.UpdateOrder(1)
	fmt.Println("UpdateOrder")
}

func (h *handler) DeleteOrder(w http.ResponseWriter, r *http.Request) {
	_ = h.service.DeleteOrder(1)
	fmt.Println("DeleteOrder")
}