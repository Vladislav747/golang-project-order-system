package service

import (
	"github.com/Vladislav747/golang-project-order-system/internal/model"
)


type Service interface {
	CreateOrder() error
	GetOrders() []model.Order
	GetOrder() (model.Order, error)
	UpdateOrder() error
	DeleteOrder() error
}

type Repository interface {
	CreateOrder(order model.Order) error
	GetOrders() []model.Order
	GetOrder(id int64) (model.Order, error)
	UpdateOrder(order model.Order) error
	DeleteOrder(id int64) error
}


type service struct {
	repository Repository
}

func NewService(repository Repository) *service {
	return &service{
		repository: repository,
	}
}

func (s *service) CreateOrder() error {
	return s.repository.CreateOrder(model.Order{})
}

func (s *service) GetOrders() []model.Order {
	return s.repository.GetOrders()
}

func (s *service) GetOrder(id int64) (model.Order, error) {
	return s.repository.GetOrder(id)
}

func (s *service) UpdateOrder(id int64) error {
	return s.repository.UpdateOrder(model.Order{})
}

func (s *service) DeleteOrder(id int64) error {
	return s.repository.DeleteOrder(id)
}