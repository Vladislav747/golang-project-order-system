package service

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

func (s *Service) HandleCreateOrder(ctx context.Context, msg kafka.CreateOrderMessage) error {
	order := model.Order{
		ID:          msg.OrderID,
		CustomerID:  msg.CustomerID,
		Status:      msg.Status,
		TotalAmount: msg.TotalAmount,
		Currency:    msg.Currency,
		Items:       msg.Items,
	}
	return s.CreateOrderFromKafka(ctx, order)
}

func (s *Service) CreateOrderKafka(ctx context.Context, order model.Order) error {
	message := kafka.CreateOrderMessage{
		OrderID:     order.ID,
		CustomerID:  order.CustomerID,
		Status:      order.Status,
		TotalAmount: order.TotalAmount,
		Currency:    order.Currency,
		Items:       order.Items,
	}
	s.logger.Info("sending message to kafka", zap.Any("message", message))
	return s.producer.SendMessage(message)
}

func (s *Service) CreateOrderFromKafka(ctx context.Context, order model.Order) error {

	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repository.CreateOrder(ctx, tx, order)
	})
}
