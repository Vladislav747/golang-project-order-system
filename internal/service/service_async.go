package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	"github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

func (s *Service) HandleCreateOrder(ctx context.Context, msg kafka.OrderCommandMessage) error {
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

func (s *Service) HandleUpdateOrder(ctx context.Context, msg kafka.OrderCommandMessage) error {

	order := model.Order{
		ID:          msg.OrderID,
		CustomerID:  msg.CustomerID,
		Status:      msg.Status,
		TotalAmount: msg.TotalAmount,
		Currency:    msg.Currency,
		Items:       msg.Items,
	}
	return s.UpdateOrderFromKafka(ctx, order)
}

func (s *Service) HandleDeleteOrder(ctx context.Context, msg kafka.OrderCommandMessage) error {
	order := model.Order{
		ID: msg.OrderID,
	}
	return s.DeleteOrderFromKafka(ctx, order.ID.String())
}

func (s *Service) CreateOrderKafka(ctx context.Context, order model.Order) error {
	message := kafka.OrderCommandMessage{
		Action:      "created",
		OrderID:     order.ID,
		CustomerID:  order.CustomerID,
		Status:      order.Status,
		TotalAmount: order.TotalAmount,
		Currency:    order.Currency,
		Items:       order.Items,
	}
	s.logger.Info("sending message on create order to kafka", zap.Any("message", message))
	return s.producer.SendMessage(message)
}

func (s *Service) CreateOrderFromKafka(ctx context.Context, order model.Order) error {

	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		if err := s.repositoryOrder.CreateOrder(ctx, tx, order); err != nil {
			return err
		}

		event, err := buildOrderEvent(order.ID, model.EventCreated, model.SourceKafka, nil)
		if err != nil {
			s.logger.Error("failed to build order event", zap.Error(err))
			return err
		}
		return s.repositoryOrderEvent.CreateOrderEvent(ctx, tx, event)
	})
}

func (s *Service) UpdateOrderFromKafka(ctx context.Context, order model.Order) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		if err := s.repositoryOrder.UpdateOrder(ctx, tx, order); err != nil {
			return err
		}

		event, err := buildOrderEvent(order.ID, model.EventUpdated, model.SourceKafka, order)
		if err != nil {
			s.logger.Error("failed to build order event", zap.Error(err))
			return err
		}
		return s.repositoryOrderEvent.CreateOrderEvent(ctx, tx, event)
	})
}

func (s *Service) DeleteOrderFromKafka(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		if err := s.repositoryOrder.DeleteSoftOrder(ctx, tx, id); err != nil {
			return err
		}

		orderIDUUID, err := uuid.Parse(id)
		if err != nil {
			s.logger.Error("failed to parse order ID", zap.Error(err))
			return err
		}

		event, err := buildOrderEvent(orderIDUUID, model.EventDeleted, model.SourceKafka, nil)
		if err != nil {
			s.logger.Error("failed to build order event", zap.Error(err))
			return err
		}
		return s.repositoryOrderEvent.CreateOrderEvent(ctx, tx, event)
	})
}

func (s *Service) UpdateOrderKafka(ctx context.Context, order model.Order) error {
	message := kafka.OrderCommandMessage{
		Action:      "updated",
		OrderID:     order.ID,
		CustomerID:  order.CustomerID,
		Status:      order.Status,
		TotalAmount: order.TotalAmount,
		Currency:    order.Currency,
		Items:       order.Items,
	}
	s.logger.Info("sending message on update order to kafka", zap.Any("message", message))
	return s.producer.SendMessage(message)
}

func (s *Service) DeleteOrderKafka(ctx context.Context, id string) error {
	message := kafka.OrderCommandMessage{
		Action:  "deleted",
		OrderID: uuid.MustParse(id),
	}
	s.logger.Info("sending message on delete order to kafka", zap.Any("message", message))
	return s.producer.SendMessage(message)
}
