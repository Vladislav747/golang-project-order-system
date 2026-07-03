package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type CreateOrderHandler interface {
    HandleCreateOrder(ctx context.Context, msg CreateOrderMessage) error
}

type Consumer struct {
    reader *kafka.Reader
	handler CreateOrderHandler
	logger *slog.Logger
}

func NewConsumer(brokers []string, topic string, groupID string, handler CreateOrderHandler, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		handler: handler,
		logger:  logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.handler == nil {
		return fmt.Errorf("consumer handler is nil")
	}

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		var msgData CreateOrderMessage
		if err := json.Unmarshal(msg.Value, &msgData); err != nil {
			c.logger.Error("failed to unmarshal message", "error", err)
			continue
		}

		if err := c.handler.HandleCreateOrder(ctx, msgData); err != nil {
			c.logger.Error("failed to handle message", "error", err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}