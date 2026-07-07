package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Producer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

func NewProducer(brokers []string, topic string, logger *zap.Logger) *Producer {
	return &Producer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: brokers,
			Topic:   topic,
		}),
		logger: logger,
	}
}

func (p *Producer) SendMessage(message CreateOrderMessage) error {

	data, err := json.Marshal(message)
	if err != nil {
		p.logger.Error("failed to marshal message SendMessage", zap.Error(err))
		return err
	}

	err = p.writer.WriteMessages(context.Background(), kafka.Message{
		Value: data,
	})
	if err != nil {
		p.logger.Error("failed to send message to kafka", zap.Error(err))
		return err
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
