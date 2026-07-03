package kafka

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/segmentio/kafka-go"
)


type Producer struct {
    writer *kafka.Writer
	logger *slog.Logger
}

func NewProducer(brokers []string, topic string, logger *slog.Logger) *Producer {
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
		p.logger.Error("failed to marshal message SendMessage", "error", err)
        return err
    }

	err = p.writer.WriteMessages(context.Background(), kafka.Message{
        Value: data,
    })
	if err != nil {
		p.logger.Error("failed to send message to kafka", "error", err)
		return err
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}