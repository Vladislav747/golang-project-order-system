package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
    reader *kafka.Reader
}

func NewConsumer(brokers []string, topic string, groupID string) *Consumer {
    return &Consumer{
        reader: kafka.NewReader(kafka.ReaderConfig{
            Brokers: brokers,
            Topic:   topic,
			GroupID: groupID,
		}),
	}
}

func (c *Consumer) Run(ctx context.Context) error {
    for {
        msg, err := c.reader.FetchMessage(ctx)
        if err != nil {
            // при shutdown context отменяется — это нормально
            if ctx.Err() != nil {
                return nil
            }
            return err
        }
        if err := c.reader.CommitMessages(ctx, msg); err != nil {
            return err
        }
    }
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}