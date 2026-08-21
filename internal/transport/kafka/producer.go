package kafka

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
	logger   *zap.Logger
}

type PublishMessage struct {
	Topic    string
	Payload  []byte
	Metadata any // uuid.UUID из outbox
}

type PublishError struct {
	Metadata any
	Err      error
}

type PublishErrors []PublishError

func (e PublishErrors) Error() string {
	return fmt.Sprintf("%d publish errors", len(e))
}

func NewProducer(brokers []string, topic string, logger *zap.Logger) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		logger.Error("failed to create producer", zap.Error(err))
		return nil, err
	}

	return &Producer{
		producer: producer,
		topic:    topic,
		logger:   logger,
	}, nil
}

func (p *Producer) SendMessage(message OrderCommandMessage) error {

	data, err := json.Marshal(message)
	if err != nil {
		p.logger.Error("failed to marshal message SendMessage", zap.Error(err))
		return err
	}

	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(data),
	})

	if err != nil {
		p.logger.Error("failed to send message to kafka", zap.Error(err))
		return err
	}
	return nil
}

func (p *Producer) PublishEvent(topic string, value []byte) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	})

	if err != nil {
		p.logger.Error("failed to publish outbox event", zap.Error(err))
		return err
	}
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func (p *Producer) PublishEvents(events []PublishMessage) error {

	if len(events) == 0 {
		return nil
	}

	msgs := make([]*sarama.ProducerMessage, 0, len(events))

	for _, event := range events {
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic:    event.Topic,
			Value:    sarama.ByteEncoder(event.Payload),
			Metadata: event.Metadata, // важно!
		})
	}

	err := p.producer.SendMessages(msgs)

	if err != nil {
		p.logger.Error("failed to publish outbox events", zap.Error(err))
		var producerErrs sarama.ProducerErrors
		if errors.As(err, &producerErrs) {
			out := make(PublishErrors, 0, len(producerErrs))
			for _, pe := range producerErrs {
				if pe == nil || pe.Msg == nil {
					continue
				}
				out = append(out, PublishError{
					Metadata: pe.Msg.Metadata,
					Err:      pe.Err,
				})
			}
			return out
		}
		return err
	}

	return nil
}
