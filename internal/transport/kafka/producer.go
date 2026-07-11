package kafka

import (
	"encoding/json"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
	logger   *zap.Logger
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

func (p *Producer) Close() error {
	return p.producer.Close()
}
