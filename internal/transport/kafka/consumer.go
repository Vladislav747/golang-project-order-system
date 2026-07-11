package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// CreateOrderHandler — контракт бизнес-логики обработки входящего сообщения.
// Consumer не знает про service/repository — только вызывает этот интерфейс.
type CreateOrderHandler interface {
	HandleCreateOrder(ctx context.Context, msg OrderCommandMessage) error
	HandleUpdateOrder(ctx context.Context, msg OrderCommandMessage) error
	HandleDeleteOrder(ctx context.Context, msg OrderCommandMessage) error
}

// Consumer оборачивает sarama.ConsumerGroup и запускает чтение из топика.
type Consumer struct {
	group   sarama.ConsumerGroup // клиент consumer group (распределение партиций между инстансами)
	topic   string
	handler CreateOrderHandler
	logger  *zap.Logger
}

// consumerGroupHandler реализует sarama.ConsumerGroupHandler.
// Sarama вызывает его методы при join/rebalance группы и при доставке сообщений.
type consumerGroupHandler struct {
	handler CreateOrderHandler
	logger  *zap.Logger
}

// Setup вызывается перед началом новой сессии (после rebalance, когда партиции назначены этому consumer).
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup вызывается после завершения сессии (перед rebalance или при выходе из группы).
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim — основной цикл обработки: sarama отдаёт сообщения из одной партиции через claim.Messages().
// Метод выполняется в отдельной горутине на каждую назначенную партицию.
func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		var msgData OrderCommandMessage
		if err := json.Unmarshal(msg.Value, &msgData); err != nil {
			h.logger.Error("failed to unmarshal message", zap.Error(err))
			continue // битое сообщение: offset не коммитим — sarama прочитает его снова
		}

		// session.Context() отменяется при shutdown/rebalance — передаём его в handler.

		var err error

		switch msgData.Action {
		case "created":
			if err = h.handler.HandleCreateOrder(session.Context(), msgData); err != nil {
				h.logger.Error("failed to handle message", zap.Error(err))
				continue // при ошибке обработки offset не коммитим — сообщение будет прочитано снова
			}
		case "updated":
			if err = h.handler.HandleUpdateOrder(session.Context(), msgData); err != nil {
				h.logger.Error("failed to handle message", zap.Error(err))
				continue // при ошибке обработки offset не коммитим — сообщение будет прочитано снова
			}
		case "deleted":
			fmt.Println("deleted order", msgData)
			if err = h.handler.HandleDeleteOrder(session.Context(), msgData); err != nil {
				h.logger.Error("failed to handle message", zap.Error(err))
				continue // при ошибке обработки offset не коммитим — сообщение будет прочитано снова
			}
		default:
			h.logger.Error("unknown action", zap.String("action", msgData.Action))
			continue
		}

		// MarkMessage помечает offset как обработанный (аналог CommitMessages в kafka-go).
		// Фактический commit в Kafka происходит периодически в фоне sarama.
		session.MarkMessage(msg, "")
	}
	return nil
}

// NewConsumer создаёт consumer group и подключается к брокерам.
func NewConsumer(brokers []string, topic string, groupID string, handler CreateOrderHandler, logger *zap.Logger) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	// OffsetNewest — при первом подключении группы читаем только новые сообщения.
	// OffsetOldest — читать с начала топика (удобно при отладке).
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		logger.Error("failed to create consumer group", zap.Error(err))
		return nil, err
	}

	return &Consumer{
		group:   group,
		topic:   topic,
		handler: handler,
		logger:  logger,
	}, nil

}

// Run блокируется и читает сообщения, пока ctx не отменён.
// Внешний for обязателен: после rebalance Consume() возвращает управление,
// и нужно вызвать его снова, чтобы продолжить чтение.
func (c *Consumer) Run(ctx context.Context) error {
	if c.handler == nil {
		return fmt.Errorf("consumer handler is nil")
	}

	h := &consumerGroupHandler{handler: c.handler, logger: c.logger}

	for {
		if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
			if ctx.Err() != nil {
				return nil // штатный shutdown — не считаем ошибкой
			}
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Close освобождает ресурсы consumer group. Вызывать после cancel контекста Run.
func (c *Consumer) Close() error {
	return c.group.Close()
}
