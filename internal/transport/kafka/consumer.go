package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
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
	group        sarama.ConsumerGroup // клиент consumer group (распределение партиций между инстансами)
	topic        string
	handler      CreateOrderHandler
	logger       *zap.Logger
	maxRetries   int
	retryBackoff time.Duration
	dlqSink      FailedMessageHandler
}

// consumerGroupHandler реализует sarama.ConsumerGroupHandler.
// Sarama вызывает его методы при join/rebalance группы и при доставке сообщений.
type consumerGroupHandler struct {
	handler      CreateOrderHandler
	logger       *zap.Logger
	maxRetries   int
	retryBackoff time.Duration
	dlqSink      FailedMessageHandler
}

// Setup вызывается перед началом новой сессии (после rebalance, когда партиции назначены этому consumer).
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup вызывается после завершения сессии (перед rebalance или при выходе из группы).
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// NewConsumer создаёт consumer group и подключается к брокерам.
func NewConsumer(
	brokers []string,
	topic string,
	groupID string,
	handler CreateOrderHandler,
	dlqSink FailedMessageHandler,
	maxRetries int,
	retryBackoff time.Duration,
	logger *zap.Logger,
) (*Consumer, error) {
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
		group:        group,
		topic:        topic,
		handler:      handler,
		logger:       logger,
		dlqSink:      dlqSink,
		maxRetries:   maxRetries,
		retryBackoff: retryBackoff,
	}, nil

}

// Run блокируется и читает сообщения, пока ctx не отменён.
// Внешний for обязателен: после rebalance Consume() возвращает управление,
// и нужно вызвать его снова, чтобы продолжить чтение.
func (c *Consumer) Run(ctx context.Context) error {
	if c.handler == nil {
		return fmt.Errorf("consumer handler is nil")
	}

	h := &consumerGroupHandler{
		handler:      c.handler,
		logger:       c.logger,
		maxRetries:   c.maxRetries,
		retryBackoff: c.retryBackoff,
		dlqSink:      c.dlqSink,
	}

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

// ConsumeClaim — основной цикл обработки: sarama отдаёт сообщения из одной партиции через claim.Messages().
// Метод выполняется в отдельной горутине на каждую назначенную партицию.
func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		// MarkMessage только после успеха handler'а или успешного DLQ.
		if err := h.processMessage(session.Context(), msg); err != nil {
			return err
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *consumerGroupHandler) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var msgData OrderCommandMessage
	if err := json.Unmarshal(msg.Value, &msgData); err != nil {
		h.logger.Error("failed to unmarshal message", zap.Error(err))
		return h.sendToDLQ(ctx, msg, err.Error(), model.ErrorClassNonRetryable, 0, time.Now())
	}

	var handle func(context.Context, OrderCommandMessage) error
	switch msgData.Action {
	case OrderActionCreated:
		handle = h.handler.HandleCreateOrder
	case OrderActionUpdated:
		handle = h.handler.HandleUpdateOrder
	case OrderActionDeleted:
		handle = h.handler.HandleDeleteOrder
	default:
		h.logger.Error("unknown action", zap.Any("action", msgData.Action))
		return h.sendToDLQ(ctx, msg, fmt.Sprintf("unknown action: %s", msgData.Action),
			model.ErrorClassNonRetryable, 0, time.Now())
	}

	var firstFailedAt time.Time
	var lastErr error
	for i := 1; i <= h.maxRetries; i++ {
		lastErr = handle(ctx, msgData)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if firstFailedAt.IsZero() {
			firstFailedAt = time.Now()
		}
		if !classify(lastErr) {
			return h.sendToDLQ(ctx, msg, lastErr.Error(), model.ErrorClassNonRetryable, i, firstFailedAt)
		}
		h.logger.Warn("retryable handler error, will retry",
			zap.Int("attempt", i),
			zap.Error(lastErr),
		)
		if i == h.maxRetries {
			break
		}
		timer := time.NewTimer(h.retryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return h.sendToDLQ(ctx, msg, lastErr.Error(), model.ErrorClassRetryable, h.maxRetries, firstFailedAt)
}

func (h *consumerGroupHandler) sendToDLQ(
	ctx context.Context,
	msg *sarama.ConsumerMessage,
	errText string,
	class model.DeadLetterErrorClass,
	attempts int,
	firstFailedAt time.Time,
) error {
	return h.dlqSink.Send(ctx, DeadLetterRecord{
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Key:           msg.Key,
		Payload:       msg.Value,
		ErrorText:     errText,
		ErrorClass:    class,
		Attempts:      attempts,
		FirstFailedAt: firstFailedAt,
	})
}
