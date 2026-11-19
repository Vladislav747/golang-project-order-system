package dlq

import (
	"context"
	"encoding/json"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
	kafkatransport "github.com/Vladislav747/golang-project-order-system/internal/transport/kafka"
)

type KafkaPublisher interface {
	PublishEvents(events []kafkatransport.PublishMessage) error
}

type DeadLetterRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, letter model.DeadLetter) error
}
type TxManager interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Sink struct {
	publisher KafkaPublisher
	repo      DeadLetterRepository
	txManager TxManager
	topic     string // orders.dlq
	logger    *zap.Logger
}

const maxErrorTextLen = 2048

func NewSink(publisher KafkaPublisher, repo DeadLetterRepository, txManager TxManager, topic string, logger *zap.Logger) *Sink {
	return &Sink{
		publisher: publisher,
		repo:      repo,
		txManager: txManager,
		topic:     topic,
		logger:    logger,
	}
}

func (s *Sink) Send(ctx context.Context, rec kafkatransport.DeadLetterRecord) error {
	errText := truncate(rec.ErrorText, maxErrorTextLen)

	headers := []kafkatransport.RecordHeader{
		{Key: kafkatransport.HeaderOriginalTopic, Value: rec.Topic},
		{Key: kafkatransport.HeaderOriginalPartition, Value: strconv.Itoa(int(rec.Partition))},
		{Key: kafkatransport.HeaderOriginalOffset, Value: strconv.FormatInt(rec.Offset, 10)},
		{Key: kafkatransport.HeaderError, Value: errText},
		{Key: kafkatransport.HeaderErrorClass, Value: string(rec.ErrorClass)},
		{Key: kafkatransport.HeaderAttempts, Value: strconv.Itoa(rec.Attempts)},
		{Key: kafkatransport.HeaderFirstErrorAt, Value: rec.FirstFailedAt.Format(time.RFC3339Nano)},
	}

	if len(rec.Key) > 0 {
		headers = append(headers, kafkatransport.RecordHeader{
			Key:   kafkatransport.HeaderOriginalKey,
			Value: string(rec.Key),
		})
	}

	if err := s.publisher.PublishEvents([]kafkatransport.PublishMessage{{
		Topic:   s.topic,
		Key:     rec.Key,
		Payload: rec.Payload,
		Headers: headers,
	}}); err != nil {
		s.logger.Error("failed to publish to kafka DLQ",
			zap.String("topic", rec.Topic),
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err),
		)
		return err
	}

	partition, offset := rec.Partition, rec.Offset
	letter := model.DeadLetter{
		Source:            model.DeadLetterSourceConsumer,
		Payload:           json.RawMessage(rec.Payload),
		OriginalTopic:     rec.Topic,
		OriginalPartition: &partition,
		OriginalOffset:    &offset,
		ErrorText:         errText,
		ErrorClass:        rec.ErrorClass,
		Attempts:          rec.Attempts,
		FirstFailedAt:     rec.FirstFailedAt,
	}

	err := pgx.BeginFunc(ctx, s.txManager, func(tx pgx.Tx) error {
		return s.repo.Insert(ctx, tx, letter)
	})
	if err != nil {
		s.logger.Error("failed to insert dead letter after kafka DLQ publish",
			zap.String("topic", rec.Topic),
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err),
		)
		return err
	}
	return nil

}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	// откат к границе руны, чтобы не резать UTF-8 символ посередине
	for max > 0 && !utf8.RuneStart(s[max]) {
		max--
	}
	return s[:max]
}
