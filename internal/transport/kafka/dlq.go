package kafka

import (
	"context"
	"time"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

type DeadLetterRecord struct {
	Topic         string
	Partition     int32
	Offset        int64
	Key           []byte
	Payload       []byte
	ErrorText     string
	ErrorClass    model.DeadLetterErrorClass
	Attempts      int
	FirstFailedAt time.Time
}

// Send пишет в Kafka DLQ и в dead_letters. nil = можно MarkMessage.
type FailedMessageHandler interface {
	Send(ctx context.Context, rec DeadLetterRecord) error
}
