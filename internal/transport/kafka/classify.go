package kafka

import (
	"errors"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

// true — ошибку можно повторить. По умолчанию всё retryable.
func classify(err error) bool {
	if model.IsNonRetryable(err) || errors.Is(err, model.ErrOrderNotFound) {
		return false
	}
	return true
}
