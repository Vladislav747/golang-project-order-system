package model

import (
	"errors"
	"fmt"
)

var ErrNonRetryable = errors.New("non-retryable")

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrNonRetryable, err)
}
func IsNonRetryable(err error) bool {
	return errors.Is(err, ErrNonRetryable)
}
