//go:build !integration

package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

func TestClassify(t *testing.T) {
	t.Parallel()

	jsonErr := json.Unmarshal([]byte("{"), &struct{}{})

	tests := []struct {
		name string
		err  error
		want bool
	}{
		// заполни сам
		{name: "broken json", err: model.NonRetryable(jsonErr), want: false},
		{name: "unknown action", err: model.NonRetryable(errors.New("unknown action")), want: false},
		{name: "order not found", err: model.ErrOrderNotFound, want: false},
		{name: "wrapped not found", err: fmt.Errorf("service: %w", model.ErrOrderNotFound), want: false},
		{name: "io timeout", err: errors.New("i/o timeout"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, classify(tt.err))
		})
	}

}
