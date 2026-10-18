//go:build !integration

package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextRetryAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	initial := time.Second
	max := 8 * time.Second

	tests := []struct {
		attempts int
		want     time.Duration
	}{
		{attempts: 1, want: time.Second},
		{attempts: 2, want: 2 * time.Second},
		{attempts: 3, want: 4 * time.Second},
		{attempts: 4, want: 8 * time.Second},
		{attempts: 5, want: 8 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.want.String(), func(t *testing.T) {
			t.Parallel()
			got := nextRetryAt(now, tt.attempts, initial, max)
			require.Equal(t, now.Add(tt.want), got)
		})
	}
}
