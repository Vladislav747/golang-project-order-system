//go:build e2e || e2e_async

package e2e

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/Vladislav747/golang-project-order-system/internal/model"
)

func requireOrderEvent(
	t *testing.T,
	events []model.OrderEvent,
	orderID uuid.UUID,
	eventType model.EventType,
	source model.EventSource,
) {
	t.Helper()
	for _, e := range events {
		if e.OrderID == orderID && e.EventType == eventType {
			require.Equal(t, source, e.Source)
			return
		}
	}
	require.Failf(t, "event not found", "order_id=%s event_type=%s source=%s", orderID, eventType, source)
}
