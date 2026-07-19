//go:build e2e || e2e_async

package e2e

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const e2eOrdersTopic = "orders.create"

func setupKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()

	container, err := tcKafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		tcKafka.WithClusterID("e2e-order-cluster"),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)

	createTopic(t, brokers, e2eOrdersTopic)

	return brokers
}

func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		// топик мог уже существовать при повторном запуске
		require.Contains(t, err.Error(), "already exists")
	}
}
