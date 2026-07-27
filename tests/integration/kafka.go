//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/IBM/sarama"
)

const (
	IntegrationOrdersTopic = "orders.create"
)

func setupKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()

	container, err := tcKafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})
	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	createTopic(t, brokers, IntegrationOrdersTopic)

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
        NumPartitions: 1, ReplicationFactor: 1,
    }, false)
	if err != nil {
		require.Contains(t, err.Error(), "already exists")
	}
}