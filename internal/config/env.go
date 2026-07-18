package config

import (
	"fmt"
	"os"
	"strings"
)

type DatabaseConfig struct {
	URL string
}

type KafkaConfig struct {
	Brokers       []string
	TopicOrders   string
	ConsumerGroup string
}

func loadEnv(cfg *Config) {
	cfg.Database = DatabaseConfig{
		URL: os.Getenv("DATABASE_URL"),
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	cfg.Kafka = KafkaConfig{
		Brokers:       splitCSV(brokers),
		TopicOrders:   os.Getenv("KAFKA_TOPIC_ORDERS"),
		ConsumerGroup: os.Getenv("KAFKA_CONSUMER_GROUP"),
	}
}

func (cfg *Config) validateEnv() error {
	if cfg.Database.URL == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is not set")
	}
	if cfg.Kafka.TopicOrders == "" {
		return fmt.Errorf("KAFKA_TOPIC_ORDERS is not set")
	}
	if cfg.Kafka.ConsumerGroup == "" {
		return fmt.Errorf("KAFKA_CONSUMER_GROUP is not set")
	}
	return nil
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
