package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServerPort             string
	DatabaseURL            string
	ClickHouseURL          string
	ClickHouseDB           string
	ClickHouseUser         string
	ClickHousePass         string
	MistralAPIKey          string
	MistralModel           string
	MistralEndpoint        string
	MistralTimeout         time.Duration
	PublicAvatarHosts      []string
	WorkerPollInterval     time.Duration
	WorkerLeaseDuration    time.Duration
	WorkerHeartbeat        time.Duration
	WorkerRetryBase        time.Duration
	WorkerRetryMax         time.Duration
	WorkerMaxAttempts      int
	WorkerShutdownTimeout  time.Duration
	WorkerCommandSource    string
	RecapPollAfter         time.Duration
	RedpandaHTTPURL        string
	RedpandaCommandTopic   string
	RedpandaLifecycleTopic string
	RedpandaDLQTopic       string
	RedpandaConsumerGroup  string
	RedpandaHTTPTimeout    time.Duration
	OutboxPollInterval     time.Duration
	OutboxLeaseDuration    time.Duration
	OutboxBatchSize        int
	SSEPollInterval        time.Duration
	SSEHeartbeatInterval   time.Duration
}

func Load() Config {
	return Config{
		ServerPort:             env("SERVER_PORT", "8080"),
		DatabaseURL:            databaseURL(),
		ClickHouseURL:          env("CLICKHOUSE_URL", "http://localhost:8123"),
		ClickHouseDB:           env("CLICKHOUSE_DATABASE", "recap"),
		ClickHouseUser:         env("CLICKHOUSE_USER", "default"),
		ClickHousePass:         os.Getenv("CLICKHOUSE_PASSWORD"),
		MistralAPIKey:          os.Getenv("MISTRAL_API_KEY"),
		MistralModel:           env("MISTRAL_MODEL", "mistral-small-latest"),
		MistralEndpoint:        os.Getenv("MISTRAL_ENDPOINT"),
		MistralTimeout:         durationEnv("MISTRAL_TIMEOUT", 15*time.Second),
		PublicAvatarHosts:      csvEnv("PUBLIC_AVATAR_HOSTS"),
		WorkerPollInterval:     durationEnv("WORKER_POLL_INTERVAL", 500*time.Millisecond),
		WorkerLeaseDuration:    durationEnv("WORKER_LEASE_DURATION", 30*time.Second),
		WorkerHeartbeat:        durationEnv("WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
		WorkerRetryBase:        durationEnv("WORKER_RETRY_BASE", 2*time.Second),
		WorkerRetryMax:         durationEnv("WORKER_RETRY_MAX", 30*time.Second),
		WorkerMaxAttempts:      intEnv("WORKER_MAX_ATTEMPTS", 3),
		WorkerShutdownTimeout:  durationEnv("WORKER_SHUTDOWN_TIMEOUT", 15*time.Second),
		WorkerCommandSource:    strings.ToLower(env("WORKER_COMMAND_SOURCE", "hybrid")),
		RecapPollAfter:         durationEnv("RECAP_POLL_AFTER", 500*time.Millisecond),
		RedpandaHTTPURL:        env("REDPANDA_HTTP_URL", "http://localhost:8082"),
		RedpandaCommandTopic:   env("REDPANDA_COMMAND_TOPIC", "recap.generation-commands.v1"),
		RedpandaLifecycleTopic: env("REDPANDA_LIFECYCLE_TOPIC", "recap.lifecycle.v1"),
		RedpandaDLQTopic:       env("REDPANDA_DLQ_TOPIC", "recap.generation-commands.dlq.v1"),
		RedpandaConsumerGroup:  env("REDPANDA_CONSUMER_GROUP", "recap-workers-v1"),
		RedpandaHTTPTimeout:    durationEnv("REDPANDA_HTTP_TIMEOUT", 15*time.Second),
		OutboxPollInterval:     durationEnv("OUTBOX_POLL_INTERVAL", 250*time.Millisecond),
		OutboxLeaseDuration:    durationEnv("OUTBOX_LEASE_DURATION", 15*time.Second),
		OutboxBatchSize:        intEnv("OUTBOX_BATCH_SIZE", 50),
		SSEPollInterval:        durationEnv("SSE_POLL_INTERVAL", 250*time.Millisecond),
		SSEHeartbeatInterval:   durationEnv("SSE_HEARTBEAT_INTERVAL", 10*time.Second),
	}
}

func databaseURL() string {
	if value := os.Getenv("DATABASE_URL"); value != "" {
		return value
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		env("DB_USER", "avito"),
		env("DB_PASSWORD", "avito_password"),
		env("DB_HOST", "localhost"),
		env("DB_PORT", "5432"),
		env("DB_NAME", "avito_recap"),
		env("DB_SSLMODE", "disable"),
	)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	if parsed, err := time.ParseDuration(value); err == nil {
		return parsed
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func csvEnv(key string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
