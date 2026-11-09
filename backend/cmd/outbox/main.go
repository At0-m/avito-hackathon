package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"recap-personalization/internal/broker"
	"recap-personalization/internal/config"
	"recap-personalization/internal/outbox"
	"recap-personalization/internal/repository"
	"recap-personalization/internal/telemetry"
	"recap-personalization/pkg/database"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	postgres, err := database.NewPostgresDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer postgres.Close()

	proxy := broker.NewHTTPProxy(cfg.RedpandaHTTPURL, cfg.RedpandaHTTPTimeout)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := proxy.Health(ctx); err != nil {
		log.Printf("redpanda is not ready yet: %v", err)
	}

	publisher := outbox.NewPublisher(
		repository.NewRepository(postgres),
		proxy,
		outbox.Config{
			PublisherID:   publisherID(),
			PollInterval:  cfg.OutboxPollInterval,
			LeaseDuration: cfg.OutboxLeaseDuration,
			BatchSize:     cfg.OutboxBatchSize,
		},
	)
	effective := publisher.EffectiveConfig()
	telemetry.SetConfigSeconds("outbox_poll_interval_seconds", effective.PollInterval.Seconds())
	telemetry.SetConfigSeconds("outbox_lease_duration_seconds", effective.LeaseDuration.Seconds())
	telemetry.ConfigValue.Set(float64(effective.BatchSize), "outbox_batch_size")
	// RetryBase/RetryMax are not read from the environment: the publisher
	// always uses its defaults (1s doubling up to 30s, unbounded attempts).
	telemetry.SetConfigSeconds("outbox_retry_base_seconds", effective.RetryBase.Seconds())
	telemetry.SetConfigSeconds("outbox_retry_max_seconds", effective.RetryMax.Seconds())
	telemetry.SetConfigSeconds("redpanda_http_timeout_seconds", cfg.RedpandaHTTPTimeout.Seconds())
	telemetry.Default.RegisterDBStats("postgres", postgres.DB)
	telemetry.StartServer(ctx, cfg.MetricsAddr)

	if err := publisher.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func publisherID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "outbox"
	}
	return hostname + "-" + uuid.NewString()[:8]
}
