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
