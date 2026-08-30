package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"recap-personalization/internal/broker"
	"recap-personalization/internal/config"
	"recap-personalization/internal/recap/narrative"
	"recap-personalization/internal/recap/pipeline"
	"recap-personalization/internal/repository"
	"recap-personalization/internal/service"
	"recap-personalization/internal/worker"
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

	clickHouse := database.NewClickHouseHTTP(
		cfg.ClickHouseURL,
		cfg.ClickHouseDB,
		cfg.ClickHouseUser,
		cfg.ClickHousePass,
	)
	pingContext, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := clickHouse.Ping(pingContext); err != nil {
		cancelPing()
		log.Fatal(err)
	}
	cancelPing()

	var provider narrative.Provider
	if cfg.MistralAPIKey != "" {
		provider = narrative.MistralHTTPProvider{
			APIKey:   cfg.MistralAPIKey,
			Model:    cfg.MistralModel,
			Endpoint: cfg.MistralEndpoint,
			Timeout:  cfg.MistralTimeout,
		}
	}
	generator := pipeline.NewGenerator(provider)
	for _, host := range cfg.PublicAvatarHosts {
		generator.Registry.PublicAvatarHosts[host] = struct{}{}
	}

	postgresRepository := repository.NewRepository(postgres)
	clickHouseRepository := repository.NewClickHouseRepository(clickHouse)
	appService := service.NewService(
		postgresRepository,
		clickHouseRepository,
		clickHouseRepository,
		generator,
	)
	appService.ConfigureAsync(cfg.RecapPollAfter, cfg.WorkerMaxAttempts)

	id := workerID()
	runner := worker.NewRunner(postgresRepository, appService, worker.Config{
		WorkerID:        id,
		PollInterval:    cfg.WorkerPollInterval,
		LeaseDuration:   cfg.WorkerLeaseDuration,
		Heartbeat:       cfg.WorkerHeartbeat,
		RetryBase:       cfg.WorkerRetryBase,
		RetryMax:        cfg.WorkerRetryMax,
		ShutdownTimeout: cfg.WorkerShutdownTimeout,
	})

	proxy := broker.NewHTTPProxy(cfg.RedpandaHTTPURL, cfg.RedpandaHTTPTimeout)
	eventPublisher := broker.EventPublisher{
		Proxy: proxy, LifecycleTopic: cfg.RedpandaLifecycleTopic, DLQTopic: cfg.RedpandaDLQTopic,
	}
	runner.SetEventPublisher(eventPublisher)
	appService.ConfigureLifecyclePublisher(eventPublisher)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 3)
	go func() { errCh <- runner.RunMaintenance(ctx) }()

	switch cfg.WorkerCommandSource {
	case "broker":
		log.Printf("worker command source: Redpanda")
		go func() { errCh <- runBrokerCommands(ctx, proxy, postgresRepository, runner, eventPublisher, cfg, id) }()
	case "database":
		log.Printf("worker command source: PostgreSQL queue")
		go func() { errCh <- runner.Run(ctx) }()
	case "hybrid":
		log.Printf("worker command source: Redpanda with PostgreSQL recovery polling")
		go func() { errCh <- runner.Run(ctx) }()
		go func() { errCh <- runBrokerCommands(ctx, proxy, postgresRepository, runner, eventPublisher, cfg, id) }()
	default:
		log.Fatalf("unsupported WORKER_COMMAND_SOURCE %q: expected broker, database or hybrid", cfg.WorkerCommandSource)
	}

	select {
	case <-ctx.Done():
		return
	case err := <-errCh:
		if err != nil {
			log.Fatal(err)
		}
	}
}

func runBrokerCommands(
	ctx context.Context,
	proxy *broker.HTTPProxy,
	inbox *repository.Repository,
	dispatcher *worker.Runner,
	publisher broker.EventPublisher,
	cfg config.Config,
	workerID string,
) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		consumer, err := proxy.NewConsumer(ctx, broker.ConsumerConfig{
			Group:           cfg.RedpandaConsumerGroup,
			Name:            workerID + "-" + uuid.NewString()[:8],
			Topics:          []string{cfg.RedpandaCommandTopic},
			AutoOffsetReset: "earliest",
			RequestTimeout:  10 * time.Second,
		})
		if err != nil {
			log.Printf("create Redpanda command consumer: %v", err)
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
		commandRunner := worker.NewBrokerCommandRunner(
			consumer,
			inbox,
			dispatcher,
			publisher,
			cfg.RedpandaConsumerGroup,
			time.Second,
		)
		if err := commandRunner.Run(ctx); err != nil && ctx.Err() == nil {
			delay := time.Second
			if errors.Is(err, worker.ErrCommandDeferred) {
				delay = 250 * time.Millisecond
			} else {
				log.Printf("Redpanda command consumer stopped: %v", err)
			}
			if !sleep(ctx, delay) {
				return nil
			}
			continue
		}
		return nil
	}
}

func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func workerID() string {
	if value := os.Getenv("WORKER_ID"); value != "" {
		return value
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}
	return hostname + "-" + uuid.NewString()[:8]
}
