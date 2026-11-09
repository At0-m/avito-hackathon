package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
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
	"recap-personalization/internal/telemetry"
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

	narrativeMode, err := cfg.NarrativeProviderMode()
	if err != nil {
		log.Fatal(err)
	}
	var provider narrative.Provider
	if narrativeMode == config.NarrativeProviderMistral {
		provider = narrative.MistralHTTPProvider{
			APIKey:   cfg.MistralAPIKey,
			Model:    cfg.MistralModel,
			Endpoint: cfg.MistralEndpoint,
			Timeout:  cfg.MistralTimeout,
		}
	}
	log.Printf("narrative provider mode: %s", narrativeMode)
	telemetry.NarrativeProviderMode.Set(1, narrativeMode)
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

	recordWorkerConfig(cfg, runner.EffectiveConfig())
	telemetry.Default.RegisterDBStats("postgres", postgres.DB)
	telemetry.StartServer(ctx, cfg.MetricsAddr)

	errCh := make(chan error, 3)
	var loops sync.WaitGroup
	start := func(loop func() error) {
		loops.Add(1)
		go func() {
			defer loops.Done()
			errCh <- loop()
		}()
	}
	start(func() error { return runner.RunMaintenance(ctx) })

	switch cfg.WorkerCommandSource {
	case "broker":
		log.Printf("worker command source: Redpanda")
		start(func() error { return runBrokerCommands(ctx, proxy, postgresRepository, runner, eventPublisher, cfg, id) })
	case "database":
		log.Printf("worker command source: PostgreSQL queue")
		start(func() error { return runner.Run(ctx) })
	case "hybrid":
		log.Printf("worker command source: Redpanda with PostgreSQL recovery polling")
		start(func() error { return runner.Run(ctx) })
		start(func() error { return runBrokerCommands(ctx, proxy, postgresRepository, runner, eventPublisher, cfg, id) })
	default:
		log.Fatalf("unsupported WORKER_COMMAND_SOURCE %q: expected broker, database or hybrid", cfg.WorkerCommandSource)
	}

	var loopErr error
	select {
	case <-ctx.Done():
	case loopErr = <-errCh:
		if loopErr != nil {
			log.Printf("worker loop failed: %v", loopErr)
		}
	}
	// Wait for the loops to finish: an interrupted attempt is requeued and
	// the Redpanda consumer instance is deleted from the HTTP Proxy. Without
	// the DELETE the dead instance stays a group member and keeps its
	// partitions until pandaproxy.consumer_instance_timeout_ms (5 min).
	stop()
	if waitTimeout(&loops, cfg.WorkerShutdownTimeout) {
		log.Printf("worker %s stopped gracefully", id)
	} else {
		log.Printf("worker %s shutdown timed out after %s", id, cfg.WorkerShutdownTimeout)
	}
	if loopErr != nil {
		_ = postgres.Close()
		os.Exit(1)
	}
}

// waitTimeout waits for the group and reports whether it finished in time.
func waitTimeout(group *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
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
			RequestTimeout:  brokerConsumerReqTimeout,
		})
		if err != nil {
			telemetry.BrokerConsumerRestarts.Inc("create_failed")
			log.Printf("create Redpanda command consumer: %v", err)
			if !sleep(ctx, brokerErrorBackoff) {
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
			brokerPollTimeout,
		)
		telemetry.BrokerConsumerActive.Set(1)
		err = commandRunner.Run(ctx)
		telemetry.BrokerConsumerActive.Set(0)
		if err != nil && ctx.Err() == nil {
			delay := brokerErrorBackoff
			if errors.Is(err, worker.ErrCommandDeferred) {
				// The command targets a request with an active lease elsewhere
				// (or a not yet due retry): the consumer is recreated and the
				// uncommitted record is redelivered until the claim succeeds.
				delay = brokerDeferredBackoff
				telemetry.BrokerConsumerRestarts.Inc("deferred")
			} else {
				telemetry.BrokerConsumerRestarts.Inc("error")
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

// Fixed reconnect delays of the Redpanda command consumer. They are not
// configurable; the effective values are exported via recap_config_value.
const (
	brokerErrorBackoff       = time.Second
	brokerDeferredBackoff    = 250 * time.Millisecond
	brokerPollTimeout        = time.Second
	brokerConsumerReqTimeout = 10 * time.Second
)

func recordWorkerConfig(cfg config.Config, effective worker.Config) {
	telemetry.SetConfigSeconds("worker_poll_interval_seconds", effective.PollInterval.Seconds())
	telemetry.SetConfigSeconds("worker_lease_duration_seconds", effective.LeaseDuration.Seconds())
	telemetry.SetConfigSeconds("worker_heartbeat_interval_seconds", effective.Heartbeat.Seconds())
	telemetry.SetConfigSeconds("worker_retry_base_seconds", effective.RetryBase.Seconds())
	telemetry.SetConfigSeconds("worker_retry_max_seconds", effective.RetryMax.Seconds())
	telemetry.SetConfigSeconds("worker_shutdown_timeout_seconds", effective.ShutdownTimeout.Seconds())
	// max_attempts is stamped on each request by the API at creation time;
	// this value only matters for requests created with the worker's config.
	telemetry.ConfigValue.Set(float64(cfg.WorkerMaxAttempts), "worker_max_attempts")
	telemetry.SetConfigSeconds("mistral_timeout_seconds", cfg.MistralTimeout.Seconds())
	telemetry.SetConfigSeconds("redpanda_http_timeout_seconds", cfg.RedpandaHTTPTimeout.Seconds())
	telemetry.SetConfigSeconds("broker_error_backoff_seconds", brokerErrorBackoff.Seconds())
	telemetry.SetConfigSeconds("broker_deferred_backoff_seconds", brokerDeferredBackoff.Seconds())
	telemetry.SetConfigSeconds("broker_poll_timeout_seconds", brokerPollTimeout.Seconds())
	telemetry.SetConfigSeconds("broker_consumer_request_timeout_seconds", brokerConsumerReqTimeout.Seconds())
	source := 0.0
	switch cfg.WorkerCommandSource {
	case "database":
		source = 1
	case "broker":
		source = 2
	case "hybrid":
		source = 3
	}
	// 1=database, 2=broker, 3=hybrid.
	telemetry.ConfigValue.Set(source, "worker_command_source")
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
