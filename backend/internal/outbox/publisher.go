package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/model"
	"recap-personalization/internal/telemetry"
)

type Queue interface {
	ClaimOutboxEvents(ctx context.Context, publisherID string, limit int, leaseDuration time.Duration) ([]model.OutboxEvent, error)
	MarkOutboxEventPublished(ctx context.Context, id uuid.UUID, publisherID string) error
	RequeueOutboxEvent(ctx context.Context, id uuid.UUID, publisherID string, delay time.Duration, message string) error
}

type Producer interface {
	Publish(ctx context.Context, topic, key string, value interface{}) error
}

type Config struct {
	PublisherID   string
	PollInterval  time.Duration
	LeaseDuration time.Duration
	BatchSize     int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

type Publisher struct {
	queue    Queue
	producer Producer
	config   Config
}

func NewPublisher(queue Queue, producer Producer, config Config) *Publisher {
	return &Publisher{queue: queue, producer: producer, config: normalizeConfig(config)}
}

func (p *Publisher) Run(ctx context.Context) error {
	log.Printf("outbox publisher %s started", p.config.PublisherID)
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		events, err := p.queue.ClaimOutboxEvents(
			ctx,
			p.config.PublisherID,
			p.config.BatchSize,
			p.config.LeaseDuration,
		)
		if err != nil {
			log.Printf("outbox publisher claim failed: %v", err)
			if !sleepContext(ctx, p.config.PollInterval) {
				return nil
			}
			continue
		}
		if len(events) == 0 {
			if !sleepContext(ctx, p.config.PollInterval) {
				return nil
			}
			continue
		}
		telemetry.OutboxClaimed.Add(float64(len(events)))
		for index := range events {
			p.publishOne(ctx, &events[index])
		}
	}
}

func (p *Publisher) publishOne(ctx context.Context, event *model.OutboxEvent) {
	var value interface{}
	if err := json.Unmarshal(event.Payload, &value); err != nil {
		p.requeue(ctx, event, fmt.Errorf("decode outbox payload: %w", err))
		return
	}
	// Timer 1: the HTTP produce call to Redpanda only (unchanged boundaries).
	publishStarted := time.Now()
	err := p.producer.Publish(ctx, event.Topic, event.EventKey, value)
	telemetry.OutboxPublishDuration.Observe(time.Since(publishStarted).Seconds())
	if err != nil {
		p.requeue(ctx, event, err)
		return
	}
	// Timer 2: the PostgreSQL UPDATE that records the publication result.
	markStarted := time.Now()
	markErr := p.queue.MarkOutboxEventPublished(ctx, event.ID, p.config.PublisherID)
	markResult := telemetry.OutboxRecordPublished
	if markErr != nil {
		markResult = telemetry.OutboxRecordError
	}
	telemetry.OutboxRecordDuration.Observe(time.Since(markStarted).Seconds(), markResult)
	if markErr != nil {
		telemetry.OutboxPublishes.Inc("mark_failed")
		log.Printf("outbox publisher failed to mark %s published: %v", event.ID, markErr)
		return
	}
	telemetry.OutboxPublishes.Inc("published")
}

// EffectiveConfig returns the configuration after defaults and normalization.
func (p *Publisher) EffectiveConfig() Config {
	return p.config
}

func (p *Publisher) requeue(ctx context.Context, event *model.OutboxEvent, cause error) {
	delay := p.retryDelay(event.AttemptCount)
	telemetry.OutboxPublishes.Inc("requeued")
	telemetry.OutboxRetryDelay.Observe(delay.Seconds())
	requeueStarted := time.Now()
	err := p.queue.RequeueOutboxEvent(
		ctx,
		event.ID,
		p.config.PublisherID,
		delay,
		cause.Error(),
	)
	result := telemetry.OutboxRecordRequeued
	if err != nil {
		result = telemetry.OutboxRecordError
	}
	telemetry.OutboxRecordDuration.Observe(time.Since(requeueStarted).Seconds(), result)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("outbox publisher failed to requeue %s: %v", event.ID, err)
	}
}

func (p *Publisher) retryDelay(attempt int) time.Duration {
	delay := p.config.RetryBase
	for index := 1; index < attempt; index++ {
		if delay >= p.config.RetryMax/2 {
			return p.config.RetryMax
		}
		delay *= 2
	}
	if delay > p.config.RetryMax {
		return p.config.RetryMax
	}
	return delay
}

func normalizeConfig(value Config) Config {
	if value.PublisherID == "" {
		value.PublisherID = "outbox-unknown"
	}
	if value.PollInterval <= 0 {
		value.PollInterval = 250 * time.Millisecond
	}
	if value.LeaseDuration <= 0 {
		value.LeaseDuration = 15 * time.Second
	}
	if value.BatchSize <= 0 {
		value.BatchSize = 50
	}
	if value.RetryBase <= 0 {
		value.RetryBase = time.Second
	}
	if value.RetryMax < value.RetryBase {
		value.RetryMax = 30 * time.Second
	}
	return value
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
