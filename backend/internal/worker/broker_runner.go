package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/broker"
	"recap-personalization/internal/eventing"
	"recap-personalization/internal/repository"
)

type CommandConsumer interface {
	Poll(ctx context.Context, timeout time.Duration) ([]broker.Record, error)
	Commit(ctx context.Context, records []broker.Record) error
	Close(ctx context.Context) error
}

var ErrCommandDeferred = errors.New("generation_command_deferred")

type Inbox interface {
	HasConsumedEvent(ctx context.Context, consumerName string, eventID uuid.UUID) (bool, error)
	RecordConsumedEvent(
		ctx context.Context,
		consumerName string,
		eventID uuid.UUID,
		topic string,
		partition int,
		offset int64,
	) (bool, error)
}

type RequestDispatcher interface {
	ProcessRequestByID(ctx context.Context, requestID string) (bool, error)
}

type BrokerCommandRunner struct {
	consumer     CommandConsumer
	inbox        Inbox
	dispatcher   RequestDispatcher
	publisher    EventPublisher
	consumerName string
	pollTimeout  time.Duration
}

func NewBrokerCommandRunner(
	consumer CommandConsumer,
	inbox Inbox,
	dispatcher RequestDispatcher,
	publisher EventPublisher,
	consumerName string,
	pollTimeout time.Duration,
) *BrokerCommandRunner {
	if pollTimeout <= 0 {
		pollTimeout = time.Second
	}
	return &BrokerCommandRunner{
		consumer: consumer, inbox: inbox, dispatcher: dispatcher,
		publisher: publisher, consumerName: consumerName, pollTimeout: pollTimeout,
	}
}

func (r *BrokerCommandRunner) Run(ctx context.Context) error {
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.consumer.Close(closeCtx); err != nil {
			log.Printf("close Redpanda consumer: %v", err)
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		records, err := r.consumer.Poll(ctx, r.pollTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for _, record := range records {
			if err := r.handleRecord(ctx, record); err != nil {
				return err
			}
		}
	}
}

func (r *BrokerCommandRunner) handleRecord(ctx context.Context, record broker.Record) error {
	var command eventing.GenerateRecapCommandV1
	if err := json.Unmarshal(record.Value, &command); err != nil || command.CommandID == uuid.Nil || command.RecapID == uuid.Nil {
		message := "invalid generation command"
		if err != nil {
			message = err.Error()
		}
		r.publishInvalidCommand(ctx, command, message)
		return r.consumer.Commit(ctx, []broker.Record{record})
	}

	consumed, err := r.inbox.HasConsumedEvent(ctx, r.consumerName, command.CommandID)
	if err != nil {
		return fmt.Errorf("check command inbox: %w", err)
	}
	if consumed {
		return r.consumer.Commit(ctx, []broker.Record{record})
	}

	handled, err := r.dispatcher.ProcessRequestByID(ctx, command.RecapID.String())
	if err != nil {
		if errors.Is(err, repository.ErrRecapRequestNotFound) {
			r.publishInvalidCommand(ctx, command, "recap request not found")
			return r.consumer.Commit(ctx, []broker.Record{record})
		}
		return fmt.Errorf("dispatch recap request %s: %w", command.RecapID, err)
	}
	if !handled {
		return ErrCommandDeferred
	}

	if _, err := r.inbox.RecordConsumedEvent(
		ctx,
		r.consumerName,
		command.CommandID,
		record.Topic,
		record.Partition,
		record.Offset,
	); err != nil {
		return fmt.Errorf("record command inbox: %w", err)
	}
	if err := r.consumer.Commit(ctx, []broker.Record{record}); err != nil {
		return fmt.Errorf("commit generation command: %w", err)
	}
	return nil
}

func (r *BrokerCommandRunner) publishInvalidCommand(
	ctx context.Context,
	command eventing.GenerateRecapCommandV1,
	message string,
) {
	if r.publisher == nil {
		return
	}
	commandID := command.CommandID
	if commandID == uuid.Nil {
		commandID = uuid.New()
	}
	recapID := command.RecapID
	if recapID == uuid.Nil {
		recapID = commandID
	}
	event := eventing.FailedCommandV1{
		EventID:      uuid.New(),
		CommandID:    commandID,
		RecapID:      recapID,
		ErrorCode:    "invalid_command",
		ErrorMessage: message,
		FailedAt:     time.Now().UTC(),
	}
	if err := r.publisher.PublishFailedCommand(ctx, event); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("publish invalid command to DLQ: %v", err)
	}
}
