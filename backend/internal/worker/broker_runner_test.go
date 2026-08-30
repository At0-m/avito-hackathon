package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/broker"
	"recap-personalization/internal/eventing"
	"recap-personalization/internal/repository"
)

type fakeCommandConsumer struct {
	mu      sync.Mutex
	commits int
}

func (c *fakeCommandConsumer) Poll(context.Context, time.Duration) ([]broker.Record, error) {
	return nil, nil
}
func (c *fakeCommandConsumer) Commit(_ context.Context, records []broker.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits += len(records)
	return nil
}
func (c *fakeCommandConsumer) Close(context.Context) error { return nil }

type fakeInbox struct {
	mu        sync.Mutex
	seen      map[uuid.UUID]struct{}
	recordErr error
}

func (i *fakeInbox) HasConsumedEvent(_ context.Context, _ string, eventID uuid.UUID) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	_, exists := i.seen[eventID]
	return exists, nil
}
func (i *fakeInbox) RecordConsumedEvent(
	_ context.Context,
	_ string,
	eventID uuid.UUID,
	_ string,
	_ int,
	_ int64,
) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.recordErr != nil {
		return false, i.recordErr
	}
	if _, exists := i.seen[eventID]; exists {
		return false, nil
	}
	i.seen[eventID] = struct{}{}
	return true, nil
}

type fakeDispatcher struct {
	mu      sync.Mutex
	calls   int
	handled bool
	err     error
}

func (d *fakeDispatcher) ProcessRequestByID(context.Context, string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	return d.handled, d.err
}

func TestBrokerRunnerDeduplicatesCommandsWithInbox(t *testing.T) {
	consumer := &fakeCommandConsumer{}
	inbox := &fakeInbox{seen: make(map[uuid.UUID]struct{})}
	dispatcher := &fakeDispatcher{handled: true}
	runner := NewBrokerCommandRunner(consumer, inbox, dispatcher, nil, "worker", time.Second)
	command := eventing.GenerateRecapCommandV1{
		CommandID: uuid.New(), RecapID: uuid.New(), ProfileID: uuid.New(), Year: 2026,
		RequestedAt: time.Now().UTC(),
	}
	payload, _ := json.Marshal(command)
	record := broker.Record{Topic: eventing.GenerationCommandsTopic, Value: payload, Partition: 0, Offset: 1}

	if err := runner.handleRecord(context.Background(), record); err != nil {
		t.Fatalf("first command: %v", err)
	}
	if err := runner.handleRecord(context.Background(), record); err != nil {
		t.Fatalf("duplicate command: %v", err)
	}

	dispatcher.mu.Lock()
	calls := dispatcher.calls
	dispatcher.mu.Unlock()
	if calls != 1 {
		t.Fatalf("duplicate command dispatched %d times", calls)
	}
	consumer.mu.Lock()
	commits := consumer.commits
	consumer.mu.Unlock()
	if commits != 2 {
		t.Fatalf("expected both deliveries committed, got %d", commits)
	}
}

func TestBrokerRunnerCommitsPoisonCommand(t *testing.T) {
	consumer := &fakeCommandConsumer{}
	runner := NewBrokerCommandRunner(
		consumer,
		&fakeInbox{seen: make(map[uuid.UUID]struct{})},
		&fakeDispatcher{handled: true},
		nil,
		"worker",
		time.Second,
	)
	if err := runner.handleRecord(context.Background(), broker.Record{
		Topic: eventing.GenerationCommandsTopic, Value: []byte(`{"broken":`), Partition: 0, Offset: 2,
	}); err != nil {
		t.Fatalf("poison command: %v", err)
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.commits != 1 {
		t.Fatalf("poison command must be committed, got %d", consumer.commits)
	}
}

func TestBrokerRunnerDefersBusyRequestWithoutInboxOrCommit(t *testing.T) {
	consumer := &fakeCommandConsumer{}
	inbox := &fakeInbox{seen: make(map[uuid.UUID]struct{})}
	dispatcher := &fakeDispatcher{handled: false}
	runner := NewBrokerCommandRunner(consumer, inbox, dispatcher, nil, "worker", time.Second)
	command := eventing.GenerateRecapCommandV1{
		CommandID: uuid.New(), RecapID: uuid.New(), ProfileID: uuid.New(), Year: 2026,
		RequestedAt: time.Now().UTC(),
	}
	payload, _ := json.Marshal(command)
	err := runner.handleRecord(context.Background(), broker.Record{
		Topic: eventing.GenerationCommandsTopic, Value: payload, Partition: 0, Offset: 3,
	})
	if !errors.Is(err, ErrCommandDeferred) {
		t.Fatalf("error=%v, want ErrCommandDeferred", err)
	}
	consumer.mu.Lock()
	commits := consumer.commits
	consumer.mu.Unlock()
	if commits != 0 {
		t.Fatalf("deferred command commits=%d, want 0", commits)
	}
	inbox.mu.Lock()
	_, consumed := inbox.seen[command.CommandID]
	inbox.mu.Unlock()
	if consumed {
		t.Fatal("deferred command must not be recorded in inbox")
	}
}

func TestBrokerRunnerCommitsCommandForMissingRequest(t *testing.T) {
	consumer := &fakeCommandConsumer{}
	inbox := &fakeInbox{seen: make(map[uuid.UUID]struct{})}
	dispatcher := &fakeDispatcher{err: repository.ErrRecapRequestNotFound}
	runner := NewBrokerCommandRunner(consumer, inbox, dispatcher, nil, "worker", time.Second)
	command := eventing.GenerateRecapCommandV1{
		CommandID: uuid.New(), RecapID: uuid.New(), ProfileID: uuid.New(), Year: 2026,
		RequestedAt: time.Now().UTC(),
	}
	payload, _ := json.Marshal(command)
	if err := runner.handleRecord(context.Background(), broker.Record{
		Topic: eventing.GenerationCommandsTopic, Value: payload, Partition: 0, Offset: 5,
	}); err != nil {
		t.Fatalf("missing request command: %v", err)
	}
	consumer.mu.Lock()
	commits := consumer.commits
	consumer.mu.Unlock()
	if commits != 1 {
		t.Fatalf("missing request command commits=%d, want 1", commits)
	}
	inbox.mu.Lock()
	_, consumed := inbox.seen[command.CommandID]
	inbox.mu.Unlock()
	if consumed {
		t.Fatal("missing request command must not be recorded in inbox")
	}
}

func TestBrokerRunnerDoesNotAcknowledgeWhenInboxWriteFails(t *testing.T) {
	consumer := &fakeCommandConsumer{}
	inbox := &fakeInbox{seen: make(map[uuid.UUID]struct{}), recordErr: errors.New("postgres unavailable")}
	dispatcher := &fakeDispatcher{handled: true}
	runner := NewBrokerCommandRunner(consumer, inbox, dispatcher, nil, "worker", time.Second)
	command := eventing.GenerateRecapCommandV1{
		CommandID: uuid.New(), RecapID: uuid.New(), ProfileID: uuid.New(), Year: 2026,
		RequestedAt: time.Now().UTC(),
	}
	payload, _ := json.Marshal(command)
	err := runner.handleRecord(context.Background(), broker.Record{
		Topic: eventing.GenerationCommandsTopic, Value: payload, Partition: 0, Offset: 4,
	})
	if err == nil {
		t.Fatal("expected inbox failure")
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.commits != 0 {
		t.Fatalf("failed inbox write commits=%d, want 0", consumer.commits)
	}
}
