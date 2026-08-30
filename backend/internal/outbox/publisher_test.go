package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/model"
)

type fakeQueue struct {
	mu        sync.Mutex
	events    []model.OutboxEvent
	published []uuid.UUID
	requeued  []uuid.UUID
}

func (q *fakeQueue) ClaimOutboxEvents(context.Context, string, int, time.Duration) ([]model.OutboxEvent, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	values := q.events
	q.events = nil
	return values, nil
}
func (q *fakeQueue) MarkOutboxEventPublished(_ context.Context, id uuid.UUID, _ string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.published = append(q.published, id)
	return nil
}
func (q *fakeQueue) RequeueOutboxEvent(_ context.Context, id uuid.UUID, _ string, _ time.Duration, _ string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.requeued = append(q.requeued, id)
	return nil
}

type fakeProducer struct{ err error }

func (p fakeProducer) Publish(context.Context, string, string, interface{}) error { return p.err }

func TestPublishOneMarksPublished(t *testing.T) {
	id := uuid.New()
	queue := &fakeQueue{}
	publisher := NewPublisher(queue, fakeProducer{}, Config{PublisherID: "test"})
	publisher.publishOne(context.Background(), &model.OutboxEvent{
		ID: id, Topic: "topic", EventKey: "key", Payload: []byte(`{"id":"value"}`), AttemptCount: 1,
	})
	if len(queue.published) != 1 || queue.published[0] != id {
		t.Fatalf("expected published event, got %+v", queue.published)
	}
}

func TestPublishOneRequeuesBrokerFailure(t *testing.T) {
	id := uuid.New()
	queue := &fakeQueue{}
	publisher := NewPublisher(queue, fakeProducer{err: errors.New("broker down")}, Config{PublisherID: "test"})
	publisher.publishOne(context.Background(), &model.OutboxEvent{
		ID: id, Topic: "topic", EventKey: "key", Payload: []byte(`{"id":"value"}`), AttemptCount: 1,
	})
	if len(queue.requeued) != 1 || queue.requeued[0] != id {
		t.Fatalf("expected requeued event, got %+v", queue.requeued)
	}
}
