package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/model"
	recap "recap-personalization/internal/recap"
	"recap-personalization/internal/repository"
	"recap-personalization/internal/service"
)

type fakeQueue struct {
	mu          sync.Mutex
	extensions  int
	requeued    bool
	failed      bool
	failedCode  string
	failedRetry bool
	current     *model.RecapRequest
	getErr      error
	requeueErr  error
	failErr     error
	sweeps      int
}

func (f *fakeQueue) GetRecapRequestByID(context.Context, string) (*model.RecapRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.current == nil {
		return nil, repository.ErrRecapRequestNotFound
	}
	copy := *f.current
	return &copy, nil
}

func (f *fakeQueue) FailExpiredExhaustedRecapRequests(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeps++
	return 0, nil
}

func (f *fakeQueue) ClaimNextRecapRequest(context.Context, string, time.Duration) (*model.RecapRequest, error) {
	return nil, repository.ErrRecapRequestNotFound
}

func (f *fakeQueue) ClaimRecapRequestByID(context.Context, string, string, time.Duration) (*model.RecapRequest, error) {
	return nil, repository.ErrRecapRequestNotFound
}

func (f *fakeQueue) ExtendRecapRequestLease(context.Context, string, string, time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.extensions++
	return nil
}

func (f *fakeQueue) RequeueRecapRequest(context.Context, string, string, time.Duration, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requeued = true
	return f.requeueErr
}

func (f *fakeQueue) MarkRecapRequestFailed(_ context.Context, _, _, code, _ string, retryable bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = true
	f.failedCode = code
	f.failedRetry = retryable
	return f.failErr
}

type processorFunc func(context.Context, *model.RecapRequest, string) error

func (f processorFunc) ProcessRecapRequest(ctx context.Context, request *model.RecapRequest, workerID string) error {
	return f(ctx, request, workerID)
}

func testRequest(attempt, maxAttempts int) *model.RecapRequest {
	return &model.RecapRequest{
		ID:           uuid.New(),
		AttemptCount: attempt,
		MaxAttempts:  maxAttempts,
	}
}

func TestProcessClaimedRequeuesRetryableError(t *testing.T) {
	queue := &fakeQueue{}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		return service.ErrActivitySourceUnavailable
	}), Config{WorkerID: "test", RetryBase: time.Millisecond, RetryMax: time.Millisecond})

	runner.processClaimed(context.Background(), testRequest(1, 3))

	if !queue.requeued {
		t.Fatal("expected retryable request to be requeued")
	}
	if queue.failed {
		t.Fatal("did not expect request to be marked failed")
	}
}

func TestProcessClaimedDoesNotAcknowledgeWhenRetryStateCannotBePersisted(t *testing.T) {
	queue := &fakeQueue{requeueErr: errors.New("postgres unavailable")}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		return service.ErrActivitySourceUnavailable
	}), Config{WorkerID: "test", RetryBase: time.Millisecond, RetryMax: time.Millisecond})

	handled, err := runner.processClaimed(context.Background(), testRequest(1, 3))
	if err == nil || handled {
		t.Fatalf("handled=%v err=%v, want unacknowledged persistence failure", handled, err)
	}
}

func TestProcessClaimedMarksPermanentFailure(t *testing.T) {
	queue := &fakeQueue{}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		return recap.ErrInsufficientActivity
	}), Config{WorkerID: "test"})

	runner.processClaimed(context.Background(), testRequest(1, 3))

	if !queue.failed {
		t.Fatal("expected permanent error to mark request failed")
	}
	if queue.failedCode != "insufficient_activity" || queue.failedRetry {
		t.Fatalf("unexpected failure classification: code=%s retryable=%v", queue.failedCode, queue.failedRetry)
	}
}

func TestProcessClaimedDoesNotAcknowledgeWhenFailureStateCannotBePersisted(t *testing.T) {
	queue := &fakeQueue{failErr: errors.New("postgres unavailable")}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		return recap.ErrInsufficientActivity
	}), Config{WorkerID: "test"})

	handled, err := runner.processClaimed(context.Background(), testRequest(1, 3))
	if err == nil || handled {
		t.Fatalf("handled=%v err=%v, want unacknowledged persistence failure", handled, err)
	}
}

func TestProcessClaimedMarksExhaustedRetryableFailure(t *testing.T) {
	queue := &fakeQueue{}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		return service.ErrActivitySourceUnavailable
	}), Config{WorkerID: "test"})

	runner.processClaimed(context.Background(), testRequest(3, 3))

	if queue.requeued {
		t.Fatal("did not expect exhausted request to be requeued")
	}
	if !queue.failed || !queue.failedRetry {
		t.Fatal("expected exhausted transient error to be marked failed and retryable")
	}
}

func TestProcessClaimedExtendsLease(t *testing.T) {
	queue := &fakeQueue{}
	runner := NewRunner(queue, processorFunc(func(ctx context.Context, _ *model.RecapRequest, _ string) error {
		select {
		case <-time.After(35 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), Config{
		WorkerID:      "test",
		LeaseDuration: 30 * time.Millisecond,
		Heartbeat:     5 * time.Millisecond,
	})

	runner.processClaimed(context.Background(), testRequest(1, 3))

	queue.mu.Lock()
	extensions := queue.extensions
	queue.mu.Unlock()
	if extensions == 0 {
		t.Fatal("expected worker to extend the request lease")
	}
}

func TestProcessRequestByIDAcknowledgesTerminalDuplicate(t *testing.T) {
	queue := &fakeQueue{current: &model.RecapRequest{ID: uuid.New(), Status: model.RecapRequestReady}}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		t.Fatal("terminal duplicate must not be processed")
		return nil
	}), Config{WorkerID: "test"})

	handled, err := runner.ProcessRequestByID(context.Background(), queue.current.ID.String())
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v, want terminal duplicate acknowledged", handled, err)
	}
}

func TestProcessRequestByIDDefersActiveLease(t *testing.T) {
	queue := &fakeQueue{current: &model.RecapRequest{ID: uuid.New(), Status: model.RecapRequestProcessing}}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		t.Fatal("active lease must not be processed")
		return nil
	}), Config{WorkerID: "test"})

	handled, err := runner.ProcessRequestByID(context.Background(), queue.current.ID.String())
	if err != nil || handled {
		t.Fatalf("handled=%v err=%v, want deferred command", handled, err)
	}
}

func TestRunMaintenanceSweepsUntilCancellation(t *testing.T) {
	queue := &fakeQueue{}
	runner := NewRunner(queue, processorFunc(func(context.Context, *model.RecapRequest, string) error { return nil }), Config{
		WorkerID: "test", PollInterval: time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Millisecond)
	defer cancel()
	if err := runner.RunMaintenance(ctx); err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.sweeps == 0 {
		t.Fatal("expected at least one expired-request sweep")
	}
}

func TestClassifyLeaseLossIsHandledByRunner(t *testing.T) {
	if !errors.Is(repository.ErrRecapRequestLeaseLost, repository.ErrRecapRequestLeaseLost) {
		t.Fatal("lease loss sentinel must be comparable with errors.Is")
	}
}

func TestRetryDelayIsExponentialAndCapped(t *testing.T) {
	runner := NewRunner(&fakeQueue{}, processorFunc(func(context.Context, *model.RecapRequest, string) error {
		return nil
	}), Config{
		WorkerID:  "test",
		RetryBase: time.Second,
		RetryMax:  5 * time.Second,
	})

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 5 * time.Second},
		{attempt: 10, want: 5 * time.Second},
	}
	for _, tc := range cases {
		if got := runner.retryDelay(tc.attempt); got != tc.want {
			t.Fatalf("attempt %d: got %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestNormalizeConfigKeepsHeartbeatBelowLease(t *testing.T) {
	value := normalizeConfig(Config{
		WorkerID:      "test",
		LeaseDuration: 30 * time.Second,
		Heartbeat:     30 * time.Second,
	})
	if value.Heartbeat <= 0 || value.Heartbeat >= value.LeaseDuration {
		t.Fatalf("heartbeat %s must be positive and below lease %s", value.Heartbeat, value.LeaseDuration)
	}
}

type leaseLosingQueue struct {
	fakeQueue
}

func (q *leaseLosingQueue) ExtendRecapRequestLease(context.Context, string, string, time.Duration) error {
	return repository.ErrRecapRequestLeaseLost
}

func TestLeaseLossCancelsProcessingWithoutStaleFinalize(t *testing.T) {
	queue := &leaseLosingQueue{}
	runner := NewRunner(queue, processorFunc(func(ctx context.Context, _ *model.RecapRequest, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	}), Config{
		WorkerID:      "test",
		LeaseDuration: 20 * time.Millisecond,
		Heartbeat:     time.Millisecond,
	})

	runner.processClaimed(context.Background(), testRequest(1, 3))

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.requeued || queue.failed {
		t.Fatal("a worker that lost ownership must not mutate the request lifecycle")
	}
}
