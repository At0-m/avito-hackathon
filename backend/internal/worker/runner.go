package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/eventing"
	"recap-personalization/internal/model"
	recap "recap-personalization/internal/recap"
	"recap-personalization/internal/repository"
	"recap-personalization/internal/service"
)

type Queue interface {
	GetRecapRequestByID(ctx context.Context, id string) (*model.RecapRequest, error)
	FailExpiredExhaustedRecapRequests(ctx context.Context) (int64, error)
	ClaimNextRecapRequest(ctx context.Context, workerID string, leaseDuration time.Duration) (*model.RecapRequest, error)
	ClaimRecapRequestByID(ctx context.Context, id, workerID string, leaseDuration time.Duration) (*model.RecapRequest, error)
	ExtendRecapRequestLease(ctx context.Context, id, workerID string, leaseDuration time.Duration) error
	RequeueRecapRequest(ctx context.Context, id, workerID string, delay time.Duration, code, message string) error
	MarkRecapRequestFailed(ctx context.Context, id, workerID, code, message string, retryable bool) error
}

type Processor interface {
	ProcessRecapRequest(ctx context.Context, request *model.RecapRequest, workerID string) error
}

type EventPublisher interface {
	PublishLifecycle(ctx context.Context, value eventing.RecapLifecycleV1) error
	PublishFailedCommand(ctx context.Context, value eventing.FailedCommandV1) error
}

type Config struct {
	WorkerID        string
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	Heartbeat       time.Duration
	RetryBase       time.Duration
	RetryMax        time.Duration
	ShutdownTimeout time.Duration
}

type Runner struct {
	queue     Queue
	processor Processor
	config    Config
	publisher EventPublisher
	workMu    sync.Mutex
}

func NewRunner(queue Queue, processor Processor, config Config) *Runner {
	return &Runner{queue: queue, processor: processor, config: normalizeConfig(config)}
}

func (r *Runner) SetEventPublisher(publisher EventPublisher) {
	r.publisher = publisher
}

func (r *Runner) Run(ctx context.Context) error {
	log.Printf("recap database-queue worker %s started", r.config.WorkerID)
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		handled, err := r.ProcessNext(ctx)
		if err != nil {
			log.Printf("worker %s failed to claim request: %v", r.config.WorkerID, err)
		}
		if !handled && !sleepContext(ctx, r.config.PollInterval) {
			return nil
		}
	}
}

func (r *Runner) RunMaintenance(ctx context.Context) error {
	log.Printf("recap worker maintenance %s started", r.config.WorkerID)
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if failed, err := r.queue.FailExpiredExhaustedRecapRequests(ctx); err != nil {
			log.Printf("worker %s failed to sweep expired requests: %v", r.config.WorkerID, err)
		} else if failed > 0 {
			log.Printf("worker %s marked %d exhausted requests failed", r.config.WorkerID, failed)
		}
		if !sleepContext(ctx, r.config.PollInterval) {
			return nil
		}
	}
}

func (r *Runner) ProcessNext(ctx context.Context) (bool, error) {
	r.workMu.Lock()
	defer r.workMu.Unlock()

	request, err := r.queue.ClaimNextRecapRequest(ctx, r.config.WorkerID, r.config.LeaseDuration)
	if errors.Is(err, repository.ErrRecapRequestNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	r.logClaim(request)
	r.publishLifecycle(ctx, request, model.RecapRequestProcessing, "starting", 5)
	_, processErr := r.processClaimed(ctx, request)
	return true, processErr
}

func (r *Runner) ProcessRequestByID(ctx context.Context, requestID string) (bool, error) {
	r.workMu.Lock()
	defer r.workMu.Unlock()

	request, err := r.queue.ClaimRecapRequestByID(
		ctx,
		requestID,
		r.config.WorkerID,
		r.config.LeaseDuration,
	)
	if errors.Is(err, repository.ErrRecapRequestNotFound) {
		current, readErr := r.queue.GetRecapRequestByID(ctx, requestID)
		if readErr != nil {
			return false, readErr
		}
		switch current.Status {
		case model.RecapRequestReady, model.RecapRequestFailed:
			return true, nil
		default:
			return false, nil
		}
	}
	if err != nil {
		return false, err
	}
	r.logClaim(request)
	r.publishLifecycle(ctx, request, model.RecapRequestProcessing, "starting", 5)
	return r.processClaimed(ctx, request)
}

func (r *Runner) logClaim(request *model.RecapRequest) {
	log.Printf(
		"worker %s claimed recap request %s (attempt %d/%d)",
		r.config.WorkerID,
		request.ID,
		request.AttemptCount,
		request.MaxAttempts,
	)
}

func (r *Runner) processClaimed(parent context.Context, request *model.RecapRequest) (bool, error) {
	processCtx, cancel := context.WithCancel(parent)
	defer cancel()

	heartbeatStopped := make(chan struct{})
	heartbeatResult := make(chan error, 1)
	go func() {
		heartbeatResult <- r.maintainLease(
			processCtx,
			cancel,
			request.ID.String(),
			heartbeatStopped,
		)
	}()

	err := r.processor.ProcessRecapRequest(processCtx, request, r.config.WorkerID)
	close(heartbeatStopped)
	leaseErr := <-heartbeatResult
	if leaseErr != nil {
		log.Printf("worker %s lost lease for request %s: %v", r.config.WorkerID, request.ID, leaseErr)
		if errors.Is(leaseErr, repository.ErrRecapRequestLeaseLost) {
			return false, nil
		}
		return false, leaseErr
	}

	if err == nil {
		r.publishLifecycle(parent, request, model.RecapRequestReady, "ready", 100)
		log.Printf("worker %s completed recap request %s", r.config.WorkerID, request.ID)
		return true, nil
	}
	if errors.Is(err, repository.ErrRecapRequestLeaseLost) {
		log.Printf("worker %s cannot finalize stale request %s", r.config.WorkerID, request.ID)
		return false, nil
	}

	code, retryable := classifyError(err)
	message := err.Error()

	if parent.Err() != nil {
		code = "worker_shutdown"
		retryable = true
	}

	operationCtx, operationCancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer operationCancel()

	if retryable && request.AttemptCount < request.MaxAttempts {
		delay := r.retryDelay(request.AttemptCount)
		if requeueErr := r.queue.RequeueRecapRequest(
			operationCtx,
			request.ID.String(),
			r.config.WorkerID,
			delay,
			code,
			message,
		); requeueErr != nil {
			log.Printf("worker %s failed to requeue request %s: %v", r.config.WorkerID, request.ID, requeueErr)
			if errors.Is(requeueErr, repository.ErrRecapRequestLeaseLost) {
				return false, nil
			}
			return false, requeueErr
		}
		r.publishLifecycle(operationCtx, request, model.RecapRequestQueued, "queued", 0)
		log.Printf("worker %s requeued request %s after %s: %v", r.config.WorkerID, request.ID, delay, err)
		return true, nil
	}

	if markErr := r.queue.MarkRecapRequestFailed(
		operationCtx,
		request.ID.String(),
		r.config.WorkerID,
		code,
		message,
		retryable,
	); markErr != nil {
		log.Printf("worker %s failed to mark request %s failed: %v", r.config.WorkerID, request.ID, markErr)
		if errors.Is(markErr, repository.ErrRecapRequestLeaseLost) {
			return false, nil
		}
		return false, markErr
	}
	r.publishLifecycle(operationCtx, request, model.RecapRequestFailed, "failed", request.ProgressPercent)
	if retryable && request.AttemptCount >= request.MaxAttempts {
		r.publishDLQ(operationCtx, request, code, message)
	}
	log.Printf("worker %s marked request %s failed: %v", r.config.WorkerID, request.ID, err)
	return true, nil
}

func (r *Runner) publishLifecycle(
	ctx context.Context,
	request *model.RecapRequest,
	status model.RecapRequestStatus,
	stage string,
	progress int,
) {
	if r.publisher == nil {
		return
	}
	event := eventing.NewLifecycle(request.ID, status, stage, progress, request.AttemptCount)
	if err := r.publisher.PublishLifecycle(ctx, event); err != nil {
		log.Printf("worker %s failed to publish lifecycle event for %s: %v", r.config.WorkerID, request.ID, err)
	}
}

func (r *Runner) publishDLQ(ctx context.Context, request *model.RecapRequest, code, message string) {
	if r.publisher == nil {
		return
	}
	event := eventing.FailedCommandV1{
		EventID:      uuid.New(),
		CommandID:    request.ID,
		RecapID:      request.ID,
		ErrorCode:    code,
		ErrorMessage: message,
		AttemptCount: request.AttemptCount,
		FailedAt:     time.Now().UTC(),
	}
	if err := r.publisher.PublishFailedCommand(ctx, event); err != nil {
		log.Printf("worker %s failed to publish DLQ event for %s: %v", r.config.WorkerID, request.ID, err)
	}
}

func (r *Runner) maintainLease(
	ctx context.Context,
	cancel context.CancelFunc,
	requestID string,
	stopped <-chan struct{},
) error {
	ticker := time.NewTicker(r.config.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-stopped:
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := r.queue.ExtendRecapRequestLease(
				ctx,
				requestID,
				r.config.WorkerID,
				r.config.LeaseDuration,
			)
			if err != nil {
				cancel()
				return err
			}
		}
	}
}

func (r *Runner) retryDelay(attempt int) time.Duration {
	delay := r.config.RetryBase
	for i := 1; i < attempt; i++ {
		if delay >= r.config.RetryMax/2 {
			return r.config.RetryMax
		}
		delay *= 2
	}
	if delay > r.config.RetryMax {
		return r.config.RetryMax
	}
	return delay
}

func classifyError(err error) (string, bool) {
	switch {
	case errors.Is(err, recap.ErrInsufficientActivity):
		return "insufficient_activity", false
	case errors.Is(err, repository.ErrProfileNotFound):
		return "profile_not_found", false
	case errors.Is(err, service.ErrYearNotAvailable):
		return "invalid_argument", false
	case errors.Is(err, service.ErrActivitySourceMissing):
		return "dependency_misconfigured", false
	case errors.Is(err, service.ErrActivitySourceUnavailable):
		return "dependency_unavailable", true
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "generation_interrupted", true
	default:
		return "internal_error", true
	}
}

func normalizeConfig(value Config) Config {
	if value.WorkerID == "" {
		value.WorkerID = "worker-unknown"
	}
	if value.PollInterval <= 0 {
		value.PollInterval = 500 * time.Millisecond
	}
	if value.LeaseDuration <= 0 {
		value.LeaseDuration = 30 * time.Second
	}
	if value.Heartbeat <= 0 || value.Heartbeat >= value.LeaseDuration {
		value.Heartbeat = value.LeaseDuration / 3
	}
	if value.RetryBase <= 0 {
		value.RetryBase = 2 * time.Second
	}
	if value.RetryMax < value.RetryBase {
		value.RetryMax = 30 * time.Second
	}
	if value.ShutdownTimeout <= 0 {
		value.ShutdownTimeout = 10 * time.Second
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

func (r *Runner) String() string {
	return fmt.Sprintf("recap worker %s", r.config.WorkerID)
}
