package telemetry

import (
	"context"
	"sync"
	"time"
)

// Lifecycle phases of synchronous worker lifecycle publications. "progress"
// calls run inside the existing attempt timer (stage="attempt"); the others
// run outside it: "starting" before the timer starts, "ready", "requeued" and
// "failed" after it stops.
const (
	LifecycleStarting = "starting"
	LifecycleProgress = "progress"
	LifecycleReady    = "ready"
	LifecycleRequeued = "requeued"
	LifecycleFailed   = "failed"

	// Scopes of the per-attempt aggregates.
	LifecycleScopeAll         = "all"
	LifecycleScopeAttemptTime = "attempt_timer"
)

// Finer buckets for per-attempt aggregates and the fine attempt histogram, so
// that values between 0.5 s and 1 s are distinguishable. The coarse
// stageBuckets of recap_worker_stage_duration_seconds stay unchanged for
// comparison with earlier runs.
var fineAttemptBuckets = []float64{
	0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7,
	0.8, 0.9, 1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 30, 60,
}

var lifecycleCallBuckets = []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20}

var (
	LifecyclePublishDuration = Default.HistogramVec(
		"recap_worker_lifecycle_publish_duration_seconds",
		"One synchronous lifecycle publication by a worker (HTTP produce to Redpanda), by phase and result.",
		httpBuckets, "phase", "result",
	)
	AttemptLifecycleCalls = Default.HistogramVec(
		"recap_worker_attempt_lifecycle_calls",
		"Lifecycle publications per attempt; scope=all (starting..ready) or attempt_timer (inside stage=attempt), by attempt result.",
		lifecycleCallBuckets, "scope", "result",
	)
	AttemptLifecycleSeconds = Default.HistogramVec(
		"recap_worker_attempt_lifecycle_seconds",
		"Total time of lifecycle publications per attempt; scope=all or attempt_timer, by attempt result.",
		fineAttemptBuckets, "scope", "result",
	)
	WorkerAttemptDuration = Default.HistogramVec(
		"recap_worker_attempt_duration_seconds",
		"Same boundaries as recap_worker_stage_duration_seconds{stage=\"attempt\"} with fine buckets between 0.1 s and 1 s.",
		fineAttemptBuckets, "result",
	)
)

type lifecycleKey struct{}

// LifecycleAttempt accumulates lifecycle publications of one attempt.
type LifecycleAttempt struct {
	mu      sync.Mutex
	calls   map[string]int
	seconds map[string]float64
}

// WithLifecycleAttempt returns a context carrying a fresh per-attempt
// accumulator. Call it once per claimed attempt.
func WithLifecycleAttempt(ctx context.Context) context.Context {
	return context.WithValue(ctx, lifecycleKey{}, &LifecycleAttempt{
		calls: map[string]int{}, seconds: map[string]float64{},
	})
}

// EnsureLifecycleAttempt adds an accumulator when ctx has none.
func EnsureLifecycleAttempt(ctx context.Context) context.Context {
	if lifecycleAttemptFrom(ctx) != nil {
		return ctx
	}
	return WithLifecycleAttempt(ctx)
}

// CarryLifecycleAttempt copies the accumulator of from into to (used for
// contexts detached from cancellation, such as the requeue context).
func CarryLifecycleAttempt(from, to context.Context) context.Context {
	if value := lifecycleAttemptFrom(from); value != nil {
		return context.WithValue(to, lifecycleKey{}, value)
	}
	return to
}

func lifecycleAttemptFrom(ctx context.Context) *LifecycleAttempt {
	value, _ := ctx.Value(lifecycleKey{}).(*LifecycleAttempt)
	return value
}

// TimeLifecyclePublish times one synchronous lifecycle publication and adds
// it to the attempt accumulator in ctx, if any.
func TimeLifecyclePublish(ctx context.Context, phase string, publish func() error) error {
	started := time.Now()
	err := publish()
	elapsed := time.Since(started).Seconds()
	result := "ok"
	if err != nil {
		result = "error"
	}
	LifecyclePublishDuration.Observe(elapsed, phase, result)
	if value := lifecycleAttemptFrom(ctx); value != nil {
		value.mu.Lock()
		value.calls[phase]++
		value.seconds[phase] += elapsed
		value.mu.Unlock()
	}
	return err
}

// LifecycleTotals returns calls and seconds for scope all and attempt_timer.
func LifecycleTotals(ctx context.Context) (allCalls int, allSeconds float64, timerCalls int, timerSeconds float64) {
	value := lifecycleAttemptFrom(ctx)
	if value == nil {
		return 0, 0, 0, 0
	}
	value.mu.Lock()
	defer value.mu.Unlock()
	for phase, calls := range value.calls {
		allCalls += calls
		allSeconds += value.seconds[phase]
		if phase == LifecycleProgress {
			timerCalls += calls
			timerSeconds += value.seconds[phase]
		}
	}
	return allCalls, allSeconds, timerCalls, timerSeconds
}

// ObserveLifecycleAttempt records the per-attempt aggregates once, after the
// last lifecycle publication of the attempt.
func ObserveLifecycleAttempt(ctx context.Context, result string) {
	if lifecycleAttemptFrom(ctx) == nil {
		return
	}
	allCalls, allSeconds, timerCalls, timerSeconds := LifecycleTotals(ctx)
	AttemptLifecycleCalls.Observe(float64(allCalls), LifecycleScopeAll, result)
	AttemptLifecycleCalls.Observe(float64(timerCalls), LifecycleScopeAttemptTime, result)
	AttemptLifecycleSeconds.Observe(allSeconds, LifecycleScopeAll, result)
	AttemptLifecycleSeconds.Observe(timerSeconds, LifecycleScopeAttemptTime, result)
}
