package telemetry

import (
	"context"
	"errors"
	"testing"
)

func TestLifecycleAccumulatorSeparatesAttemptTimerScope(t *testing.T) {
	ctx := WithLifecycleAttempt(context.Background())
	publish := func(phase string, err error) {
		_ = TimeLifecyclePublish(ctx, phase, func() error { return err })
	}
	publish(LifecycleStarting, nil)
	for range 5 {
		publish(LifecycleProgress, nil)
	}
	publish(LifecycleReady, errors.New("broker down"))

	allCalls, allSeconds, timerCalls, timerSeconds := LifecycleTotals(ctx)
	if allCalls != 7 || timerCalls != 5 {
		t.Fatalf("calls all=%d attempt_timer=%d, want 7 and 5", allCalls, timerCalls)
	}
	if allSeconds < timerSeconds || timerSeconds < 0 {
		t.Fatalf("seconds all=%f attempt_timer=%f", allSeconds, timerSeconds)
	}
	if LifecyclePublishDuration.Count(LifecycleReady, "error") == 0 {
		t.Fatal("failed publication must be observed with result=error")
	}
}

func TestLifecycleAccumulatorIsPerAttemptAndCarried(t *testing.T) {
	first := WithLifecycleAttempt(context.Background())
	_ = TimeLifecyclePublish(first, LifecycleStarting, func() error { return nil })
	second := WithLifecycleAttempt(first)
	if calls, _, _, _ := LifecycleTotals(second); calls != 0 {
		t.Fatalf("a new attempt must start empty, got %d calls", calls)
	}
	if EnsureLifecycleAttempt(first) != first {
		t.Fatal("EnsureLifecycleAttempt must keep an existing accumulator")
	}
	detached := CarryLifecycleAttempt(first, context.Background())
	_ = TimeLifecyclePublish(detached, LifecycleRequeued, func() error { return nil })
	if calls, _, _, _ := LifecycleTotals(first); calls != 2 {
		t.Fatalf("carried context must add to the same attempt, got %d calls", calls)
	}
	// Without an accumulator the call is still timed and nothing panics.
	_ = TimeLifecyclePublish(context.Background(), LifecycleStarting, func() error { return nil })
	ObserveLifecycleAttempt(context.Background(), AttemptReady)
}

func TestObserveLifecycleAttemptRecordsBothScopes(t *testing.T) {
	ctx := WithLifecycleAttempt(context.Background())
	_ = TimeLifecyclePublish(ctx, LifecycleProgress, func() error { return nil })
	beforeAll := AttemptLifecycleCalls.Count(LifecycleScopeAll, AttemptRequeued)
	beforeTimer := AttemptLifecycleSeconds.Count(LifecycleScopeAttemptTime, AttemptRequeued)
	ObserveLifecycleAttempt(ctx, AttemptRequeued)
	if AttemptLifecycleCalls.Count(LifecycleScopeAll, AttemptRequeued) != beforeAll+1 {
		t.Fatal("scope=all calls must be observed once per attempt")
	}
	if AttemptLifecycleSeconds.Count(LifecycleScopeAttemptTime, AttemptRequeued) != beforeTimer+1 {
		t.Fatal("scope=attempt_timer seconds must be observed once per attempt")
	}
}
