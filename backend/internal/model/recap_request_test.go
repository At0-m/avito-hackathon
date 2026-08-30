package model

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRecapRequestStatusResponseDoesNotExposeInternalError(t *testing.T) {
	code := "internal_error"
	internalMessage := "dial tcp 10.0.0.12:5432: connection refused"
	value := &RecapRequest{
		ID:              uuid.New(),
		ProfileID:       uuid.New(),
		Year:            2026,
		Status:          RecapRequestFailed,
		Stage:           "failed",
		MaxAttempts:     3,
		ErrorCode:       &code,
		ErrorMessage:    &internalMessage,
		Retryable:       true,
		ProgressPercent: 35,
	}

	response := NewRecapRequestStatusResponse(value, 500*time.Millisecond)
	if response.Error == nil {
		t.Fatal("expected public failure details")
	}
	if response.Error.Message == internalMessage {
		t.Fatal("internal infrastructure error must not be exposed to the frontend")
	}
	if response.Error.Message != "Recap generation failed" {
		t.Fatalf("unexpected safe error message: %q", response.Error.Message)
	}
}

func TestRecapRequestStatusResponseMapsRetryableDependencyError(t *testing.T) {
	code := "dependency_unavailable"
	value := &RecapRequest{
		ID:          uuid.New(),
		ProfileID:   uuid.New(),
		Year:        2026,
		Status:      RecapRequestFailed,
		Stage:       "failed",
		MaxAttempts: 3,
		ErrorCode:   &code,
		Retryable:   true,
	}

	response := NewRecapRequestStatusResponse(value, 500*time.Millisecond)
	if response.Error == nil || !response.Error.Retryable {
		t.Fatal("expected retryable public error")
	}
	if response.Error.Message != "Recap generation is temporarily unavailable" {
		t.Fatalf("unexpected dependency message: %q", response.Error.Message)
	}
}

func TestQueuedRetryUsesAvailableAtForPollingDelay(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	value := &RecapRequest{
		Status:      RecapRequestQueued,
		AvailableAt: now.Add(5 * time.Second),
	}

	if got := effectivePollAfter(value, 500*time.Millisecond, now); got != 5*time.Second {
		t.Fatalf("got %s, want 5s", got)
	}
}
