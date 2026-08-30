package model

import (
	"time"

	"github.com/google/uuid"
)

type RecapRequestStatus string

const (
	RecapRequestQueued     RecapRequestStatus = "queued"
	RecapRequestProcessing RecapRequestStatus = "processing"
	RecapRequestReady      RecapRequestStatus = "ready"
	RecapRequestFailed     RecapRequestStatus = "failed"
)

type RecapRequest struct {
	ID               uuid.UUID
	ProfileID        uuid.UUID
	Year             int
	Status           RecapRequestStatus
	Stage            string
	ProgressPercent  int
	AlgorithmVersion string
	IdempotencyKey   string
	Priority         int
	AttemptCount     int
	MaxAttempts      int
	AvailableAt      time.Time
	WorkerID         *string
	LockedAt         *time.Time
	LeaseExpiresAt   *time.Time
	RecapID          *uuid.UUID
	ErrorCode        *string
	ErrorMessage     *string
	Retryable        bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
}

type RecapRequestError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type RecapRequestLinks struct {
	Self   string `json:"self"`
	Stream string `json:"stream"`
}

type RecapRequestStatusResponse struct {
	ID              uuid.UUID          `json:"id"`
	ProfileID       uuid.UUID          `json:"profile_id"`
	Year            int                `json:"year"`
	Status          RecapRequestStatus `json:"status"`
	Stage           string             `json:"stage"`
	ProgressPercent int                `json:"progress_percent"`
	Attempt         int                `json:"attempt"`
	MaxAttempts     int                `json:"max_attempts"`
	PollAfterMS     int                `json:"poll_after_ms"`
	Links           RecapRequestLinks  `json:"links"`
	Error           *RecapRequestError `json:"error,omitempty"`
}

func NewRecapRequestStatusResponse(value *RecapRequest, pollAfter time.Duration) *RecapRequestStatusResponse {
	pollAfter = effectivePollAfter(value, pollAfter, time.Now().UTC())
	response := &RecapRequestStatusResponse{
		ID:              value.ID,
		ProfileID:       value.ProfileID,
		Year:            value.Year,
		Status:          value.Status,
		Stage:           value.Stage,
		ProgressPercent: value.ProgressPercent,
		Attempt:         value.AttemptCount,
		MaxAttempts:     value.MaxAttempts,
		PollAfterMS:     int(pollAfter.Milliseconds()),
		Links: RecapRequestLinks{
			Self:   "/api/v1/recaps/" + value.ID.String(),
			Stream: "/api/v1/recaps/" + value.ID.String() + "/stream",
		},
	}
	if value.Status == RecapRequestFailed && value.ErrorCode != nil {
		response.Error = &RecapRequestError{
			Code:      *value.ErrorCode,
			Message:   publicRecapRequestErrorMessage(*value.ErrorCode),
			Retryable: value.Retryable,
		}
	}
	return response
}

func publicRecapRequestErrorMessage(code string) string {
	switch code {
	case "insufficient_activity":
		return "Not enough activity data for the selected year"
	case "profile_not_found":
		return "Profile not found"
	case "invalid_argument":
		return "The recap request is invalid"
	case "dependency_misconfigured":
		return "Recap generation is not configured"
	case "dependency_unavailable", "generation_interrupted", "worker_lease_expired", "worker_shutdown":
		return "Recap generation is temporarily unavailable"
	default:
		return "Recap generation failed"
	}
}

func effectivePollAfter(value *RecapRequest, configured time.Duration, now time.Time) time.Duration {
	if configured < 100*time.Millisecond {
		configured = 100 * time.Millisecond
	}
	if value.Status != RecapRequestQueued || !value.AvailableAt.After(now) {
		return configured
	}
	wait := value.AvailableAt.Sub(now)
	if wait > configured {
		return wait
	}
	return configured
}
