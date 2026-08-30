package eventing

import (
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/model"
)

const (
	GenerationCommandsTopic = "recap.generation-commands.v1"
	LifecycleTopic          = "recap.lifecycle.v1"
	GenerationDLQTopic      = "recap.generation-commands.dlq.v1"

	GenerateRecapCommandType = "GenerateRecapCommandV1"
	RecapLifecycleType       = "RecapLifecycleV1"
	FailedCommandType        = "FailedCommandV1"
)

type GenerateRecapCommandV1 struct {
	CommandID        uuid.UUID `json:"command_id"`
	RecapID          uuid.UUID `json:"recap_id"`
	ProfileID        uuid.UUID `json:"profile_id"`
	Year             int       `json:"year"`
	AlgorithmVersion string    `json:"algorithm_version"`
	RequestedAt      time.Time `json:"requested_at"`
	ForceRegenerate  bool      `json:"force_regenerate"`
}

type RecapLifecycleV1 struct {
	EventID         uuid.UUID                `json:"event_id"`
	RecapID         uuid.UUID                `json:"recap_id"`
	Status          model.RecapRequestStatus `json:"status"`
	Stage           string                   `json:"stage"`
	ProgressPercent int                      `json:"progress_percent"`
	Attempt         int                      `json:"attempt"`
	OccurredAt      time.Time                `json:"occurred_at"`
}

type FailedCommandV1 struct {
	EventID      uuid.UUID `json:"event_id"`
	CommandID    uuid.UUID `json:"command_id"`
	RecapID      uuid.UUID `json:"recap_id"`
	ErrorCode    string    `json:"error_code"`
	ErrorMessage string    `json:"error_message"`
	AttemptCount int       `json:"attempt_count"`
	FailedAt     time.Time `json:"failed_at"`
}

func NewLifecycle(
	recapID uuid.UUID,
	status model.RecapRequestStatus,
	stage string,
	progress, attempt int,
) RecapLifecycleV1 {
	return RecapLifecycleV1{
		EventID:         uuid.New(),
		RecapID:         recapID,
		Status:          status,
		Stage:           stage,
		ProgressPercent: progress,
		Attempt:         attempt,
		OccurredAt:      time.Now().UTC(),
	}
}
