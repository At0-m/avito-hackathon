package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type OutboxEventStatus string

const (
	OutboxEventPending    OutboxEventStatus = "pending"
	OutboxEventPublishing OutboxEventStatus = "publishing"
	OutboxEventPublished  OutboxEventStatus = "published"
)

type OutboxEvent struct {
	ID             uuid.UUID
	AggregateID    uuid.UUID
	Topic          string
	EventKey       string
	EventType      string
	Payload        json.RawMessage
	Status         OutboxEventStatus
	AttemptCount   int
	AvailableAt    time.Time
	LockedBy       *string
	LockedAt       *time.Time
	LeaseExpiresAt *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	PublishedAt    *time.Time
}
