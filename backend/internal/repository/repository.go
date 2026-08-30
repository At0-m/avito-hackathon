package repository

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"

	"recap-personalization/internal/model"
	"recap-personalization/pkg/database"
)

type ProfileRepository interface {
	GetProfiles(ctx context.Context) ([]model.ProfileSummary, error)
	GetProfileByID(ctx context.Context, id string) (*model.Profile, error)
}

type RecapRepository interface {
	GetRecapByProfileAndYear(ctx context.Context, profileID string, year int) (*model.Recap, error)
	GetRecapByID(ctx context.Context, id string) (*model.Recap, error)
	CreateRecap(ctx context.Context, value *model.Recap) error
	FinalizeRecapRequest(ctx context.Context, requestID, workerID string, value *model.Recap) error
}

type RecapRequestRepository interface {
	CreateOrGetRecapRequest(ctx context.Context, value *model.RecapRequest) (*model.RecapRequest, bool, error)
	GetRecapRequestByID(ctx context.Context, id string) (*model.RecapRequest, error)
	RestartFailedRecapRequest(ctx context.Context, id string) (*model.RecapRequest, error)
	FailExpiredExhaustedRecapRequests(ctx context.Context) (int64, error)
	ClaimNextRecapRequest(ctx context.Context, workerID string, leaseDuration time.Duration) (*model.RecapRequest, error)
	ClaimRecapRequestByID(ctx context.Context, id, workerID string, leaseDuration time.Duration) (*model.RecapRequest, error)
	ExtendRecapRequestLease(ctx context.Context, id, workerID string, leaseDuration time.Duration) error
	UpdateRecapRequestProgress(ctx context.Context, id, workerID, stage string, progress int) error
	RequeueRecapRequest(ctx context.Context, id, workerID string, delay time.Duration, code, message string) error
	MarkRecapRequestFailed(ctx context.Context, id, workerID, code, message string, retryable bool) error
}

type OutboxRepository interface {
	ClaimOutboxEvents(ctx context.Context, publisherID string, limit int, leaseDuration time.Duration) ([]model.OutboxEvent, error)
	MarkOutboxEventPublished(ctx context.Context, id uuid.UUID, publisherID string) error
	RequeueOutboxEvent(ctx context.Context, id uuid.UUID, publisherID string, delay time.Duration, message string) error
	HasConsumedEvent(ctx context.Context, consumerName string, eventID uuid.UUID) (bool, error)
	RecordConsumedEvent(ctx context.Context, consumerName string, eventID uuid.UUID, topic string, partition int, offset int64) (bool, error)
}

type Repository struct {
	DB *database.PostgresDB
}

func NewRepository(db *database.PostgresDB) *Repository {
	return &Repository{DB: db}
}

var (
	_ ProfileRepository      = (*Repository)(nil)
	_ RecapRepository        = (*Repository)(nil)
	_ RecapRequestRepository = (*Repository)(nil)
	_ OutboxRepository       = (*Repository)(nil)
)

func qualifiedColumns(alias, columns string) string {
	parts := strings.Split(columns, ",")
	qualified := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name != "" {
			qualified = append(qualified, alias+"."+name)
		}
	}
	return strings.Join(qualified, ", ")
}
