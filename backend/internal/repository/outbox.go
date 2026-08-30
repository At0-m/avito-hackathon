package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/model"
)

var ErrOutboxLeaseLost = errors.New("outbox_event_lease_lost")

const outboxColumns = `
	id,
	aggregate_id,
	topic,
	event_key,
	event_type,
	payload,
	status,
	attempt_count,
	available_at,
	locked_by,
	locked_at,
	lease_expires_at,
	last_error,
	created_at,
	updated_at,
	published_at
`

func insertOutboxEventTx(ctx context.Context, tx *sql.Tx, value *model.OutboxEvent) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (
			id,
			aggregate_id,
			topic,
			event_key,
			event_type,
			payload,
			status,
			available_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)
	`,
		value.ID,
		value.AggregateID,
		value.Topic,
		value.EventKey,
		value.EventType,
		value.Payload,
		value.AvailableAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func newOutboxEvent(aggregateID uuid.UUID, topic, key, eventType string, payload interface{}) (*model.OutboxEvent, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal outbox payload: %w", err)
	}
	return &model.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: aggregateID,
		Topic:       topic,
		EventKey:    key,
		EventType:   eventType,
		Payload:     encoded,
		Status:      model.OutboxEventPending,
		AvailableAt: time.Now().UTC(),
	}, nil
}

func (r *Repository) ClaimOutboxEvents(
	ctx context.Context,
	publisherID string,
	limit int,
	leaseDuration time.Duration,
) ([]model.OutboxEvent, error) {
	if publisherID == "" {
		return nil, errors.New("publisher id is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if leaseDuration <= 0 {
		return nil, errors.New("outbox lease duration must be positive")
	}
	rows, err := r.DB.DB.QueryContext(ctx, `
		WITH candidates AS (
			SELECT id
			FROM outbox_events
			WHERE (
				(status = 'pending' AND available_at <= CURRENT_TIMESTAMP)
				OR
				(status = 'publishing' AND lease_expires_at <= CURRENT_TIMESTAMP)
			)
			ORDER BY available_at, created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox_events AS event
		SET
			status = 'publishing',
			attempt_count = event.attempt_count + 1,
			locked_by = $2,
			locked_at = CURRENT_TIMESTAMP,
			lease_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
			updated_at = CURRENT_TIMESTAMP
		FROM candidates
		WHERE event.id = candidates.id
		RETURNING `+qualifiedColumns("event", outboxColumns),
		limit,
		publisherID,
		leaseDuration.Milliseconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()

	result := make([]model.OutboxEvent, 0, limit)
	for rows.Next() {
		value, scanErr := scanOutboxEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, *value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return result, nil
}

func (r *Repository) MarkOutboxEventPublished(ctx context.Context, id uuid.UUID, publisherID string) error {
	return requireOwnedOutboxUpdate(ctx, r.DB.DB, `
		UPDATE outbox_events
		SET
			status = 'published',
			locked_by = NULL,
			locked_at = NULL,
			lease_expires_at = NULL,
			last_error = NULL,
			published_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'publishing' AND locked_by = $2
	`, id, publisherID)
}

func (r *Repository) RequeueOutboxEvent(
	ctx context.Context,
	id uuid.UUID,
	publisherID string,
	delay time.Duration,
	message string,
) error {
	if delay < 0 {
		return errors.New("outbox retry delay must not be negative")
	}
	return requireOwnedOutboxUpdate(ctx, r.DB.DB, `
		UPDATE outbox_events
		SET
			status = 'pending',
			available_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
			locked_by = NULL,
			locked_at = NULL,
			lease_expires_at = NULL,
			last_error = $4,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'publishing' AND locked_by = $2
	`, id, publisherID, delay.Milliseconds(), truncateErrorMessage(message))
}

func (r *Repository) HasConsumedEvent(
	ctx context.Context,
	consumerName string,
	eventID uuid.UUID,
) (bool, error) {
	var exists bool
	err := r.DB.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM consumed_events
			WHERE consumer_name = $1 AND event_id = $2
		)
	`, consumerName, eventID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check consumed event: %w", err)
	}
	return exists, nil
}

func (r *Repository) RecordConsumedEvent(
	ctx context.Context,
	consumerName string,
	eventID uuid.UUID,
	topic string,
	partition int,
	offset int64,
) (bool, error) {
	result, err := r.DB.DB.ExecContext(ctx, `
		INSERT INTO consumed_events (
			consumer_name,
			event_id,
			topic,
			partition_id,
			offset_value
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (consumer_name, event_id) DO NOTHING
	`, consumerName, eventID, topic, partition, offset)
	if err != nil {
		return false, fmt.Errorf("record consumed event: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read consumed event result: %w", err)
	}
	return affected == 1, nil
}

func requireOwnedOutboxUpdate(
	ctx context.Context,
	execer interface {
		ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	},
	query string,
	args ...interface{},
) error {
	result, err := execer.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrOutboxLeaseLost
	}
	return nil
}

func scanOutboxEvent(scanner rowScanner) (*model.OutboxEvent, error) {
	var value model.OutboxEvent
	var lockedBy sql.NullString
	var lockedAt sql.NullTime
	var leaseExpiresAt sql.NullTime
	var lastError sql.NullString
	var publishedAt sql.NullTime
	if err := scanner.Scan(
		&value.ID,
		&value.AggregateID,
		&value.Topic,
		&value.EventKey,
		&value.EventType,
		&value.Payload,
		&value.Status,
		&value.AttemptCount,
		&value.AvailableAt,
		&lockedBy,
		&lockedAt,
		&leaseExpiresAt,
		&lastError,
		&value.CreatedAt,
		&value.UpdatedAt,
		&publishedAt,
	); err != nil {
		return nil, fmt.Errorf("scan outbox event: %w", err)
	}
	value.LockedBy = nullableString(lockedBy)
	value.LockedAt = nullableTime(lockedAt)
	value.LeaseExpiresAt = nullableTime(leaseExpiresAt)
	value.LastError = nullableString(lastError)
	value.PublishedAt = nullableTime(publishedAt)
	return &value, nil
}
