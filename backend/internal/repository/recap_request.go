package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"recap-personalization/internal/eventing"
	"recap-personalization/internal/model"
)

var (
	ErrRecapRequestNotFound  = errors.New("recap_request_not_found")
	ErrRecapRequestLeaseLost = errors.New("recap_request_lease_lost")
)

type rowScanner interface {
	Scan(dest ...interface{}) error
}

const recapRequestColumns = `
	id,
	profile_id,
	year,
	status,
	stage,
	progress_percent,
	algorithm_version,
	idempotency_key,
	priority,
	attempt_count,
	max_attempts,
	available_at,
	worker_id,
	locked_at,
	lease_expires_at,
	recap_id,
	error_code,
	error_message,
	retryable,
	created_at,
	updated_at,
	started_at,
	finished_at
`

func (r *Repository) CreateOrGetRecapRequest(
	ctx context.Context,
	value *model.RecapRequest,
) (*model.RecapRequest, bool, error) {
	var stored *model.RecapRequest
	created := false
	err := r.DB.WithinTransaction(ctx, nil, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
			INSERT INTO recap_requests (
				id,
				profile_id,
				year,
				status,
				stage,
				progress_percent,
				algorithm_version,
				idempotency_key,
				priority,
				max_attempts,
				available_at
			) VALUES ($1, $2, $3, 'queued', 'queued', 0, $4, $5, $6, $7, $8)
			ON CONFLICT (profile_id, year, algorithm_version) DO NOTHING
			RETURNING `+recapRequestColumns,
			value.ID,
			value.ProfileID,
			value.Year,
			value.AlgorithmVersion,
			value.IdempotencyKey,
			value.Priority,
			value.MaxAttempts,
			value.AvailableAt,
		)

		inserted, err := scanRecapRequest(row)
		if err == nil {
			if err := enqueueGenerationCommandTx(ctx, tx, inserted, inserted.CreatedAt, inserted.CreatedAt); err != nil {
				return err
			}
			stored = inserted
			created = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("create recap request: %w", err)
		}

		existing, err := scanRecapRequest(tx.QueryRowContext(ctx, `
			SELECT `+recapRequestColumns+`
			FROM recap_requests
			WHERE profile_id = $1 AND year = $2 AND algorithm_version = $3
		`, value.ProfileID, value.Year, value.AlgorithmVersion))
		if err != nil {
			return fmt.Errorf("get existing recap request: %w", err)
		}
		stored = existing
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return stored, created, nil
}

func (r *Repository) GetRecapRequestByID(ctx context.Context, id string) (*model.RecapRequest, error) {
	return r.getRecapRequest(ctx, `
		SELECT `+recapRequestColumns+`
		FROM recap_requests
		WHERE id = $1
	`, id)
}

func (r *Repository) RestartFailedRecapRequest(ctx context.Context, id string) (*model.RecapRequest, error) {
	var restarted *model.RecapRequest
	err := r.DB.WithinTransaction(ctx, nil, func(tx *sql.Tx) error {
		value, err := scanRecapRequest(tx.QueryRowContext(ctx, `
			UPDATE recap_requests
			SET
				status = 'queued',
				stage = 'queued',
				progress_percent = 0,
				attempt_count = 0,
				available_at = CURRENT_TIMESTAMP,
				recap_id = NULL,
				error_code = NULL,
				error_message = NULL,
				retryable = FALSE,
				started_at = NULL,
				finished_at = NULL,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = $1 AND status = 'failed' AND retryable = TRUE
			RETURNING `+recapRequestColumns,
			id,
		))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrRecapRequestNotFound
		}
		if err != nil {
			return fmt.Errorf("restart failed recap request: %w", err)
		}
		if err := enqueueGenerationCommandTx(ctx, tx, value, value.UpdatedAt, value.AvailableAt); err != nil {
			return err
		}
		restarted = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	return restarted, nil
}

func enqueueGenerationCommandTx(
	ctx context.Context,
	tx *sql.Tx,
	request *model.RecapRequest,
	requestedAt time.Time,
	availableAt time.Time,
) error {
	command := eventing.GenerateRecapCommandV1{
		CommandID:        uuid.New(),
		RecapID:          request.ID,
		ProfileID:        request.ProfileID,
		Year:             request.Year,
		AlgorithmVersion: request.AlgorithmVersion,
		RequestedAt:      requestedAt.UTC(),
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal generation command: %w", err)
	}
	return insertOutboxEventTx(ctx, tx, &model.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: request.ID,
		Topic:       eventing.GenerationCommandsTopic,
		EventKey:    request.ID.String(),
		EventType:   eventing.GenerateRecapCommandType,
		Payload:     payload,
		AvailableAt: availableAt.UTC(),
	})
}

func (r *Repository) FailExpiredExhaustedRecapRequests(ctx context.Context) (int64, error) {
	result, err := r.DB.DB.ExecContext(ctx, `
		UPDATE recap_requests
		SET
			status = 'failed',
			stage = 'failed',
			worker_id = NULL,
			locked_at = NULL,
			lease_expires_at = NULL,
			error_code = 'worker_lease_expired',
			error_message = 'Worker lease expired after the maximum number of attempts',
			retryable = TRUE,
			finished_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE status = 'processing'
			AND lease_expires_at <= CURRENT_TIMESTAMP
			AND attempt_count >= max_attempts
	`)
	if err != nil {
		return 0, fmt.Errorf("fail expired exhausted recap requests: %w", err)
	}
	return result.RowsAffected()
}

func (r *Repository) getRecapRequest(
	ctx context.Context,
	query string,
	args ...interface{},
) (*model.RecapRequest, error) {
	value, err := scanRecapRequest(r.DB.DB.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRecapRequestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get recap request: %w", err)
	}
	return value, nil
}

func (r *Repository) ClaimNextRecapRequest(
	ctx context.Context,
	workerID string,
	leaseDuration time.Duration,
) (*model.RecapRequest, error) {
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}
	value, err := scanRecapRequest(r.DB.DB.QueryRowContext(ctx, `
		WITH candidate AS (
			SELECT id
			FROM recap_requests
			WHERE (
				(status = 'queued' AND available_at <= CURRENT_TIMESTAMP)
				OR
				(status = 'processing' AND lease_expires_at <= CURRENT_TIMESTAMP)
			)
			AND attempt_count < max_attempts
			ORDER BY priority DESC, available_at, created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE recap_requests AS request
		SET
			status = 'processing',
			stage = 'starting',
			progress_percent = 5,
			attempt_count = request.attempt_count + 1,
			worker_id = $1,
			locked_at = CURRENT_TIMESTAMP,
			lease_expires_at = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 millisecond'),
			started_at = COALESCE(request.started_at, CURRENT_TIMESTAMP),
			updated_at = CURRENT_TIMESTAMP,
			error_code = NULL,
			error_message = NULL,
			retryable = FALSE
		FROM candidate
		WHERE request.id = candidate.id
		RETURNING `+qualifiedColumns("request", recapRequestColumns),
		workerID,
		leaseDuration.Milliseconds(),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRecapRequestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("claim recap request: %w", err)
	}
	return value, nil
}

func (r *Repository) ClaimRecapRequestByID(
	ctx context.Context,
	id, workerID string,
	leaseDuration time.Duration,
) (*model.RecapRequest, error) {
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}
	value, err := scanRecapRequest(r.DB.DB.QueryRowContext(ctx, `
		UPDATE recap_requests
		SET
			status = 'processing',
			stage = 'starting',
			progress_percent = GREATEST(progress_percent, 5),
			attempt_count = attempt_count + 1,
			worker_id = $2,
			locked_at = CURRENT_TIMESTAMP,
			lease_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
			started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
			updated_at = CURRENT_TIMESTAMP,
			error_code = NULL,
			error_message = NULL,
			retryable = FALSE
		WHERE id = $1
			AND (
				(status = 'queued' AND available_at <= CURRENT_TIMESTAMP)
				OR
				(status = 'processing' AND lease_expires_at <= CURRENT_TIMESTAMP)
			)
			AND attempt_count < max_attempts
		RETURNING `+recapRequestColumns,
		id,
		workerID,
		leaseDuration.Milliseconds(),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRecapRequestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("claim recap request by id: %w", err)
	}
	return value, nil
}

func (r *Repository) ExtendRecapRequestLease(
	ctx context.Context,
	id, workerID string,
	leaseDuration time.Duration,
) error {
	if leaseDuration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	return requireOwnedRequestUpdate(ctx, r.DB.DB, `
		UPDATE recap_requests
		SET
			lease_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'processing' AND worker_id = $2
	`, id, workerID, leaseDuration.Milliseconds())
}

func (r *Repository) UpdateRecapRequestProgress(
	ctx context.Context,
	id, workerID, stage string,
	progress int,
) error {
	if progress < 0 || progress > 100 {
		return fmt.Errorf("progress must be between 0 and 100")
	}
	return requireOwnedRequestUpdate(ctx, r.DB.DB, `
		UPDATE recap_requests
		SET
			stage = $3,
			progress_percent = GREATEST(progress_percent, $4),
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'processing' AND worker_id = $2
	`, id, workerID, stage, progress)
}

func (r *Repository) RequeueRecapRequest(
	ctx context.Context,
	id, workerID string,
	delay time.Duration,
	code, message string,
) error {
	if delay < 0 {
		return fmt.Errorf("retry delay must not be negative")
	}
	return r.DB.WithinTransaction(ctx, nil, func(tx *sql.Tx) error {
		value, err := scanRecapRequest(tx.QueryRowContext(ctx, `
			UPDATE recap_requests
			SET
				status = 'queued',
				stage = 'queued',
				progress_percent = 0,
				available_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
				worker_id = NULL,
				locked_at = NULL,
				lease_expires_at = NULL,
				error_code = NULLIF($4, ''),
				error_message = NULLIF($5, ''),
				retryable = TRUE,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = $1
				AND status = 'processing'
				AND worker_id = $2
				AND attempt_count < max_attempts
			RETURNING `+recapRequestColumns,
			id,
			workerID,
			delay.Milliseconds(),
			code,
			truncateErrorMessage(message),
		))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrRecapRequestLeaseLost
		}
		if err != nil {
			return fmt.Errorf("requeue recap request: %w", err)
		}
		if err := enqueueGenerationCommandTx(ctx, tx, value, value.UpdatedAt, value.AvailableAt); err != nil {
			return fmt.Errorf("enqueue retry command: %w", err)
		}
		return nil
	})
}

func (r *Repository) MarkRecapRequestFailed(
	ctx context.Context,
	id, workerID, code, message string,
	retryable bool,
) error {
	return requireOwnedRequestUpdate(ctx, r.DB.DB, `
		UPDATE recap_requests
		SET
			status = 'failed',
			stage = 'failed',
			worker_id = NULL,
			locked_at = NULL,
			lease_expires_at = NULL,
			error_code = $3,
			error_message = $4,
			retryable = $5,
			finished_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'processing' AND worker_id = $2
	`, id, workerID, code, truncateErrorMessage(message), retryable)
}

func requireOwnedRequestUpdate(
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
		return ErrRecapRequestLeaseLost
	}
	return nil
}

func scanRecapRequest(scanner rowScanner) (*model.RecapRequest, error) {
	var value model.RecapRequest
	var workerID sql.NullString
	var lockedAt sql.NullTime
	var leaseExpiresAt sql.NullTime
	var recapID sql.NullString
	var errorCode sql.NullString
	var errorMessage sql.NullString
	var startedAt sql.NullTime
	var finishedAt sql.NullTime

	err := scanner.Scan(
		&value.ID,
		&value.ProfileID,
		&value.Year,
		&value.Status,
		&value.Stage,
		&value.ProgressPercent,
		&value.AlgorithmVersion,
		&value.IdempotencyKey,
		&value.Priority,
		&value.AttemptCount,
		&value.MaxAttempts,
		&value.AvailableAt,
		&workerID,
		&lockedAt,
		&leaseExpiresAt,
		&recapID,
		&errorCode,
		&errorMessage,
		&value.Retryable,
		&value.CreatedAt,
		&value.UpdatedAt,
		&startedAt,
		&finishedAt,
	)
	if err != nil {
		return nil, err
	}

	value.WorkerID = nullableString(workerID)
	value.LockedAt = nullableTime(lockedAt)
	value.LeaseExpiresAt = nullableTime(leaseExpiresAt)
	value.ErrorCode = nullableString(errorCode)
	value.ErrorMessage = nullableString(errorMessage)
	value.StartedAt = nullableTime(startedAt)
	value.FinishedAt = nullableTime(finishedAt)
	if recapID.Valid {
		parsed, err := uuid.Parse(recapID.String)
		if err != nil {
			return nil, fmt.Errorf("parse recap request recap id: %w", err)
		}
		value.RecapID = &parsed
	}
	return &value, nil
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func truncateErrorMessage(value string) string {
	const limit = 500
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
