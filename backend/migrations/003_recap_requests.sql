CREATE TYPE recap_request_status AS ENUM (
    'queued',
    'processing',
    'ready',
    'failed'
);

CREATE TABLE recap_requests (
    id UUID PRIMARY KEY,
    profile_id UUID NOT NULL,
    year SMALLINT NOT NULL CHECK (year BETWEEN 2000 AND 2100),

    status recap_request_status NOT NULL DEFAULT 'queued',
    stage VARCHAR(64) NOT NULL DEFAULT 'queued',
    progress_percent SMALLINT NOT NULL DEFAULT 0 CHECK (progress_percent BETWEEN 0 AND 100),

    algorithm_version VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    priority SMALLINT NOT NULL DEFAULT 0,

    attempt_count SMALLINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts SMALLINT NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    worker_id VARCHAR(128),
    locked_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,

    recap_id UUID REFERENCES recaps(id),
    error_code VARCHAR(100),
    error_message VARCHAR(500),
    retryable BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,

    FOREIGN KEY (profile_id, year)
        REFERENCES profile_available_years(profile_id, year),
    UNIQUE (profile_id, year, algorithm_version),

    CHECK (
        (status = 'queued' AND worker_id IS NULL AND locked_at IS NULL AND lease_expires_at IS NULL)
        OR
        (status = 'processing' AND worker_id IS NOT NULL AND locked_at IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status IN ('ready', 'failed') AND worker_id IS NULL AND locked_at IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (status <> 'ready' OR (recap_id IS NOT NULL AND progress_percent = 100)),
    CHECK (status <> 'failed' OR error_code IS NOT NULL)
);

CREATE INDEX idx_recap_requests_claim
    ON recap_requests (priority DESC, available_at, created_at, id)
    WHERE status IN ('queued', 'processing');

CREATE INDEX idx_recap_requests_expired_lease
    ON recap_requests (lease_expires_at)
    WHERE status = 'processing';

CREATE INDEX idx_recap_requests_profile_year
    ON recap_requests (profile_id, year, algorithm_version);
