CREATE TYPE outbox_event_status AS ENUM (
    'pending',
    'publishing',
    'published'
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    topic VARCHAR(200) NOT NULL,
    event_key VARCHAR(255) NOT NULL,
    event_type VARCHAR(120) NOT NULL,
    payload JSONB NOT NULL,

    status outbox_event_status NOT NULL DEFAULT 'pending',
    attempt_count SMALLINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    locked_by VARCHAR(128),
    locked_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_error VARCHAR(500),

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,

    CHECK (
        (status = 'pending' AND locked_by IS NULL AND locked_at IS NULL AND lease_expires_at IS NULL)
        OR
        (status = 'publishing' AND locked_by IS NOT NULL AND locked_at IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status = 'published' AND locked_by IS NULL AND locked_at IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX idx_outbox_events_aggregate
    ON outbox_events (aggregate_id, topic, created_at);

CREATE INDEX idx_outbox_events_publish
    ON outbox_events (available_at, created_at, id)
    WHERE status IN ('pending', 'publishing');

CREATE INDEX idx_outbox_events_expired_lease
    ON outbox_events (lease_expires_at)
    WHERE status = 'publishing';

CREATE TABLE consumed_events (
    consumer_name VARCHAR(128) NOT NULL,
    event_id UUID NOT NULL,
    topic VARCHAR(200) NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_value BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX idx_consumed_events_processed_at
    ON consumed_events (processed_at);
