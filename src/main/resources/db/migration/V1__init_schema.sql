-- Consumer-side Inbox Table for event deduplication and out-of-order protection
CREATE TABLE IF NOT EXISTS processed_events (
    event_id          UUID PRIMARY KEY,
    aggregate_type    VARCHAR(255) NOT NULL,
    aggregate_id      VARCHAR(255) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_type        VARCHAR(255) NOT NULL,
    status            VARCHAR(32) NOT NULL DEFAULT 'SUCCESS',
    retry_count       INTEGER NOT NULL DEFAULT 0,
    error_message     TEXT,
    kafka_topic       VARCHAR(255),
    kafka_partition   INTEGER,
    kafka_offset      BIGINT,
    trace_id          VARCHAR(128),
    processed_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_processed_events_status CHECK (status IN ('SUCCESS', 'FAILED', 'RETRYING', 'SKIPPED'))
);

-- Strict unique constraint to prevent duplicate processing of the same aggregate version
CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_events_aggregate_unique
    ON processed_events(aggregate_type, aggregate_id, aggregate_version);

-- Supporting periodic retention/purging jobs
CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events(processed_at);

-- Documentation comments
COMMENT ON TABLE processed_events IS 'Stores consumer-side processed event IDs and version markers from Kafka for idempotent consumption and version validation.';
COMMENT ON COLUMN processed_events.aggregate_version IS 'Version of the aggregate used to drop or quarantine out-of-order stale events.';

-- Trigger function for auto-updating timestamps on status changes or retries
CREATE OR REPLACE FUNCTION update_processed_events_modtime()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Safely drop and recreate the trigger
DROP TRIGGER IF EXISTS update_processed_events_modtime_trigger ON processed_events;
CREATE TRIGGER update_processed_events_modtime_trigger
    BEFORE UPDATE ON processed_events
    FOR EACH ROW
    EXECUTE FUNCTION update_processed_events_modtime();