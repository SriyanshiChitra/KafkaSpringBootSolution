CREATE TABLE consumed_messages (
    id UUID PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    payload VARCHAR(12000) NOT NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    consumed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);