CREATE TABLE IF NOT EXISTS telemetry_reading (
    message_id UUID PRIMARY KEY,
    device_id TEXT NOT NULL,
    machine_id INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    firmware TEXT,
    volt DOUBLE PRECISION NOT NULL,
    rotate DOUBLE PRECISION NOT NULL,
    pressure DOUBLE PRECISION NOT NULL,
    vibration DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_reading_device_time ON telemetry_reading(device_id, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_reading_ingested ON telemetry_reading(ingested_at DESC);

CREATE TABLE IF NOT EXISTS device_status (
    device_id TEXT PRIMARY KEY,
    machine_id INTEGER NOT NULL,
    last_event_time TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    firmware TEXT,
    volt DOUBLE PRECISION,
    rotate DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    health TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sensor_alert (
    id BIGSERIAL PRIMARY KEY,
    message_id UUID NOT NULL,
    device_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    alert_type TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id, alert_type)
);
CREATE INDEX IF NOT EXISTS ix_alert_created ON sensor_alert(created_at DESC);
