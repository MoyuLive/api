CREATE TABLE IF NOT EXISTS srs_server (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(128) NOT NULL UNIQUE,
    ip VARCHAR(64) NOT NULL DEFAULT '',
    last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cpu_usage REAL NOT NULL DEFAULT 0,
    mem_usage REAL NOT NULL DEFAULT 0,
    uptime_seconds BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_srs_server_active ON srs_server(is_active);
