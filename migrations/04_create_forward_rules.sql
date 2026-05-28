CREATE TABLE IF NOT EXISTS forward_rule (
    id SERIAL PRIMARY KEY,
    stream_filter VARCHAR(256) NOT NULL DEFAULT '*',
    target_url VARCHAR(512) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_forward_rule_enabled ON forward_rule(enabled);
CREATE INDEX IF NOT EXISTS idx_forward_rule_filter ON forward_rule(stream_filter);
