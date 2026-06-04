CREATE TABLE IF NOT EXISTS live_stream_state (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(256) NOT NULL UNIQUE,
    user_id INT REFERENCES "user"(id),
    status VARCHAR(16) NOT NULL DEFAULT 'active',
    episode_started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_unpublished_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_live_stream_state_status ON live_stream_state(status);
CREATE INDEX IF NOT EXISTS idx_live_stream_state_user ON live_stream_state(user_id);
CREATE INDEX IF NOT EXISTS idx_live_stream_state_episode_started_at ON live_stream_state(episode_started_at);
