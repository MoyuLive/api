CREATE TABLE IF NOT EXISTS live_session (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(256) NOT NULL UNIQUE,
    app VARCHAR(64) NOT NULL DEFAULT 'live',
    vhost VARCHAR(128) NOT NULL DEFAULT '__defaultVhost__',
    user_id INT REFERENCES "user"(id),
    client_id VARCHAR(128) NOT NULL DEFAULT '',
    server_id VARCHAR(128) NOT NULL DEFAULT '',
    stream_url VARCHAR(512) NOT NULL DEFAULT '',
    status VARCHAR(16) NOT NULL DEFAULT 'active',
    video_codec VARCHAR(32) NOT NULL DEFAULT '',
    audio_codec VARCHAR(32) NOT NULL DEFAULT '',
    video_width INT NOT NULL DEFAULT 0,
    video_height INT NOT NULL DEFAULT 0,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_live_session_status ON live_session(status);
CREATE INDEX IF NOT EXISTS idx_live_session_user ON live_session(user_id);
