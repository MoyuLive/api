ALTER TABLE IF EXISTS "user"
ADD COLUMN IF NOT EXISTS role VARCHAR(32) NOT NULL DEFAULT 'user';

ALTER TABLE IF EXISTS "user"
ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_user_role ON "user"(role);
CREATE INDEX IF NOT EXISTS idx_user_enabled ON "user"(enabled);

UPDATE "user"
SET role = 'super_admin'
WHERE id = (
    SELECT id
    FROM "user"
    ORDER BY id ASC
    LIMIT 1
)
AND NOT EXISTS (
    SELECT 1
    FROM "user"
    WHERE role IN ('admin', 'super_admin')
);

CREATE TABLE IF NOT EXISTS live_room (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    stream_id VARCHAR(256) NOT NULL UNIQUE,
    title VARCHAR(128) NOT NULL DEFAULT '',
    stream_code VARCHAR(64) NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_room_user ON live_room(user_id);
CREATE INDEX IF NOT EXISTS idx_live_room_enabled ON live_room(enabled);

INSERT INTO live_room (user_id, stream_id, title, stream_code, enabled)
SELECT
    id,
    username,
    room_title,
    CASE
        WHEN stream_code = '' THEN substr(md5(random()::text || clock_timestamp()::text), 1, 16)
        ELSE stream_code
    END,
    enabled
FROM "user"
ON CONFLICT (stream_id) DO NOTHING;
