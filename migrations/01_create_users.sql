CREATE TABLE "user" (
    id SERIAL PRIMARY KEY,
    username VARCHAR(64) NOT NULL UNIQUE,
    password VARCHAR(256) NOT NULL,
    stream_code VARCHAR(64) NOT NULL DEFAULT ''
);
CREATE INDEX idx_user_username ON "user"(username);
