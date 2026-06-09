# syntax=docker/dockerfile:1.7

FROM rust:bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo fetch --locked

COPY src src
COPY migrations migrations
COPY config.docker.toml config.docker.toml
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    rm -f target/release/yantube-api target/release/deps/yantube_api-* && \
    rm -rf target/release/.fingerprint/yantube-api-* && \
    cargo build --release --locked && \
    cp target/release/yantube-api /app/yantube-api

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/yantube-api .
COPY --from=builder /app/migrations migrations/
COPY config.docker.toml config.toml

EXPOSE 9081

CMD ["./yantube-api", "--config", "config.toml"]
