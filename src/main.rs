mod auth;
mod config;
mod db;
mod entities;
mod handlers;
mod response;
mod srs_client;

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderValue, Method, Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, post, put},
    Router,
};
use clap::Parser;
use sea_orm::DatabaseConnection;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::auth::JwtSecret;
use crate::srs_client::SrsClient;

pub struct AppState {
    pub db: DatabaseConnection,
    pub config: Arc<config::AppConfig>,
    pub srs_client: Arc<SrsClient>,
}

// JWT auth middleware - validates Bearer token and injects CurrentUser
async fn jwt_auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    use axum::extract::FromRequestParts;

    // Inject JWT secret into request extensions
    req.extensions_mut()
        .insert(JwtSecret(state.config.user.auth_secret.clone()));

    let (mut parts, body) = req.into_parts();

    match crate::auth::CurrentUser::from_request_parts(&mut parts, &state).await {
        Ok(user) => {
            parts.extensions.insert(user);
            let req = Request::from_parts(parts, body);
            Ok(next.run(req).await)
        }
        Err(_) => Err(StatusCode::UNAUTHORIZED),
    }
}

// SRS callback secret validation middleware — protects internal SRS callback routes
async fn validate_callback_secret(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let secret = &state.config.srs.callback_secret;

    // If callback_secret is not configured, allow all (backward compatibility)
    if secret.is_empty() {
        return Ok(next.run(req).await);
    }

    // Check X-SRS-Callback-Secret header first
    let header_match = req
        .headers()
        .get("x-srs-callback-secret")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == secret)
        .unwrap_or(false);

    // Check query param callback_secret
    let query_match = req
        .uri()
        .query()
        .map(|q| {
            q.split('&').any(|kv| {
                let mut parts = kv.splitn(2, '=');
                parts.next() == Some("callback_secret") && parts.next() == Some(secret.as_str())
            })
        })
        .unwrap_or(false);

    if header_match || query_match {
        Ok(next.run(req).await)
    } else {
        tracing::warn!("validate_callback_secret: secret mismatch or missing, returning 403");
        Err(StatusCode::FORBIDDEN)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = config::Cli::parse();
    info!("Loading config from: {}", cli.config);

    let cfg = config::load_config(&cli.config).expect("Failed to load configuration");
    info!("Config loaded, http_port: {}", cfg.http_port);

    let db = db::init_db(&cfg.db.dsn).await;

    let srs_client = Arc::new(SrsClient::new(
        cfg.srs.api_url.clone(),
        cfg.srs.api_user.clone(),
        cfg.srs.api_password.clone(),
    ));

    let state = Arc::new(AppState {
        db,
        config: cfg.clone(),
        srs_client,
    });

    // Public routes (no auth required)
    let public_routes = Router::new()
        .route("/api/account/create", post(handlers::account::create))
        .route("/api/account/login", post(handlers::account::login))
        .route("/api/live/rooms", get(handlers::live::public_live_rooms))
        .route("/feeds/live.xml", get(handlers::live_feed::live_rss_feed))
        .route(
            "/feeds/live/:stream_id",
            get(handlers::live_feed::live_room_rss_feed),
        )
        .route(
            "/api/playback/protocols",
            get(handlers::playback::protocols),
        );

    // SRS callback routes — protected by callback_secret middleware
    let srs_state = state.clone();
    let srs_callback_routes = Router::new()
        .route(
            "/api/internal/srs/on_publish",
            post(handlers::srs_callback::on_publish),
        )
        .route(
            "/api/internal/srs/on_unpublish",
            post(handlers::srs_callback::on_unpublish),
        )
        .route(
            "/api/internal/srs/on_play",
            post(handlers::srs_callback::on_play),
        )
        .route(
            "/api/internal/srs/on_stop",
            post(handlers::srs_callback::on_stop),
        )
        .route(
            "/api/internal/srs/heartbeat",
            post(handlers::srs_callback::heartbeat),
        )
        .route_layer(middleware::from_fn_with_state(
            srs_state,
            validate_callback_secret,
        ));

    // JWT-protected routes
    let protected_routes = Router::new()
        .route("/api/account/refresh", get(handlers::account::refresh))
        .route("/api/account/logout", post(handlers::account::logout))
        .route("/api/live/stream/code", get(handlers::live::stream_code))
        .route(
            "/api/live/stream/code/reset",
            post(handlers::live::reset_stream_code),
        )
        .route(
            "/api/live/room/title",
            put(handlers::live::update_room_title),
        )
        .route(
            "/api/live/stream/status",
            get(handlers::live::stream_status),
        )
        .route("/api/live/stream/stop", post(handlers::live::stop_stream))
        .route("/api/live/stream/list", get(handlers::live::stream_list))
        .route("/api/live/forward/rules", get(handlers::forward::list))
        .route("/api/live/forward/rules", post(handlers::forward::add))
        .route(
            "/api/live/forward/rules/:id",
            delete(handlers::forward::delete),
        )
        .route("/api/system/status", get(handlers::system::status))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            jwt_auth_middleware,
        ));

    let origins: Vec<HeaderValue> = state
        .config
        .cors_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    let cors_layer = if origins.is_empty() {
        tracing::warn!("No cors_origins configured, falling back to Any for dev convenience");
        CorsLayer::new()
            .allow_origin(tower_http::cors::Any)
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
    } else {
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
    };

    let app = Router::new()
        .merge(public_routes)
        .merge(srs_callback_routes)
        .merge(protected_routes)
        .layer(cors_layer)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.http_port);
    info!("Starting HTTP server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind address");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Server error");
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C handler");
    info!("Shutting down gracefully...");
}
