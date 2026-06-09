mod auth;
mod config;
mod db;
mod entities;
mod handlers;
mod response;
mod srs_client;

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderValue, Method, Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post, put},
    Router,
};
use clap::Parser;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use std::sync::Arc;
use tower_http::{cors::CorsLayer, services::ServeDir, trace::TraceLayer};
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::auth::JwtSecret;
use crate::entities::user;
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
        Ok(token_user) => {
            let db_user = user::Entity::find()
                .filter(user::Column::Id.eq(token_user.user_id))
                .one(&state.db)
                .await
                .map_err(|_| StatusCode::UNAUTHORIZED)?;

            let Some(db_user) = db_user else {
                return Err(StatusCode::UNAUTHORIZED);
            };

            if !db_user.enabled {
                return Err(StatusCode::UNAUTHORIZED);
            }

            parts.extensions.insert(crate::auth::CurrentUser {
                username: db_user.username,
                user_id: db_user.id,
                role: db_user.role,
            });
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
            url::form_urlencoded::parse(q.as_bytes())
                .any(|(key, value)| key == "callback_secret" && value.as_ref() == secret.as_str())
        })
        .unwrap_or(false);

    let heartbeat_path_match = req
        .uri()
        .path()
        .strip_prefix("/api/internal/srs/heartbeat/")
        .map(|value| value.trim_end_matches('/') == secret)
        .unwrap_or(false);

    if header_match || query_match || heartbeat_path_match {
        Ok(next.run(req).await)
    } else {
        tracing::warn!(
            path = %req.uri().path(),
            has_query = req.uri().query().is_some(),
            has_secret_header = req.headers().contains_key("x-srs-callback-secret"),
            "validate_callback_secret: secret mismatch or missing, returning 403"
        );
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
            "/api/internal/srs/on_forward",
            post(handlers::srs_callback::on_forward),
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
        .route(
            "/api/internal/srs/heartbeat/:callback_secret",
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
        .route("/api/admin/me", get(handlers::admin::me))
        .route(
            "/api/admin/users",
            get(handlers::admin::list_users).post(handlers::admin::create_user),
        )
        .route(
            "/api/admin/users/:id",
            put(handlers::admin::update_user).delete(handlers::admin::delete_user),
        )
        .route(
            "/api/admin/rooms",
            get(handlers::admin::list_rooms).post(handlers::admin::create_room),
        )
        .route(
            "/api/admin/rooms/:id",
            put(handlers::admin::update_room).delete(handlers::admin::delete_room),
        )
        .route(
            "/api/admin/rooms/:id/stream-code/reset",
            post(handlers::admin::reset_room_stream_code),
        )
        .route(
            "/api/admin/streams/:stream_id/stop",
            post(handlers::admin::stop_stream),
        )
        .route("/api/live/my/rooms", get(handlers::live::my_live_rooms))
        .route("/api/live/stream/code", get(handlers::live::stream_code))
        .route(
            "/api/live/stream/code/reset",
            post(handlers::live::reset_stream_code),
        )
        .route(
            "/api/live/rooms/:id/title",
            put(handlers::live::update_room_title_by_id),
        )
        .route(
            "/api/live/rooms/:id/stream-code/reset",
            post(handlers::live::reset_stream_code_by_id),
        )
        .route(
            "/api/publish/protocols",
            get(handlers::playback::publish_protocols),
        )
        .route(
            "/api/live/room/title",
            put(handlers::live::update_room_title),
        )
        .route(
            "/api/live/room/cover",
            put(handlers::live::update_room_cover).route_layer(DefaultBodyLimit::max(
                handlers::live::MAX_COVER_REQUEST_BYTES,
            )),
        )
        .route(
            "/api/live/rooms/:id/cover",
            put(handlers::live::update_room_cover_by_id).route_layer(DefaultBodyLimit::max(
                handlers::live::MAX_COVER_REQUEST_BYTES,
            )),
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
            put(handlers::forward::update).delete(handlers::forward::delete),
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

    let uploads_dir = state.config.storage.upload_dir.clone();
    let app = Router::new()
        .merge(public_routes)
        .merge(srs_callback_routes)
        .merge(protected_routes)
        .nest_service("/uploads", ServeDir::new(uploads_dir))
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
