mod auth;
mod config;
mod danmaku;
mod db;
mod entities;
mod handlers;
mod live_hub;
mod response;
mod room_access;
mod room_privacy;
mod srs_client;

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, MatchedPath, State},
    http::{header, HeaderValue, Method, Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post, put},
    Router,
};
use clap::Parser;
use percent_encoding::percent_decode_str;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use std::sync::Arc;
use tower_http::{cors::CorsLayer, services::ServeDir, trace::TraceLayer};
use tracing::{info, info_span};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::auth::JwtSecret;
use crate::entities::user;
use crate::live_hub::LiveHub;
use crate::srs_client::SrsClient;

pub struct AppState {
    pub db: DatabaseConnection,
    pub config: Arc<config::AppConfig>,
    pub srs_client: Arc<SrsClient>,
    pub live_hub: Arc<LiveHub>,
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

fn callback_secret_value_matches(value: &str, secret: &str) -> bool {
    if value == secret {
        return true;
    }

    let mut current = value.to_string();
    for _ in 0..2 {
        let decoded = percent_decode_str(&current)
            .decode_utf8_lossy()
            .into_owned();
        if decoded == secret {
            return true;
        }
        if decoded == current {
            return false;
        }
        current = decoded;
    }

    false
}

fn path_secret_segment_matches(path: &str, prefix: &str, secret: &str) -> bool {
    path.strip_prefix(prefix)
        .and_then(|rest| rest.split('/').next())
        .filter(|segment| !segment.is_empty())
        .map(|segment| callback_secret_value_matches(segment, secret))
        .unwrap_or(false)
}

fn path_secret_segment_present(path: &str, prefix: &str) -> bool {
    path.strip_prefix(prefix)
        .and_then(|rest| rest.split('/').next())
        .filter(|segment| !segment.is_empty())
        .is_some()
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

    if request_has_callback_secret(&req, secret) {
        Ok(next.run(req).await)
    } else {
        let callback_debug = callback_secret_debug(&req);
        tracing::warn!(
            header_present = callback_debug.header_present,
            path_present = callback_debug.path_present,
            query_present = callback_debug.query_present,
            query_value_len = callback_debug.query_value_len,
            query_value_has_percent = callback_debug.query_value_has_percent,
            "validate_callback_secret: secret mismatch or missing, returning 403"
        );
        Err(StatusCode::FORBIDDEN)
    }
}

#[derive(Debug, Default)]
struct CallbackSecretDebug {
    header_present: bool,
    path_present: bool,
    query_present: bool,
    query_value_len: usize,
    query_value_has_percent: bool,
}

fn request_has_callback_secret(req: &Request<Body>, secret: &str) -> bool {
    let header_match = req
        .headers()
        .get("x-srs-callback-secret")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == secret)
        .unwrap_or(false);

    let query_match = req
        .uri()
        .query()
        .map(|query| query_has_callback_secret(query, secret))
        .unwrap_or(false);
    let path = req.uri().path();
    let heartbeat_path_match =
        path_secret_segment_matches(path, "/api/internal/srs/heartbeat/", secret);
    let legacy_callback_path_match =
        path_secret_segment_matches(path, "/api/internal/srs/callback/", secret);

    header_match || query_match || heartbeat_path_match || legacy_callback_path_match
}

fn query_has_callback_secret(query: &str, secret: &str) -> bool {
    query.split('&').any(|kv| {
        let mut parts = kv.splitn(2, '=');
        parts.next() == Some("callback_secret")
            && parts
                .next()
                .map(|value| callback_secret_value_matches(value, secret))
                .unwrap_or(false)
    })
}

fn callback_secret_debug(req: &Request<Body>) -> CallbackSecretDebug {
    let header_present = req.headers().contains_key("x-srs-callback-secret");
    let path = req.uri().path();
    let path_present = req
        .uri()
        .path()
        .strip_prefix("/api/internal/srs/callback/")
        .and_then(|rest| rest.split_once('/'))
        .is_some()
        || path_secret_segment_present(path, "/api/internal/srs/heartbeat/");
    let Some(query) = req.uri().query() else {
        return CallbackSecretDebug {
            header_present,
            path_present,
            ..Default::default()
        };
    };

    let mut debug = CallbackSecretDebug {
        header_present,
        path_present,
        ..Default::default()
    };
    for kv in query.split('&') {
        let mut parts = kv.splitn(2, '=');
        if parts.next() == Some("callback_secret") {
            if let Some(value) = parts.next() {
                debug.query_present = true;
                debug.query_value_len = value.len();
                debug.query_value_has_percent = value.contains('%');
            }
            break;
        }
    }
    debug
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
        live_hub: Arc::new(LiveHub::new()),
    });

    // Public routes (no auth required)
    let public_routes = Router::new()
        .route("/api/account/create", post(handlers::account::create))
        .route("/api/account/login", post(handlers::account::login))
        .route("/api/live/rooms", get(handlers::live::public_live_rooms))
        .route("/api/live/rooms/:stream_id", get(handlers::room::metadata))
        .route(
            "/api/live/rooms/:stream_id/ws",
            get(handlers::room::websocket),
        )
        .route(
            "/api/live/rooms/:stream_id/access",
            post(handlers::room::access),
        )
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
            "/api/internal/srs/callback/:callback_secret/on_publish",
            post(handlers::srs_callback::on_publish),
        )
        .route(
            "/api/internal/srs/on_forward",
            post(handlers::srs_callback::on_forward),
        )
        .route(
            "/api/internal/srs/callback/:callback_secret/on_forward",
            post(handlers::srs_callback::on_forward),
        )
        .route(
            "/api/internal/srs/on_unpublish",
            post(handlers::srs_callback::on_unpublish),
        )
        .route(
            "/api/internal/srs/callback/:callback_secret/on_unpublish",
            post(handlers::srs_callback::on_unpublish),
        )
        .route(
            "/api/internal/srs/on_play",
            post(handlers::srs_callback::on_play),
        )
        .route(
            "/api/internal/srs/callback/:callback_secret/on_play",
            post(handlers::srs_callback::on_play),
        )
        .route(
            "/api/internal/srs/on_stop",
            post(handlers::srs_callback::on_stop),
        )
        .route(
            "/api/internal/srs/callback/:callback_secret/on_stop",
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
        .route(
            "/api/internal/srs/callback/:callback_secret/heartbeat",
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
            "/api/live/rooms/:id/privacy",
            put(handlers::room::update_owned_privacy),
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
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<_>| {
                let matched_path = request
                    .extensions()
                    .get::<MatchedPath>()
                    .map(MatchedPath::as_str)
                    .unwrap_or("unmatched");
                info_span!("http_request", method = %request.method(), matched_path)
            }),
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_secret_segment_matches_legacy_encoded_callback_secret() {
        let secret = "fZ5VZDQjViJttQSgGwHyiqWpVo8XN0la8LUO+Ibcz/w=";
        let path = "/api/internal/srs/callback/fZ5VZDQjViJttQSgGwHyiqWpVo8XN0la8LUO%2BIbcz%2Fw%3D/on_publish";

        assert!(path_secret_segment_matches(
            path,
            "/api/internal/srs/callback/",
            secret
        ));
    }

    #[test]
    fn callback_secret_query_accepts_percent_encoded_value() {
        assert!(query_has_callback_secret(
            "callback_secret=abc%2B%2F%3D",
            "abc+/="
        ));
    }

    #[test]
    fn path_secret_segment_rejects_mismatched_secret() {
        assert!(!path_secret_segment_matches(
            "/api/internal/srs/callback/wrong/on_publish",
            "/api/internal/srs/callback/",
            "expected"
        ));
    }

    #[test]
    fn callback_secret_query_keeps_raw_value_compatibility() {
        assert!(query_has_callback_secret(
            "callback_secret=abc+/=",
            "abc+/="
        ));
    }

    #[test]
    fn callback_secret_query_rejects_wrong_value() {
        assert!(!query_has_callback_secret(
            "callback_secret=abc%2B%2F%3D",
            "wrong"
        ));
    }

    #[test]
    fn callback_secret_query_accepts_double_encoded_value() {
        assert!(query_has_callback_secret(
            "callback_secret=abc%252B%252F%253D",
            "abc+/="
        ));
    }

    #[test]
    fn callback_secret_path_accepts_percent_encoded_value() {
        assert!(path_secret_segment_matches(
            "/api/internal/srs/callback/abc%2B%2F%3D/heartbeat",
            "/api/internal/srs/callback/",
            "abc+/="
        ));
    }
}
