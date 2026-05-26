mod config;
mod db;
mod entities;
mod auth;
mod response;
mod srs_client;
mod handlers;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, post},
    Router,
};
use clap::Parser;
use sea_orm::DatabaseConnection;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::auth::JwtSecret;
use crate::srs_client::SrsClient;

#[derive(Clone)]
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
        );

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
            "/api/live/stream/status",
            get(handlers::live::stream_status),
        )
        .route("/api/live/stream/stop", post(handlers::live::stop_stream))
        .route("/api/live/stream/list", get(handlers::live::stream_list))
        .route(
            "/api/live/forward/rules",
            get(handlers::forward::list),
        )
        .route(
            "/api/live/forward/rules",
            post(handlers::forward::add),
        )
        .route(
            "/api/live/forward/rules/:id",
            delete(handlers::forward::delete),
        )
        .route("/api/system/status", get(handlers::system::status))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            jwt_auth_middleware,
        ));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .merge(public_routes)
        .merge(protected_routes)
        .layer(cors)
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
