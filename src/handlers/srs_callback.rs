use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::entities::{live_session, user, forward_rule, srs_server};
use crate::AppState;

// SRS callback body types

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct PublishBody {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub app: String,
    #[serde(default)]
    pub stream: String,
    #[serde(default)]
    pub param: String,
    #[serde(default, alias = "tcUrl")]
    pub tc_url: String,
    #[serde(default)]
    pub vhost: String,
    #[serde(default, alias = "client_id")]
    pub client_id: String,
    #[serde(default)]
    pub ip: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct UnpublishBody {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub app: String,
    #[serde(default)]
    pub stream: String,
    #[serde(default)]
    pub param: String,
    #[serde(default)]
    pub vhost: String,
    #[serde(default, alias = "client_id")]
    pub client_id: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct PlayBody {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub app: String,
    #[serde(default)]
    pub stream: String,
    #[serde(default)]
    pub param: String,
    #[serde(default, alias = "pageUrl")]
    pub page_url: String,
    #[serde(default)]
    pub vhost: String,
    #[serde(default, alias = "client_id")]
    pub client_id: String,
    #[serde(default)]
    pub ip: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct StopBody {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub app: String,
    #[serde(default)]
    pub stream: String,
    #[serde(default)]
    pub param: String,
    #[serde(default)]
    pub vhost: String,
    #[serde(default, alias = "client_id")]
    pub client_id: String,
}

#[derive(Deserialize, Debug)]
pub struct HeartbeatBody {
    #[serde(default)]
    pub device_id: String,
    #[serde(default)]
    pub ip: String,
    #[serde(default)]
    pub cpu_usage: f64,
    #[serde(default)]
    pub mem_usage: f64,
    #[serde(default)]
    pub uptime_seconds: i64,
    #[serde(default)]
    pub is_active: bool,
}

#[derive(serde::Serialize)]
struct CallbackResponse {
    code: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<CallbackData>,
}

#[derive(serde::Serialize)]
struct CallbackData {
    urls: Vec<String>,
}

fn validate_callback_secret(config: &crate::config::AppConfig, headers: &axum::http::HeaderMap) -> bool {
    let secret = &config.srs.callback_secret;
    if secret.is_empty() {
        return true; // Not configured, allow all
    }
    headers
        .get("X-Callback-Secret")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == secret)
        .unwrap_or(false)
}

fn parse_token_from_param(param: &str) -> Option<String> {
    let param = param.strip_prefix('?').unwrap_or(param);
    for pair in param.split('&') {
        let mut kv = pair.splitn(2, '=');
        if let (Some("token"), Some(val)) = (kv.next(), kv.next()) {
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

// POST /api/internal/srs/on_publish
pub async fn on_publish(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<PublishBody>,
) -> impl IntoResponse {
    if !validate_callback_secret(&state.config, &headers) {
        warn!("on_publish: invalid callback secret, denying");
        return (StatusCode::OK, Json(CallbackResponse { code: 1, data: None }));
    }

    let stream_code = match parse_token_from_param(&body.param) {
        Some(code) => code,
        None => {
            warn!("on_publish: no token in param, denying");
            return (StatusCode::OK, Json(CallbackResponse { code: 1, data: None }));
        }
    };

    // Validate stream_code by finding the user
    let user_model = match user::Entity::find()
        .filter(user::Column::StreamCode.eq(&stream_code))
        .one(&state.db)
        .await
    {
        Ok(Some(u)) => u,
        Ok(None) => {
            warn!(stream_code = %stream_code, "on_publish: invalid stream_code, denying");
            return (StatusCode::OK, Json(CallbackResponse { code: 1, data: None }));
        }
        Err(e) => {
            error!("on_publish: db error: {}", e);
            return (StatusCode::OK, Json(CallbackResponse { code: 1, data: None }));
        }
    };

    let stream_url = if body.tc_url.is_empty() {
        body.stream.clone()
    } else {
        format!("{}/{}", body.tc_url, body.stream)
    };

    let now = chrono::Utc::now().naive_utc();

    // Check if session exists, upsert
    let existing = live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&body.stream))
        .one(&state.db)
        .await;

    match existing {
        Ok(Some(existing_session)) => {
            // Update existing session
            let mut active: live_session::ActiveModel = existing_session.into();
            active.app = Set(body.app.clone());
            active.vhost = Set(body.vhost.clone());
            active.user_id = Set(user_model.id);
            active.client_id = Set(body.client_id.clone());
            active.stream_url = Set(stream_url);
            active.status = Set("active".to_string());
            active.started_at = Set(now);
            if let Err(e) = active.update(&state.db).await {
                error!("on_publish: failed to update live session: {}", e);
            }
        }
        Ok(None) => {
            // Create new session
            let session = live_session::ActiveModel {
                stream_id: Set(body.stream.clone()),
                app: Set(body.app.clone()),
                vhost: Set(body.vhost.clone()),
                user_id: Set(user_model.id),
                client_id: Set(body.client_id.clone()),
                stream_url: Set(stream_url),
                status: Set("active".to_string()),
                started_at: Set(now),
                ..Default::default()
            };
            if let Err(e) = session.insert(&state.db).await {
                error!("on_publish: failed to create live session: {}", e);
            }
        }
        Err(e) => {
            error!("on_publish: db query error: {}", e);
        }
    }

    // Query matching forward rules
    let stream_key = format!("{}/{}", body.app, body.stream);
    let rules = match forward_rule::Entity::find()
        .filter(
            forward_rule::Column::Enabled.eq(true)
                .and(
                    forward_rule::Column::StreamFilter
                        .eq("*")
                        .or(forward_rule::Column::StreamFilter.eq(&stream_key)),
                )
        )
        .all(&state.db)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("on_publish: failed to query forward rules: {}", e);
            vec![]
        }
    };

    info!(
        stream = %body.stream,
        user_id = user_model.id,
        forward_rules = rules.len(),
        "on_publish: stream allowed"
    );

    if !rules.is_empty() {
        let urls: Vec<String> = rules.into_iter().map(|r| r.target_url).collect();
        return (StatusCode::OK, Json(CallbackResponse {
            code: 0,
            data: Some(CallbackData { urls }),
        }));
    }

    (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }))
}

// POST /api/internal/srs/on_unpublish
pub async fn on_unpublish(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<UnpublishBody>,
) -> impl IntoResponse {
    if !validate_callback_secret(&state.config, &headers) {
        return (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }));
    }

    info!(stream = %body.stream, "on_unpublish callback");

    let now = chrono::Utc::now().naive_utc();
    let result = live_session::Entity::update_many()
        .filter(live_session::Column::StreamId.eq(&body.stream))
        .filter(live_session::Column::Status.eq("active"))
        .col_expr(live_session::Column::Status, sea_orm::sea_query::Expr::value("ended"))
        .col_expr(live_session::Column::EndedAt, sea_orm::sea_query::Expr::value(Some(now)))
        .exec(&state.db)
        .await;

    if let Err(e) = result {
        error!("on_unpublish: failed to end live session: {}", e);
    }

    (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }))
}

// POST /api/internal/srs/on_play
pub async fn on_play(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<PlayBody>,
) -> impl IntoResponse {
    if !validate_callback_secret(&state.config, &headers) {
        return (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }));
    }

    info!(stream = %body.stream, ip = %body.ip, "on_play callback");
    (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }))
}

// POST /api/internal/srs/on_stop
pub async fn on_stop(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<StopBody>,
) -> impl IntoResponse {
    if !validate_callback_secret(&state.config, &headers) {
        return (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }));
    }

    info!(stream = %body.stream, "on_stop callback");
    (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }))
}

// POST /api/internal/srs/heartbeat
pub async fn heartbeat(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<HeartbeatBody>,
) -> impl IntoResponse {
    if !validate_callback_secret(&state.config, &headers) {
        return (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }));
    }

    let now = chrono::Utc::now().naive_utc();

    // Try to find existing server, upsert
    let existing = srs_server::Entity::find()
        .filter(srs_server::Column::DeviceId.eq(&body.device_id))
        .one(&state.db)
        .await;

    match existing {
        Ok(Some(existing_server)) => {
            let mut active: srs_server::ActiveModel = existing_server.into();
            active.ip = Set(body.ip);
            active.last_heartbeat = Set(now);
            active.is_active = Set(body.is_active);
            active.cpu_usage = Set(body.cpu_usage as f32);
            active.mem_usage = Set(body.mem_usage as f32);
            active.uptime_seconds = Set(body.uptime_seconds);
            if let Err(e) = active.update(&state.db).await {
                error!("heartbeat: failed to update srs server: {}", e);
            }
        }
        Ok(None) => {
            let server = srs_server::ActiveModel {
                device_id: Set(body.device_id),
                ip: Set(body.ip),
                last_heartbeat: Set(now),
                is_active: Set(body.is_active),
                cpu_usage: Set(body.cpu_usage as f32),
                mem_usage: Set(body.mem_usage as f32),
                uptime_seconds: Set(body.uptime_seconds),
                ..Default::default()
            };
            if let Err(e) = server.insert(&state.db).await {
                error!("heartbeat: failed to insert srs server: {}", e);
            }
        }
        Err(e) => {
            error!("heartbeat: db query error: {}", e);
        }
    }

    (StatusCode::OK, Json(CallbackResponse { code: 0, data: None }))
}
