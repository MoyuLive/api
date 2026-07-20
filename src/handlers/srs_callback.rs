use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use chrono::{Duration, NaiveDateTime, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder,
    Set,
};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{error, info, warn};
use url::form_urlencoded;

use crate::{
    entities::{forward_rule, live_room, live_session, live_stream_state, srs_server, user},
    room_access::admit_room_ticket_with_account_check,
    AppState,
};

// SRS callback body types

const LIVE_RECONNECT_GRACE_SECONDS: i64 = 600;

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
pub struct ForwardBody {
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
    #[serde(default, alias = "server_id")]
    pub server_id: String,
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
    #[serde(default = "default_active")]
    pub is_active: bool,
}

fn default_active() -> bool {
    true
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

fn parse_token_from_param(param: &str) -> Option<String> {
    let param = param.strip_prefix('?').unwrap_or(param);
    for (key, value) in form_urlencoded::parse(param.as_bytes()) {
        if key == "token" && !value.is_empty() {
            return Some(value.into_owned());
        }
        if key == "streamid" {
            if let Some(token) = parse_token_from_srt_stream_id(&value) {
                return Some(token);
            }
        }
    }

    parse_token_from_srt_stream_id(param)
}

fn parse_room_ticket_from_param(param: &str) -> Option<String> {
    let param = param.strip_prefix('?').unwrap_or(param);
    let mut ticket = None;

    for (key, value) in form_urlencoded::parse(param.as_bytes()) {
        if key != "ticket" {
            continue;
        }

        // Reject duplicate or empty ticket parameters rather than selecting an ambiguous value.
        if ticket.is_some() || value.is_empty() {
            return None;
        }
        ticket = Some(value.into_owned());
    }

    ticket
}

fn parse_token_from_srt_stream_id(stream_id: &str) -> Option<String> {
    for part in stream_id.split([',', '&']) {
        let mut kv = part.splitn(2, '=');
        if let (Some("token"), Some(value)) = (kv.next().map(str::trim), kv.next()) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }

    None
}

fn forward_rule_filters(app: &str, stream: &str) -> Vec<String> {
    let mut filters = Vec::from(["*".to_string()]);

    if !stream.is_empty() {
        filters.push(stream.to_string());
    }

    if !app.is_empty() {
        filters.push(format!("{}/*", app));
        if !stream.is_empty() {
            filters.push(format!("{}/{}", app, stream));
        }
    }

    filters.sort();
    filters.dedup();
    filters
}

fn render_forward_target_url(template: &str, app: &str, stream: &str) -> String {
    template
        .replace("{app}", app)
        .replace("{stream}", stream)
        .replace("[app]", app)
        .replace("[stream]", stream)
}

fn forward_urls_from_rules(
    rules: Vec<forward_rule::Model>,
    app: &str,
    stream: &str,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();

    for rule in rules {
        let url = render_forward_target_url(&rule.target_url, app, stream);
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }

    urls
}

async fn matching_forward_rules(
    db: &DatabaseConnection,
    app: &str,
    stream: &str,
) -> Result<Vec<forward_rule::Model>, DbErr> {
    forward_rule::Entity::find()
        .filter(forward_rule::Column::Enabled.eq(true))
        .filter(forward_rule::Column::StreamFilter.is_in(forward_rule_filters(app, stream)))
        .order_by_asc(forward_rule::Column::Id)
        .all(db)
        .await
}

fn next_episode_started_at(
    previous: Option<&live_stream_state::Model>,
    now: NaiveDateTime,
) -> NaiveDateTime {
    let Some(previous) = previous else {
        return now;
    };

    if previous.status == "active" {
        return previous.episode_started_at;
    }

    let Some(last_unpublished_at) = previous.last_unpublished_at else {
        return now;
    };

    if now - last_unpublished_at <= Duration::seconds(LIVE_RECONNECT_GRACE_SECONDS) {
        previous.episode_started_at
    } else {
        now
    }
}

async fn mark_live_stream_published(
    db: &DatabaseConnection,
    stream_id: &str,
    user_id: i32,
    now: NaiveDateTime,
) -> Result<(), DbErr> {
    let previous = live_stream_state::Entity::find()
        .filter(live_stream_state::Column::StreamId.eq(stream_id))
        .one(db)
        .await?;
    let episode_started_at = next_episode_started_at(previous.as_ref(), now);

    if let Some(previous) = previous {
        let mut active: live_stream_state::ActiveModel = previous.into();
        active.user_id = Set(user_id);
        active.status = Set("active".to_string());
        active.episode_started_at = Set(episode_started_at);
        active.last_unpublished_at = Set(None);
        active.updated_at = Set(now);
        active.update(db).await?;
        return Ok(());
    }

    let state = live_stream_state::ActiveModel {
        stream_id: Set(stream_id.to_string()),
        user_id: Set(user_id),
        status: Set("active".to_string()),
        episode_started_at: Set(episode_started_at),
        last_unpublished_at: Set(None),
        updated_at: Set(now),
        ..Default::default()
    };
    state.insert(db).await?;
    Ok(())
}

async fn mark_live_stream_unpublished(
    db: &DatabaseConnection,
    stream_id: &str,
    now: NaiveDateTime,
) -> Result<(), DbErr> {
    live_stream_state::Entity::update_many()
        .filter(live_stream_state::Column::StreamId.eq(stream_id))
        .col_expr(
            live_stream_state::Column::Status,
            sea_orm::sea_query::Expr::value("ended"),
        )
        .col_expr(
            live_stream_state::Column::LastUnpublishedAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .col_expr(
            live_stream_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .exec(db)
        .await?;
    Ok(())
}

// POST /api/internal/srs/on_publish
pub async fn on_publish(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PublishBody>,
) -> impl IntoResponse {
    let stream_code = match parse_token_from_param(&body.param) {
        Some(code) => code,
        None => {
            warn!("on_publish: no token in param, denying");
            return (
                StatusCode::OK,
                Json(CallbackResponse {
                    code: 1,
                    data: None,
                }),
            );
        }
    };

    // Validate stream_code against the addressed live room.
    let room_model = match live_room::Entity::find()
        .filter(live_room::Column::StreamId.eq(&body.stream))
        .filter(live_room::Column::StreamCode.eq(&stream_code))
        .filter(live_room::Column::Enabled.eq(true))
        .one(&state.db)
        .await
    {
        Ok(Some(room)) => room,
        Ok(None) => {
            warn!(
                stream = %body.stream,
                "on_publish: invalid stream_code or disabled room, denying"
            );
            return (
                StatusCode::OK,
                Json(CallbackResponse {
                    code: 1,
                    data: None,
                }),
            );
        }
        Err(e) => {
            error!("on_publish: db error: {}", e);
            return (
                StatusCode::OK,
                Json(CallbackResponse {
                    code: 1,
                    data: None,
                }),
            );
        }
    };

    let user_model = match user::Entity::find_by_id(room_model.user_id)
        .one(&state.db)
        .await
    {
        Ok(Some(user)) if user.enabled => user,
        Ok(Some(_)) | Ok(None) => {
            warn!(
                stream = %body.stream,
                user_id = room_model.user_id,
                "on_publish: room owner missing or disabled, denying"
            );
            return (
                StatusCode::OK,
                Json(CallbackResponse {
                    code: 1,
                    data: None,
                }),
            );
        }
        Err(e) => {
            error!("on_publish: failed to load room owner: {}", e);
            return (
                StatusCode::OK,
                Json(CallbackResponse {
                    code: 1,
                    data: None,
                }),
            );
        }
    };

    let stream_url = if body.tc_url.is_empty() {
        body.stream.clone()
    } else {
        format!("{}/{}", body.tc_url, body.stream)
    };

    let now = chrono::Utc::now().naive_utc();

    // Atomic upsert — handles race condition with ON CONFLICT
    let session = live_session::ActiveModel {
        stream_id: Set(body.stream.clone()),
        app: Set(body.app.clone()),
        vhost: Set(body.vhost.clone()),
        user_id: Set(user_model.id),
        client_id: Set(body.client_id.clone()),
        stream_url: Set(stream_url.clone()),
        status: Set("active".to_string()),
        started_at: Set(now),
        ..Default::default()
    };

    if let Err(e) = live_session::Entity::insert(session)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(live_session::Column::StreamId)
                .update_columns([
                    live_session::Column::App,
                    live_session::Column::Vhost,
                    live_session::Column::UserId,
                    live_session::Column::ClientId,
                    live_session::Column::StreamUrl,
                    live_session::Column::Status,
                    live_session::Column::StartedAt,
                ])
                .to_owned(),
        )
        .exec(&state.db)
        .await
    {
        error!("on_publish: failed to upsert live session: {}", e);
    }
    if let Err(e) = mark_live_stream_published(&state.db, &body.stream, user_model.id, now).await {
        error!("on_publish: failed to update live stream state: {}", e);
    }

    info!(
        stream = %body.stream,
        user_id = user_model.id,
        room_id = room_model.id,
        "on_publish: stream allowed"
    );

    (
        StatusCode::OK,
        Json(CallbackResponse {
            code: 0,
            data: None,
        }),
    )
}

// POST /api/internal/srs/on_forward
pub async fn on_forward(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ForwardBody>,
) -> impl IntoResponse {
    let rules = match matching_forward_rules(&state.db, &body.app, &body.stream).await {
        Ok(rules) => rules,
        Err(e) => {
            error!("on_forward: failed to query forward rules: {}", e);
            Vec::new()
        }
    };
    let urls = forward_urls_from_rules(rules, &body.app, &body.stream);

    info!(
        app = %body.app,
        stream = %body.stream,
        forward_urls = urls.len(),
        "on_forward callback"
    );

    (
        StatusCode::OK,
        Json(CallbackResponse {
            code: 0,
            data: Some(CallbackData { urls }),
        }),
    )
}

// POST /api/internal/srs/on_unpublish
pub async fn on_unpublish(
    State(state): State<Arc<AppState>>,
    Json(body): Json<UnpublishBody>,
) -> impl IntoResponse {
    info!(stream = %body.stream, "on_unpublish callback");

    let now = chrono::Utc::now().naive_utc();
    let result = live_session::Entity::update_many()
        .filter(live_session::Column::StreamId.eq(&body.stream))
        .filter(live_session::Column::Status.eq("active"))
        .col_expr(
            live_session::Column::Status,
            sea_orm::sea_query::Expr::value("ended"),
        )
        .col_expr(
            live_session::Column::EndedAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .exec(&state.db)
        .await;

    if let Err(e) = result {
        error!("on_unpublish: failed to end live session: {}", e);
    }
    if let Err(e) = mark_live_stream_unpublished(&state.db, &body.stream, now).await {
        error!("on_unpublish: failed to update live stream state: {}", e);
    }
    state.live_hub.clear_stream(&body.stream).await;

    (
        StatusCode::OK,
        Json(CallbackResponse {
            code: 0,
            data: None,
        }),
    )
}

// POST /api/internal/srs/on_play
pub async fn on_play(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PlayBody>,
) -> impl IntoResponse {
    let deny = |stream: &str| {
        warn!(stream = %stream, reason = "room access denied", "on_play denied");
        (
            StatusCode::OK,
            Json(CallbackResponse {
                code: 1,
                data: None,
            }),
        )
    };

    if body.stream.is_empty() || body.client_id.is_empty() {
        return deny(&body.stream);
    }

    let Some(ticket) = parse_room_ticket_from_param(&body.param) else {
        return deny(&body.stream);
    };

    let room = match live_room::Entity::find()
        .filter(live_room::Column::StreamId.eq(&body.stream))
        .filter(live_room::Column::Enabled.eq(true))
        .one(&state.db)
        .await
    {
        Ok(Some(room)) => room,
        Ok(None) | Err(_) => return deny(&body.stream),
    };

    let claims = match admit_room_ticket_with_account_check(
        &state.db,
        &ticket,
        &body.stream,
        &room,
        &state.config.user.auth_secret,
        Utc::now(),
    )
    .await
    {
        Ok(claims) => claims,
        Err(_) => return deny(&body.stream),
    };

    state
        .live_hub
        .play(&body.stream, &body.client_id, &claims.viewer_key)
        .await;

    (
        StatusCode::OK,
        Json(CallbackResponse {
            code: 0,
            data: None,
        }),
    )
}

// POST /api/internal/srs/on_stop
pub async fn on_stop(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StopBody>,
) -> impl IntoResponse {
    if !body.stream.is_empty() && !body.client_id.is_empty() {
        state.live_hub.stop(&body.stream, &body.client_id).await;
    }

    (
        StatusCode::OK,
        Json(CallbackResponse {
            code: 0,
            data: None,
        }),
    )
}

// POST /api/internal/srs/heartbeat
pub async fn heartbeat(
    State(state): State<Arc<AppState>>,
    Json(body): Json<HeartbeatBody>,
) -> impl IntoResponse {
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

    (
        StatusCode::OK,
        Json(CallbackResponse {
            code: 0,
            data: None,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, response::IntoResponse};
    use chrono::NaiveDateTime;
    use sea_orm::{DbBackend, MockDatabase, MockExecResult};

    use crate::config::{
        AppConfig, DbConfig, MetricsConfig, PlaybackConfig, PublishConfig, SrsConfig,
        StorageConfig, UserConfig,
    };
    use crate::entities::live_stream_state;
    use crate::live_hub::LiveHub;
    use crate::srs_client::SrsClient;

    fn test_state_with_hub(
        db: sea_orm::DatabaseConnection,
        live_hub: Arc<LiveHub>,
    ) -> Arc<AppState> {
        Arc::new(AppState {
            db,
            config: Arc::new(AppConfig {
                http_port: 9081,
                db: DbConfig {
                    dsn: "mock".to_string(),
                },
                user: UserConfig {
                    allow_register: false,
                    auth_realm: "stream api".to_string(),
                    auth_secret: "test-secret".to_string(),
                },
                srs: SrsConfig {
                    api_url: "http://srs:1985".to_string(),
                    api_user: "admin".to_string(),
                    api_password: "password".to_string(),
                    callback_secret: "callback-secret".to_string(),
                },
                playback: PlaybackConfig {
                    protocols: "webrtc,hls".to_string(),
                },
                publish: PublishConfig {
                    protocols: "rtmp,whip".to_string(),
                },
                storage: StorageConfig {
                    upload_dir: "uploads-test".to_string(),
                },
                metrics: MetricsConfig { enabled: false },
                cors_origins: vec!["http://localhost:5173".to_string()],
            }),
            srs_client: Arc::new(SrsClient::new(
                "http://srs:1985".to_string(),
                "admin".to_string(),
                "password".to_string(),
            )),
            live_hub,
        })
    }

    fn test_state(db: sea_orm::DatabaseConnection) -> Arc<AppState> {
        test_state_with_hub(db, Arc::new(LiveHub::new()))
    }

    async fn callback_json(response: impl IntoResponse) -> serde_json::Value {
        let response = response.into_response();
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("callback body should be readable");
        serde_json::from_slice::<serde_json::Value>(&body).expect("callback body should be json")
    }

    async fn callback_code(response: impl IntoResponse) -> i32 {
        callback_json(response).await["code"]
            .as_i64()
            .expect("callback code should be an integer") as i32
    }

    fn room_model(stream_id: &str, stream_code: &str) -> live_room::Model {
        let now =
            NaiveDateTime::parse_from_str("2026-06-04 00:00:00", "%F %T").expect("valid time");
        live_room::Model {
            id: 1,
            user_id: 1,
            stream_id: stream_id.to_string(),
            title: String::new(),
            cover_url: String::new(),
            stream_code: stream_code.to_string(),
            enabled: true,
            require_login: false,
            password_hash: String::new(),
            access_revision: 0,
            created_at: now,
            updated_at: now,
        }
    }

    fn user_model() -> user::Model {
        user_model_with_enabled(true)
    }

    fn user_model_with_enabled(enabled: bool) -> user::Model {
        user::Model {
            id: 1,
            username: "dawu".to_string(),
            password: "hashed".to_string(),
            stream_code: "valid-stream-code".to_string(),
            room_title: String::new(),
            role: crate::auth::ROLE_USER.to_string(),
            enabled,
        }
    }

    fn play_body(stream: &str, param: String, client_id: &str) -> PlayBody {
        PlayBody {
            action: "on_play".to_string(),
            app: "live".to_string(),
            stream: stream.to_string(),
            param,
            page_url: String::new(),
            vhost: "__defaultVhost__".to_string(),
            client_id: client_id.to_string(),
            ip: "127.0.0.1".to_string(),
        }
    }

    fn room_ticket(room: &live_room::Model, viewer_key: &str, secret: &str, year: i32) -> String {
        let issued_at = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, year, 1, 1, 0, 0, 0)
            .single()
            .expect("valid ticket timestamp");
        crate::room_access::issue_room_ticket(
            room,
            viewer_key.to_string(),
            crate::room_access::ViewerIdentity {
                kind: crate::room_access::ViewerKind::Guest,
                name: "Test viewer".to_string(),
            },
            None,
            false,
            false,
            secret,
            issued_at,
        )
        .expect("ticket should be issued")
        .token
    }

    fn account_room_ticket(room: &live_room::Model, secret: &str, year: i32) -> String {
        let issued_at = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, year, 1, 1, 0, 0, 0)
            .single()
            .expect("valid ticket timestamp");
        crate::room_access::issue_room_ticket(
            room,
            "user:1".to_string(),
            crate::room_access::ViewerIdentity {
                kind: crate::room_access::ViewerKind::User,
                name: "Test account".to_string(),
            },
            Some(1),
            true,
            false,
            secret,
            issued_at,
        )
        .expect("account ticket should be issued")
        .token
    }

    #[tokio::test]
    async fn on_play_denies_missing_or_invalid_room_tickets() {
        let room = room_model("room-one", "stream-code");
        let valid_ticket = room_ticket(
            &room,
            "guest:00000000-0000-0000-0000-000000000001",
            "test-secret",
            2099,
        );
        let wrong_signature = room_ticket(
            &room,
            "guest:00000000-0000-0000-0000-000000000001",
            "wrong-secret",
            2099,
        );
        let expired_ticket = room_ticket(
            &room,
            "guest:00000000-0000-0000-0000-000000000001",
            "test-secret",
            2000,
        );
        let other_room = room_model("room-two", "stream-code");
        let other_room_ticket = room_ticket(
            &other_room,
            "guest:00000000-0000-0000-0000-000000000001",
            "test-secret",
            2099,
        );
        let mut stale_room = room.clone();
        stale_room.access_revision = 1;

        let denied_cases = vec![
            (String::new(), room.clone()),
            ("?ticket=".to_string(), room.clone()),
            (format!("?token={valid_ticket}"), room.clone()),
            (format!("?streamid=token={valid_ticket}"), room.clone()),
            (
                format!("?ticket={valid_ticket}&ticket={valid_ticket}"),
                room.clone(),
            ),
            (format!("?ticket={wrong_signature}"), room.clone()),
            (format!("?ticket={expired_ticket}"), room.clone()),
            (format!("?ticket={other_room_ticket}"), room.clone()),
            (format!("?ticket={valid_ticket}"), stale_room),
        ];

        for (param, room) in denied_cases {
            let db = MockDatabase::new(DbBackend::Postgres)
                .append_query_results([[room]])
                .into_connection();
            let code = callback_code(
                on_play(
                    State(test_state(db)),
                    Json(play_body("room-one", param, "client-a")),
                )
                .await,
            )
            .await;

            assert_eq!(code, 1);
        }
    }

    #[tokio::test]
    async fn on_play_denies_new_access_for_disabled_ticket_account() {
        let room = room_model("room-one", "stream-code");
        let ticket = account_room_ticket(&room, "test-secret", 2099);
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[room]])
            .append_query_results([[user_model_with_enabled(false)]])
            .into_connection();

        let code = callback_code(
            on_play(
                State(test_state(db)),
                Json(play_body(
                    "room-one",
                    format!("?ticket={ticket}"),
                    "client-a",
                )),
            )
            .await,
        )
        .await;

        assert_eq!(code, 1);
    }

    #[tokio::test]
    async fn on_play_tracks_viewers_idempotently_and_unpublish_clears_them() {
        use tokio::sync::broadcast::error::TryRecvError;

        let room = room_model("room-one", "stream-code");
        let viewer_a_ticket = room_ticket(
            &room,
            "guest:00000000-0000-0000-0000-000000000001",
            "test-secret",
            2099,
        );
        let viewer_b_ticket = room_ticket(
            &room,
            "guest:00000000-0000-0000-0000-000000000002",
            "test-secret",
            2099,
        );
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([
                [room.clone()],
                [room.clone()],
                [room.clone()],
                [room.clone()],
            ])
            .append_exec_results([
                MockExecResult {
                    last_insert_id: 0,
                    rows_affected: 1,
                },
                MockExecResult {
                    last_insert_id: 0,
                    rows_affected: 1,
                },
            ])
            .into_connection();
        let hub = Arc::new(LiveHub::new());
        let (_, mut events) = hub.subscribe(&room.stream_id).await;
        let state = test_state_with_hub(db, hub.clone());

        assert_eq!(
            callback_code(
                on_play(
                    State(state.clone()),
                    Json(play_body(
                        &room.stream_id,
                        format!("?ticket={}", viewer_a_ticket.replace('.', "%2E")),
                        "client-a",
                    )),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count(&room.stream_id).await, 1);
        assert_eq!(
            events.try_recv().expect("first viewer count event"),
            crate::live_hub::RoomEvent::ViewerCount { count: 1 }
        );

        assert_eq!(
            callback_code(
                on_play(
                    State(state.clone()),
                    Json(play_body(
                        &room.stream_id,
                        format!("?ticket={viewer_a_ticket}"),
                        "client-a",
                    )),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count(&room.stream_id).await, 1);
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(
            callback_code(
                on_play(
                    State(state.clone()),
                    Json(play_body(
                        &room.stream_id,
                        format!("?ticket={viewer_a_ticket}"),
                        "client-b",
                    )),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count(&room.stream_id).await, 1);
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(
            callback_code(
                on_play(
                    State(state.clone()),
                    Json(play_body(
                        &room.stream_id,
                        format!("?ticket={viewer_b_ticket}"),
                        "client-c",
                    )),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count(&room.stream_id).await, 2);
        assert_eq!(
            events.try_recv().expect("second viewer count event"),
            crate::live_hub::RoomEvent::ViewerCount { count: 2 }
        );

        assert_eq!(
            callback_code(
                on_stop(
                    State(state.clone()),
                    Json(StopBody {
                        action: "on_stop".to_string(),
                        app: "live".to_string(),
                        stream: room.stream_id.clone(),
                        param: String::new(),
                        vhost: "__defaultVhost__".to_string(),
                        client_id: "client-a".to_string(),
                    }),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count(&room.stream_id).await, 2);
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(
            callback_code(
                on_stop(
                    State(state.clone()),
                    Json(StopBody {
                        action: "on_stop".to_string(),
                        app: "live".to_string(),
                        stream: room.stream_id.clone(),
                        param: String::new(),
                        vhost: "__defaultVhost__".to_string(),
                        client_id: "client-b".to_string(),
                    }),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count(&room.stream_id).await, 1);
        assert_eq!(
            events.try_recv().expect("last viewer reference stop event"),
            crate::live_hub::RoomEvent::ViewerCount { count: 1 }
        );

        for client_id in ["unknown", "client-b"] {
            assert_eq!(
                callback_code(
                    on_stop(
                        State(state.clone()),
                        Json(StopBody {
                            action: "on_stop".to_string(),
                            app: "live".to_string(),
                            stream: room.stream_id.clone(),
                            param: String::new(),
                            vhost: "__defaultVhost__".to_string(),
                            client_id: client_id.to_string(),
                        }),
                    )
                    .await,
                )
                .await,
                0
            );
            assert_eq!(hub.viewer_count(&room.stream_id).await, 1);
            assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
        }

        assert_eq!(
            callback_code(
                on_unpublish(
                    State(state),
                    Json(UnpublishBody {
                        action: "on_unpublish".to_string(),
                        app: "live".to_string(),
                        stream: room.stream_id.clone(),
                        param: String::new(),
                        vhost: "__defaultVhost__".to_string(),
                        client_id: "publisher".to_string(),
                    }),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count(&room.stream_id).await, 0);
        assert_eq!(
            events.try_recv().expect("unpublish clears viewer count"),
            crate::live_hub::RoomEvent::ViewerCount { count: 0 }
        );
    }

    #[tokio::test]
    async fn on_unpublish_clears_viewers_when_database_updates_fail() {
        let hub = Arc::new(LiveHub::new());
        let (_, mut events) = hub.subscribe("room-one").await;
        hub.play(
            "room-one",
            "client-a",
            "guest:00000000-0000-0000-0000-000000000001",
        )
        .await;
        assert_eq!(
            events
                .try_recv()
                .expect("viewer count event before unpublish"),
            crate::live_hub::RoomEvent::ViewerCount { count: 1 }
        );
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_exec_errors([
                DbErr::Custom("live session update failed".to_string()),
                DbErr::Custom("stream state update failed".to_string()),
            ])
            .into_connection();

        assert_eq!(
            callback_code(
                on_unpublish(
                    State(test_state_with_hub(db, hub.clone())),
                    Json(UnpublishBody {
                        action: "on_unpublish".to_string(),
                        app: "live".to_string(),
                        stream: "room-one".to_string(),
                        param: String::new(),
                        vhost: "__defaultVhost__".to_string(),
                        client_id: "publisher".to_string(),
                    }),
                )
                .await,
            )
            .await,
            0
        );
        assert_eq!(hub.viewer_count("room-one").await, 0);
        assert_eq!(
            events
                .try_recv()
                .expect("unpublish broadcasts zero after database errors"),
            crate::live_hub::RoomEvent::ViewerCount { count: 0 }
        );
    }

    #[tokio::test]
    async fn on_publish_rejects_valid_token_for_another_users_stream() {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([Vec::<live_room::Model>::new()])
            .into_connection();

        let code = callback_code(
            on_publish(
                State(test_state(db)),
                Json(PublishBody {
                    action: "on_publish".to_string(),
                    app: "live".to_string(),
                    stream: "ytb".to_string(),
                    param: "?token=valid-stream-code".to_string(),
                    tc_url: "rtmp://live.example.test/live".to_string(),
                    vhost: "__defaultVhost__".to_string(),
                    client_id: "client-1".to_string(),
                    ip: "127.0.0.1".to_string(),
                }),
            )
            .await,
        )
        .await;

        assert_eq!(code, 1);
    }

    #[test]
    fn parse_token_supports_rtmp_whip_and_srt_params() {
        assert_eq!(
            parse_token_from_param("?token=stream-token"),
            Some("stream-token".to_string())
        );
        assert_eq!(
            parse_token_from_param("?app=live&stream=dawu&token=stream-token"),
            Some("stream-token".to_string())
        );
        assert_eq!(
            parse_token_from_param(
                "?streamid=%23%21%3A%3Ar%3Dlive%2Fdawu%2Cm%3Dpublish%2Ctoken%3Dstream-token"
            ),
            Some("stream-token".to_string())
        );
    }

    #[test]
    fn heartbeat_defaults_to_active_when_field_is_missing() {
        let body: HeartbeatBody = serde_json::from_str(
            r#"{"device_id":"srs-1","ip":"127.0.0.1","cpu_usage":1.0,"mem_usage":2.0}"#,
        )
        .expect("heartbeat body should deserialize");

        assert!(body.is_active);
    }

    #[tokio::test]
    async fn on_publish_allows_valid_token_for_own_stream() {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[room_model("dawu", "valid-stream-code")]])
            .append_query_results([[user_model()]])
            .append_exec_results([
                MockExecResult {
                    last_insert_id: 1,
                    rows_affected: 1,
                },
                MockExecResult {
                    last_insert_id: 1,
                    rows_affected: 1,
                },
            ])
            .append_query_results([Vec::<live_stream_state::Model>::new()])
            .into_connection();

        let code = callback_code(
            on_publish(
                State(test_state(db)),
                Json(PublishBody {
                    action: "on_publish".to_string(),
                    app: "live".to_string(),
                    stream: "dawu".to_string(),
                    param: "?token=valid-stream-code".to_string(),
                    tc_url: "rtmp://live.example.test/live".to_string(),
                    vhost: "__defaultVhost__".to_string(),
                    client_id: "client-1".to_string(),
                    ip: "127.0.0.1".to_string(),
                }),
            )
            .await,
        )
        .await;

        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn on_forward_returns_matching_rules_and_expands_templates() {
        let now =
            NaiveDateTime::parse_from_str("2026-06-04 12:00:00", "%F %T").expect("valid time");
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[
                forward_rule::Model {
                    id: 1,
                    stream_filter: "*".to_string(),
                    target_url: "rtmp://edge.example/live/{stream}".to_string(),
                    enabled: true,
                    created_at: now,
                    updated_at: now,
                },
                forward_rule::Model {
                    id: 2,
                    stream_filter: "live/dawu".to_string(),
                    target_url: "rtmp://backup.example/{app}/{stream}".to_string(),
                    enabled: true,
                    created_at: now,
                    updated_at: now,
                },
            ]])
            .into_connection();

        let json = callback_json(
            on_forward(
                State(test_state(db)),
                Json(ForwardBody {
                    action: "on_forward".to_string(),
                    app: "live".to_string(),
                    stream: "dawu".to_string(),
                    param: String::new(),
                    tc_url: "rtmp://live.example.test/live".to_string(),
                    vhost: "__defaultVhost__".to_string(),
                    client_id: "client-1".to_string(),
                    server_id: "server-1".to_string(),
                    ip: "127.0.0.1".to_string(),
                }),
            )
            .await,
        )
        .await;

        assert_eq!(json["code"], 0);
        assert_eq!(
            json["data"]["urls"],
            serde_json::json!([
                "rtmp://edge.example/live/dawu",
                "rtmp://backup.example/live/dawu"
            ])
        );
    }

    #[test]
    fn forward_rule_filters_cover_global_stream_app_and_exact() {
        assert_eq!(
            forward_rule_filters("live", "dawu"),
            vec!["*", "dawu", "live/*", "live/dawu"]
        );
    }

    fn live_state(
        status: &str,
        episode_started_at: &str,
        last_unpublished_at: Option<&str>,
    ) -> live_stream_state::Model {
        live_stream_state::Model {
            id: 1,
            stream_id: "dawu".to_string(),
            user_id: 1,
            status: status.to_string(),
            episode_started_at: NaiveDateTime::parse_from_str(episode_started_at, "%F %T")
                .expect("valid episode timestamp"),
            last_unpublished_at: last_unpublished_at.map(|value| {
                NaiveDateTime::parse_from_str(value, "%F %T").expect("valid unpublished timestamp")
            }),
            updated_at: NaiveDateTime::parse_from_str(episode_started_at, "%F %T")
                .expect("valid update timestamp"),
        }
    }

    #[test]
    fn reconnect_inside_grace_keeps_original_episode_start() {
        let previous = live_state("ended", "2026-06-04 12:00:00", Some("2026-06-04 12:03:00"));
        let now =
            NaiveDateTime::parse_from_str("2026-06-04 12:08:00", "%F %T").expect("valid timestamp");

        let started_at = next_episode_started_at(Some(&previous), now);

        assert_eq!(started_at, previous.episode_started_at);
    }

    #[test]
    fn reconnect_after_grace_starts_new_episode() {
        let previous = live_state("ended", "2026-06-04 12:00:00", Some("2026-06-04 12:03:00"));
        let now =
            NaiveDateTime::parse_from_str("2026-06-04 12:20:00", "%F %T").expect("valid timestamp");

        let started_at = next_episode_started_at(Some(&previous), now);

        assert_eq!(started_at, now);
    }
}
