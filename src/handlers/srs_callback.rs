use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use chrono::{Duration, NaiveDateTime};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, Set,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::entities::{forward_rule, live_session, live_stream_state, srs_server, user};
use crate::AppState;

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

    // Validate stream_code by finding the user
    let user_model = match user::Entity::find()
        .filter(user::Column::StreamCode.eq(&stream_code))
        .one(&state.db)
        .await
    {
        Ok(Some(u)) => u,
        Ok(None) => {
            warn!(stream_code = %stream_code, "on_publish: invalid stream_code, denying");
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

    if body.stream != user_model.username {
        warn!(
            stream = %body.stream,
            username = %user_model.username,
            "on_publish: stream does not belong to user, denying"
        );
        return (
            StatusCode::OK,
            Json(CallbackResponse {
                code: 1,
                data: None,
            }),
        );
    }

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

    let stream_key = format!("{}/{}", body.app, body.stream);
    let rules = match forward_rule::Entity::find()
        .filter(
            forward_rule::Column::Enabled.eq(true).and(
                forward_rule::Column::StreamFilter
                    .eq("*")
                    .or(forward_rule::Column::StreamFilter.eq(&stream_key)),
            ),
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
        return (
            StatusCode::OK,
            Json(CallbackResponse {
                code: 0,
                data: Some(CallbackData { urls }),
            }),
        );
    }

    (
        StatusCode::OK,
        Json(CallbackResponse {
            code: 0,
            data: None,
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
    State(_state): State<Arc<AppState>>,
    Json(body): Json<PlayBody>,
) -> impl IntoResponse {
    info!(stream = %body.stream, ip = %body.ip, "on_play callback");
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
    State(_state): State<Arc<AppState>>,
    Json(body): Json<StopBody>,
) -> impl IntoResponse {
    info!(stream = %body.stream, "on_stop callback");
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
        AppConfig, DbConfig, MetricsConfig, PlaybackConfig, SrsConfig, UserConfig,
    };
    use crate::entities::live_stream_state;
    use crate::srs_client::SrsClient;

    fn test_state(db: sea_orm::DatabaseConnection) -> Arc<AppState> {
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
                metrics: MetricsConfig { enabled: false },
                cors_origins: vec!["http://localhost:5173".to_string()],
            }),
            srs_client: Arc::new(SrsClient::new(
                "http://srs:1985".to_string(),
                "admin".to_string(),
                "password".to_string(),
            )),
        })
    }

    async fn callback_code(response: impl IntoResponse) -> i32 {
        let response = response.into_response();
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("callback body should be readable");
        serde_json::from_slice::<serde_json::Value>(&body).expect("callback body should be json")
            ["code"]
            .as_i64()
            .expect("callback code should be an integer") as i32
    }

    #[tokio::test]
    async fn on_publish_rejects_valid_token_for_another_users_stream() {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[user::Model {
                id: 1,
                username: "dawu".to_string(),
                password: "hashed".to_string(),
                stream_code: "valid-stream-code".to_string(),
                room_title: String::new(),
            }]])
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .append_query_results([Vec::<forward_rule::Model>::new()])
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

    #[tokio::test]
    async fn on_publish_allows_valid_token_for_own_stream() {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[user::Model {
                id: 1,
                username: "dawu".to_string(),
                password: "hashed".to_string(),
                stream_code: "valid-stream-code".to_string(),
                room_title: String::new(),
            }]])
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .append_query_results([Vec::<forward_rule::Model>::new()])
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
