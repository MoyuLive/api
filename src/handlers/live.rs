use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder,
    Set,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tracing::{error, info};

use crate::auth::{generate_random_string, CurrentUser};
use crate::entities::{live_room, live_session};
use crate::response::{error_response, success_response};
use crate::srs_client::SrsStream;
use crate::AppState;

const MAX_ROOM_TITLE_CHARS: usize = 80;

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct PublicLiveRoom {
    pub stream_id: String,
    pub title: String,
    pub app: String,
    pub status: String,
    pub started_at_ms: Option<i64>,
    pub live_ms: i64,
    pub video_width: Option<i32>,
    pub video_height: Option<i32>,
    pub recv_kbps: Option<i32>,
    pub send_kbps: Option<i32>,
}

async fn default_live_room(
    db: &DatabaseConnection,
    auth_user: &CurrentUser,
) -> Result<Option<live_room::Model>, DbErr> {
    let username_room = live_room::Entity::find()
        .filter(live_room::Column::UserId.eq(auth_user.user_id))
        .filter(live_room::Column::StreamId.eq(&auth_user.username))
        .one(db)
        .await?;

    if username_room.is_some() {
        return Ok(username_room);
    }

    live_room::Entity::find()
        .filter(live_room::Column::UserId.eq(auth_user.user_id))
        .order_by_asc(live_room::Column::Id)
        .one(db)
        .await
}

// GET /api/live/stream/code
pub async fn stream_code(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    let room = match default_live_room(&state.db, &auth_user).await {
        Ok(Some(room)) => room,
        Ok(None) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "default live room not found"),
            );
        }
        Err(e) => {
            error!("Failed to get default live room: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get live room"),
            );
        }
    };

    (
        StatusCode::OK,
        success_response(serde_json::json!({
            "stream_code": room.stream_code,
            "stream_id": room.stream_id,
            "username": auth_user.username,
            "title": room.title,
        })),
    )
}

// POST /api/live/stream/code/reset
pub async fn reset_stream_code(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    let new_code = generate_random_string(16);
    let room = match default_live_room(&state.db, &auth_user).await {
        Ok(Some(room)) => room,
        Ok(None) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "default live room not found"),
            );
        }
        Err(e) => {
            error!("Failed to get default live room: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get live room"),
            );
        }
    };

    let mut active: live_room::ActiveModel = room.clone().into();
    active.stream_code = Set(new_code.clone());
    active.updated_at = Set(Utc::now().naive_utc());
    let result = active.update(&state.db).await;

    match result {
        Ok(updated_room) => {
            sync_legacy_user_room_fields(&state.db, &auth_user, &updated_room).await;
            (
                StatusCode::OK,
                success_response(serde_json::json!({
                    "stream_code": new_code,
                    "stream_id": updated_room.stream_id,
                    "username": auth_user.username,
                    "title": updated_room.title,
                })),
            )
        }
        Err(e) => {
            error!("Failed to reset stream code: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to reset stream code"),
            )
        }
    }
}

#[derive(Deserialize)]
pub struct UpdateRoomTitleRequest {
    pub title: String,
}

// PUT /api/live/room/title
pub async fn update_room_title(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Json(req): Json<UpdateRoomTitleRequest>,
) -> impl IntoResponse {
    let title = match normalize_room_title(&req.title) {
        Ok(title) => title,
        Err(message) => {
            return (StatusCode::BAD_REQUEST, error_response(400, message));
        }
    };

    let room = match default_live_room(&state.db, &auth_user).await {
        Ok(Some(room)) => room,
        Ok(None) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "default live room not found"),
            );
        }
        Err(e) => {
            error!("Failed to get default live room: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get live room"),
            );
        }
    };

    let mut active: live_room::ActiveModel = room.into();
    active.title = Set(title.clone());
    active.updated_at = Set(Utc::now().naive_utc());
    let result = active.update(&state.db).await;

    match result {
        Ok(updated_room) => {
            sync_legacy_user_room_fields(&state.db, &auth_user, &updated_room).await;
            (
                StatusCode::OK,
                success_response(serde_json::json!({
                    "stream_id": updated_room.stream_id,
                    "username": auth_user.username,
                    "title": title,
                })),
            )
        }
        Err(e) => {
            error!("Failed to update room title: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update room title"),
            )
        }
    }
}

async fn sync_legacy_user_room_fields(
    db: &DatabaseConnection,
    auth_user: &CurrentUser,
    room: &live_room::Model,
) {
    if room.stream_id != auth_user.username {
        return;
    }

    let result = crate::entities::user::Entity::update_many()
        .filter(crate::entities::user::Column::Id.eq(auth_user.user_id))
        .col_expr(
            crate::entities::user::Column::StreamCode,
            sea_orm::sea_query::Expr::value(room.stream_code.clone()),
        )
        .col_expr(
            crate::entities::user::Column::RoomTitle,
            sea_orm::sea_query::Expr::value(room.title.clone()),
        )
        .exec(db)
        .await;

    if let Err(e) = result {
        error!("Failed to sync legacy user room fields: {}", e);
    }
}

#[derive(Deserialize)]
pub struct StreamStatusQuery {
    pub stream: Option<String>,
}

// GET /api/live/stream/status
pub async fn stream_status(
    State(state): State<Arc<AppState>>,
    Query(query): Query<StreamStatusQuery>,
) -> impl IntoResponse {
    let stream_id = match query.stream {
        Some(ref s) if !s.is_empty() => s.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                error_response(400, "missing stream parameter"),
            );
        }
    };

    match state.srs_client.get_stream(&stream_id).await {
        Ok(Some(stream)) => (
            StatusCode::OK,
            success_response(serde_json::json!({
                "stream_id": stream_id,
                "online": true,
                "stream": stream,
            })),
        ),
        Ok(None) => (
            StatusCode::OK,
            success_response(serde_json::json!({
                "stream_id": stream_id,
                "online": false,
            })),
        ),
        Err(e) => {
            error!("Failed to query stream status: {}", e);
            (
                StatusCode::OK,
                success_response(serde_json::json!({
                    "stream_id": stream_id,
                    "online": false,
                    "error": "failed to query stream status",
                })),
            )
        }
    }
}

#[derive(Deserialize)]
pub struct StopStreamRequest {
    pub stream_id: String,
}

// POST /api/live/stream/stop
pub async fn stop_stream(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Json(req): Json<StopStreamRequest>,
) -> impl IntoResponse {
    // Get the live session to find the client ID
    let session = live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&req.stream_id))
        .one(&state.db)
        .await;

    match session {
        Ok(Some(s)) => {
            // Verify stream ownership
            if !auth_user.is_admin() && s.user_id != auth_user.user_id {
                return (
                    StatusCode::FORBIDDEN,
                    error_response(403, "you can only stop your own streams"),
                );
            }

            let client_id = if s.client_id.is_empty() {
                req.stream_id.clone()
            } else {
                s.client_id
            };

            match state.srs_client.kick_client(&client_id).await {
                Ok(_) => {
                    info!("Stream stopped: {}", req.stream_id);
                    (
                        StatusCode::OK,
                        success_response(serde_json::json!({
                            "stream_id": req.stream_id,
                            "stopped": true,
                        })),
                    )
                }
                Err(e) => {
                    error!("Failed to kick client: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        error_response(500, "failed to stop stream"),
                    )
                }
            }
        }
        Ok(None) => {
            match live_room::Entity::find()
                .filter(live_room::Column::StreamId.eq(&req.stream_id))
                .one(&state.db)
                .await
            {
                Ok(Some(room)) if auth_user.is_admin() || room.user_id == auth_user.user_id => {}
                Ok(Some(_)) => {
                    return (
                        StatusCode::FORBIDDEN,
                        error_response(403, "you can only stop your own streams"),
                    );
                }
                Ok(None) => {
                    return (
                        StatusCode::NOT_FOUND,
                        error_response(404, "live room not found"),
                    );
                }
                Err(e) => {
                    error!("Failed to get live room: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        error_response(500, "failed to get live room"),
                    );
                }
            }

            // Try kicking via stream name as fallback after authorization.
            match state.srs_client.kick_client(&req.stream_id).await {
                Ok(_) => (
                    StatusCode::OK,
                    success_response(serde_json::json!({
                        "stream_id": req.stream_id,
                        "stopped": true,
                    })),
                ),
                Err(e) => {
                    error!("Failed to kick client: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        error_response(500, "failed to stop stream"),
                    )
                }
            }
        }
        Err(e) => {
            error!("Failed to get live session: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get live session"),
            )
        }
    }
}

// GET /api/live/stream/list
pub async fn stream_list(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    if !auth_user.is_admin() {
        return (StatusCode::FORBIDDEN, error_response(403, "admin required"));
    }

    let sessions = live_session::Entity::find()
        .filter(live_session::Column::Status.eq("active"))
        .all(&state.db)
        .await;

    match sessions {
        Ok(list) => (StatusCode::OK, success_response(list)),
        Err(e) => {
            error!("Failed to get active sessions: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get active sessions"),
            )
        }
    }
}

// GET /api/live/rooms
pub async fn public_live_rooms(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let streams = match state.srs_client.list_all_streams().await {
        Ok(streams) => streams,
        Err(e) => {
            error!("Failed to query SRS live streams: {}", e);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                error_response(503, "failed to query live streams"),
            );
        }
    };

    let streams: Vec<SrsStream> = streams
        .into_iter()
        .filter(SrsStream::is_publishing)
        .collect();
    let stream_ids: Vec<String> = streams.iter().map(|stream| stream.name.clone()).collect();
    let sessions = if stream_ids.is_empty() {
        Vec::new()
    } else {
        match live_session::Entity::find()
            .filter(live_session::Column::StreamId.is_in(stream_ids))
            .filter(live_session::Column::Status.eq("active"))
            .all(&state.db)
            .await
        {
            Ok(sessions) => sessions,
            Err(e) => {
                error!("Failed to load active live session metadata: {}", e);
                Vec::new()
            }
        }
    };

    let room_ids: Vec<String> = sessions
        .iter()
        .map(|session| session.stream_id.clone())
        .collect();
    let rooms = if room_ids.is_empty() {
        Vec::new()
    } else {
        match live_room::Entity::find()
            .filter(live_room::Column::StreamId.is_in(room_ids))
            .all(&state.db)
            .await
        {
            Ok(rooms) => rooms,
            Err(e) => {
                error!("Failed to load live room metadata: {}", e);
                Vec::new()
            }
        }
    };

    let rooms = build_public_live_rooms(streams, sessions, rooms);
    (StatusCode::OK, success_response(rooms))
}

fn build_public_live_rooms(
    streams: Vec<SrsStream>,
    sessions: Vec<live_session::Model>,
    rooms: Vec<live_room::Model>,
) -> Vec<PublicLiveRoom> {
    let sessions_by_stream_id: HashMap<String, live_session::Model> = sessions
        .into_iter()
        .map(|session| (session.stream_id.clone(), session))
        .collect();
    let rooms_by_stream_id: HashMap<String, live_room::Model> = rooms
        .into_iter()
        .map(|room| (room.stream_id.clone(), room))
        .collect();

    let mut rooms: Vec<PublicLiveRoom> = streams
        .into_iter()
        .filter(SrsStream::is_publishing)
        .map(|stream| {
            let session = sessions_by_stream_id.get(&stream.name);
            let room = rooms_by_stream_id.get(&stream.name);
            let started_at = session.map(|session| session.started_at);
            PublicLiveRoom {
                title: room
                    .map(|room| public_room_title(&room.title, &stream.name))
                    .unwrap_or_else(|| stream.name.clone()),
                stream_id: stream.name,
                app: stream.app,
                status: "live".to_string(),
                started_at_ms: public_started_at_ms(started_at),
                live_ms: public_live_duration_ms(started_at, stream.live_ms),
                video_width: stream
                    .video
                    .as_ref()
                    .and_then(|video| positive_dimension(video.width)),
                video_height: stream
                    .video
                    .as_ref()
                    .and_then(|video| positive_dimension(video.height)),
                recv_kbps: stream.kbps.as_ref().map(|kbps| kbps.recv_30s),
                send_kbps: stream.kbps.as_ref().map(|kbps| kbps.send_30s),
            }
        })
        .collect();

    rooms.sort_by(|a, b| {
        b.started_at_ms
            .cmp(&a.started_at_ms)
            .then_with(|| a.stream_id.cmp(&b.stream_id))
    });
    rooms
}

pub(crate) fn normalize_room_title(title: &str) -> Result<String, &'static str> {
    let title = title.trim();
    if title.chars().count() > MAX_ROOM_TITLE_CHARS {
        return Err("room title is too long");
    }

    Ok(title.to_string())
}

fn public_room_title(room_title: &str, fallback: &str) -> String {
    let title = room_title.trim();
    if title.is_empty() {
        fallback.to_string()
    } else {
        title.to_string()
    }
}

fn positive_dimension(value: i32) -> Option<i32> {
    (value > 0).then_some(value)
}

fn public_started_at_ms(started_at: Option<NaiveDateTime>) -> Option<i64> {
    started_at.map(|started_at| {
        DateTime::<Utc>::from_naive_utc_and_offset(started_at, Utc).timestamp_millis()
    })
}

fn public_live_duration_ms(started_at: Option<NaiveDateTime>, srs_live_ms: i64) -> i64 {
    if let Some(started_at) = started_at {
        return (Utc::now().naive_utc() - started_at)
            .num_milliseconds()
            .max(0);
    }

    normalize_srs_live_ms(srs_live_ms)
}

fn normalize_srs_live_ms(value: i64) -> i64 {
    if value > 1_000_000_000_000 {
        return (Utc::now().timestamp_millis() - value).max(0);
    }

    value.max(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::srs_client::{SrsStreamKbps, SrsStreamPublish, SrsStreamVideo};

    fn live_session(stream_id: &str, started_at: NaiveDateTime) -> live_session::Model {
        live_session::Model {
            id: 1,
            stream_id: stream_id.to_string(),
            app: "live".to_string(),
            vhost: "__defaultVhost__".to_string(),
            user_id: 1,
            client_id: "client-1".to_string(),
            server_id: "server-1".to_string(),
            stream_url: format!("rtmp://localhost/live/{}", stream_id),
            status: "active".to_string(),
            video_codec: "".to_string(),
            audio_codec: "".to_string(),
            video_width: 0,
            video_height: 0,
            started_at,
            ended_at: None,
        }
    }

    fn live_room_model(stream_id: &str, title: &str) -> live_room::Model {
        let now =
            NaiveDateTime::parse_from_str("2026-06-01 00:00:00", "%F %T").expect("valid timestamp");
        live_room::Model {
            id: 1,
            user_id: 1,
            stream_id: stream_id.to_string(),
            title: title.to_string(),
            stream_code: "stream-code".to_string(),
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }

    fn live_room_model_with_id(id: i32, stream_id: &str, title: &str) -> live_room::Model {
        live_room::Model {
            id,
            ..live_room_model(stream_id, title)
        }
    }

    fn srs_stream(name: &str, live_ms: i64) -> SrsStream {
        SrsStream {
            id: format!("__defaultVhost__/live/{}", name),
            name: name.to_string(),
            vhost: "__defaultVhost__".to_string(),
            app: "live".to_string(),
            live_ms,
            clients: 3,
            frames: 12_000,
            send_bytes: 2048,
            recv_bytes: 4096,
            kbps: Some(SrsStreamKbps {
                recv_30s: 1800,
                send_30s: 600,
            }),
            audio: None,
            video: Some(SrsStreamVideo {
                codec: "H264".to_string(),
                profile: "High".to_string(),
                level: "4.1".to_string(),
                width: 1920,
                height: 1080,
            }),
            publish: Some(SrsStreamPublish {
                active: true,
                cid: Some("client-1".to_string()),
            }),
        }
    }

    fn inactive_srs_stream(name: &str) -> SrsStream {
        SrsStream {
            publish: Some(SrsStreamPublish {
                active: false,
                cid: None,
            }),
            ..srs_stream(name, 60_000)
        }
    }

    #[test]
    fn public_rooms_are_built_only_from_srs_online_streams() {
        let started_at =
            NaiveDateTime::parse_from_str("2026-06-01 12:30:00", "%F %T").expect("valid timestamp");
        let rooms = build_public_live_rooms(
            vec![
                srs_stream("dawu", 60_000),
                inactive_srs_stream("inactive-source"),
            ],
            vec![
                live_session("dawu", started_at),
                live_session("inactive-source", started_at),
                live_session("offline-but-active-in-db", started_at),
            ],
            vec![live_room_model("dawu", "大雾的游戏时间")],
        );

        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].stream_id, "dawu");
        assert_eq!(rooms[0].title, "大雾的游戏时间");
        assert_eq!(rooms[0].started_at_ms, Some(1_780_317_000_000));
        assert_eq!(rooms[0].video_width, Some(1920));
        assert_eq!(rooms[0].video_height, Some(1080));
        assert_eq!(rooms[0].recv_kbps, Some(1800));
        assert_eq!(rooms[0].send_kbps, Some(600));
    }

    #[test]
    fn public_rooms_sort_by_known_started_time_then_stream_id() {
        let old =
            NaiveDateTime::parse_from_str("2026-06-01 12:00:00", "%F %T").expect("valid timestamp");
        let new =
            NaiveDateTime::parse_from_str("2026-06-01 13:00:00", "%F %T").expect("valid timestamp");

        let rooms = build_public_live_rooms(
            vec![
                srs_stream("without-session", 10_000),
                srs_stream("old", 20_000),
                srs_stream("new", 30_000),
            ],
            vec![live_session("old", old), live_session("new", new)],
            vec![
                live_room_model_with_id(1, "old", "旧直播间"),
                live_room_model_with_id(2, "new", "新直播间"),
            ],
        );

        let ids: Vec<&str> = rooms.iter().map(|room| room.stream_id.as_str()).collect();
        assert_eq!(ids, vec!["new", "old", "without-session"]);
    }

    #[test]
    fn public_room_title_falls_back_to_stream_id_when_blank() {
        let started_at =
            NaiveDateTime::parse_from_str("2026-06-01 12:00:00", "%F %T").expect("valid timestamp");
        let rooms = build_public_live_rooms(
            vec![srs_stream("dawu", 60_000)],
            vec![live_session("dawu", started_at)],
            vec![live_room_model("dawu", "   ")],
        );

        assert_eq!(rooms[0].title, "dawu");
    }

    #[test]
    fn room_title_is_trimmed_before_persisting() {
        let title = normalize_room_title("  晚上打 Terraria  ");

        assert_eq!(title.expect("title should be valid"), "晚上打 Terraria");
    }

    #[test]
    fn room_title_rejects_overlong_values() {
        let title = normalize_room_title(&"a".repeat(MAX_ROOM_TITLE_CHARS + 1));

        assert_eq!(title, Err("room title is too long"));
    }
}
