use axum::{
    extract::{Multipart, Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder,
    Set,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    path::Path,
    sync::Arc,
};
use tokio::fs;
use tracing::{error, info};

use crate::auth::{generate_random_string, CurrentUser};
use crate::entities::{live_room, live_session};
use crate::response::{error_response, success_response};
use crate::srs_client::SrsStream;
use crate::AppState;

const MAX_ROOM_TITLE_CHARS: usize = 80;
pub(crate) const MAX_COVER_UPLOAD_BYTES: usize = 5 * 1024 * 1024;
pub(crate) const MAX_COVER_REQUEST_BYTES: usize = MAX_COVER_UPLOAD_BYTES + 64 * 1024;
const COVER_DIR_NAME: &str = "covers";
const COVER_PUBLIC_PREFIX: &str = "/uploads/covers/";

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct PublicLiveRoom {
    pub stream_id: String,
    pub title: String,
    pub cover_url: String,
    pub app: String,
    pub status: String,
    pub started_at_ms: Option<i64>,
    pub live_ms: i64,
    pub video_width: Option<i32>,
    pub video_height: Option<i32>,
    pub recv_kbps: Option<i32>,
    pub send_kbps: Option<i32>,
    pub require_login: bool,
    pub has_password: bool,
    pub viewer_count: usize,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct OwnLiveRoom {
    pub id: i32,
    pub user_id: i32,
    pub username: String,
    pub stream_id: String,
    pub title: String,
    pub cover_url: String,
    pub stream_code: String,
    pub enabled: bool,
    pub require_login: bool,
    pub has_password: bool,
    pub status: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
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

fn own_live_room_response(room: live_room::Model, username: &str, is_live: bool) -> OwnLiveRoom {
    OwnLiveRoom {
        id: room.id,
        user_id: room.user_id,
        username: username.to_string(),
        stream_id: room.stream_id,
        title: room.title,
        cover_url: room.cover_url,
        stream_code: room.stream_code,
        enabled: room.enabled,
        require_login: room.require_login,
        has_password: !room.password_hash.is_empty(),
        status: if is_live { "live" } else { "offline" }.to_string(),
        created_at: room.created_at,
        updated_at: room.updated_at,
    }
}

fn build_own_live_room_responses(
    rooms: Vec<live_room::Model>,
    sessions: Vec<live_session::Model>,
    username: &str,
) -> Vec<OwnLiveRoom> {
    let active_stream_ids: HashSet<String> = sessions
        .into_iter()
        .map(|session| session.stream_id)
        .collect();

    rooms
        .into_iter()
        .map(|room| {
            let is_live = active_stream_ids.contains(&room.stream_id);
            own_live_room_response(room, username, is_live)
        })
        .collect()
}

#[allow(clippy::result_large_err)]
async fn owned_live_room_by_id(
    db: &DatabaseConnection,
    auth_user: &CurrentUser,
    room_id: i32,
) -> Result<live_room::Model, (StatusCode, Response)> {
    let room = match live_room::Entity::find_by_id(room_id).one(db).await {
        Ok(Some(room)) => room,
        Ok(None) => return Err((StatusCode::NOT_FOUND, error_response(404, "room not found"))),
        Err(e) => {
            error!("Failed to find owned live room: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get live room"),
            ));
        }
    };

    if room.user_id != auth_user.user_id {
        return Err((
            StatusCode::FORBIDDEN,
            error_response(403, "you can only manage your own live rooms"),
        ));
    }

    Ok(room)
}

#[allow(clippy::result_large_err)]
async fn load_own_live_room_response(
    db: &DatabaseConnection,
    auth_user: &CurrentUser,
    room: live_room::Model,
) -> Result<OwnLiveRoom, (StatusCode, Response)> {
    let is_live = match live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&room.stream_id))
        .filter(live_session::Column::Status.eq("active"))
        .one(db)
        .await
    {
        Ok(session) => session.is_some(),
        Err(e) => {
            error!("Failed to load owned room live session: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to load live session"),
            ));
        }
    };

    Ok(own_live_room_response(room, &auth_user.username, is_live))
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
            "id": room.id,
            "stream_code": room.stream_code,
            "stream_id": room.stream_id,
            "username": auth_user.username,
            "title": room.title,
            "cover_url": room.cover_url,
            "enabled": room.enabled,
        })),
    )
}

// GET /api/live/my/rooms
pub async fn my_live_rooms(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    let rooms = match live_room::Entity::find()
        .filter(live_room::Column::UserId.eq(auth_user.user_id))
        .order_by_asc(live_room::Column::Id)
        .all(&state.db)
        .await
    {
        Ok(rooms) => rooms,
        Err(e) => {
            error!("Failed to list owned live rooms: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to list live rooms"),
            );
        }
    };

    let stream_ids: Vec<String> = rooms.iter().map(|room| room.stream_id.clone()).collect();
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
                error!("Failed to load owned room sessions: {}", e);
                Vec::new()
            }
        }
    };

    (
        StatusCode::OK,
        success_response(build_own_live_room_responses(
            rooms,
            sessions,
            &auth_user.username,
        )),
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
                    "cover_url": updated_room.cover_url,
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

// POST /api/live/rooms/:id/stream-code/reset
pub async fn reset_stream_code_by_id(
    AxumPath(id): AxumPath<i32>,
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    let room = match owned_live_room_by_id(&state.db, &auth_user, id).await {
        Ok(room) => room,
        Err(response) => return response,
    };

    let mut active: live_room::ActiveModel = room.into();
    active.stream_code = Set(generate_random_string(16));
    active.updated_at = Set(Utc::now().naive_utc());

    match active.update(&state.db).await {
        Ok(updated_room) => {
            sync_legacy_user_room_fields(&state.db, &auth_user, &updated_room).await;
            match load_own_live_room_response(&state.db, &auth_user, updated_room).await {
                Ok(response) => (StatusCode::OK, success_response(response)),
                Err(response) => response,
            }
        }
        Err(e) => {
            error!("Failed to reset owned room stream code: {}", e);
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
                    "cover_url": updated_room.cover_url,
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

// PUT /api/live/rooms/:id/title
pub async fn update_room_title_by_id(
    AxumPath(id): AxumPath<i32>,
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

    let room = match owned_live_room_by_id(&state.db, &auth_user, id).await {
        Ok(room) => room,
        Err(response) => return response,
    };

    let mut active: live_room::ActiveModel = room.into();
    active.title = Set(title);
    active.updated_at = Set(Utc::now().naive_utc());

    match active.update(&state.db).await {
        Ok(updated_room) => {
            sync_legacy_user_room_fields(&state.db, &auth_user, &updated_room).await;
            match load_own_live_room_response(&state.db, &auth_user, updated_room).await {
                Ok(response) => (StatusCode::OK, success_response(response)),
                Err(response) => response,
            }
        }
        Err(e) => {
            error!("Failed to update owned room title: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update room title"),
            )
        }
    }
}

// PUT /api/live/room/cover
pub async fn update_room_cover(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    mut multipart: Multipart,
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

    update_room_cover_file(&state, &auth_user, room, &mut multipart).await
}

// PUT /api/live/rooms/:id/cover
pub async fn update_room_cover_by_id(
    AxumPath(id): AxumPath<i32>,
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let room = match live_room::Entity::find_by_id(id).one(&state.db).await {
        Ok(Some(room)) => room,
        Ok(None) => return (StatusCode::NOT_FOUND, error_response(404, "room not found")),
        Err(e) => {
            error!("Failed to find live room before cover update: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to find live room"),
            );
        }
    };

    if !auth_user.is_admin() && room.user_id != auth_user.user_id {
        return (
            StatusCode::FORBIDDEN,
            error_response(403, "you can only update your own room cover"),
        );
    }

    update_room_cover_file(&state, &auth_user, room, &mut multipart).await
}

async fn update_room_cover_file(
    state: &Arc<AppState>,
    auth_user: &CurrentUser,
    room: live_room::Model,
    multipart: &mut Multipart,
) -> (StatusCode, Response) {
    let (bytes, extension) = match read_cover_upload(multipart).await {
        Ok(upload) => upload,
        Err(message) => return (StatusCode::BAD_REQUEST, error_response(400, message)),
    };

    let cover_url =
        match save_cover_file(&state.config.storage.upload_dir, room.id, &bytes, extension).await {
            Ok(cover_url) => cover_url,
            Err(e) => {
                error!("Failed to save room cover: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_response(500, "failed to save room cover"),
                );
            }
        };

    let previous_cover_url = room.cover_url.clone();
    let mut active: live_room::ActiveModel = room.into();
    active.cover_url = Set(cover_url.clone());
    active.updated_at = Set(Utc::now().naive_utc());

    match active.update(&state.db).await {
        Ok(updated_room) => {
            remove_cover_file(&state.config.storage.upload_dir, &previous_cover_url).await;
            (
                StatusCode::OK,
                success_response(serde_json::json!({
                    "id": updated_room.id,
                    "stream_id": updated_room.stream_id,
                    "username": auth_user.username,
                    "cover_url": updated_room.cover_url,
                })),
            )
        }
        Err(e) => {
            error!("Failed to update room cover: {}", e);
            remove_cover_file(&state.config.storage.upload_dir, &cover_url).await;
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update room cover"),
            )
        }
    }
}

async fn read_cover_upload(
    multipart: &mut Multipart,
) -> Result<(Vec<u8>, &'static str), &'static str> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| "failed to read cover upload")?
    {
        if field.name() != Some("cover") {
            continue;
        }

        let bytes = field
            .bytes()
            .await
            .map_err(|_| "failed to read cover upload")?;
        if bytes.is_empty() {
            return Err("cover image is empty");
        }
        if bytes.len() > MAX_COVER_UPLOAD_BYTES {
            return Err("cover image is too large");
        }

        let extension = cover_image_extension(&bytes).ok_or("unsupported cover image type")?;
        return Ok((bytes.to_vec(), extension));
    }

    Err("missing cover image")
}

fn cover_image_extension(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("jpg");
    }
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some("png");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Some("webp");
    }

    None
}

async fn save_cover_file(
    upload_dir: &str,
    room_id: i32,
    bytes: &[u8],
    extension: &str,
) -> Result<String, std::io::Error> {
    let cover_dir = Path::new(upload_dir).join(COVER_DIR_NAME);
    fs::create_dir_all(&cover_dir).await?;

    let filename = format!(
        "room-{}-{}-{}.{}",
        room_id,
        Utc::now().timestamp_millis(),
        generate_random_string(8),
        extension
    );
    let path = cover_dir.join(&filename);
    fs::write(path, bytes).await?;

    Ok(format!("{}{}", COVER_PUBLIC_PREFIX, filename))
}

async fn remove_cover_file(upload_dir: &str, cover_url: &str) {
    let Some(filename) = cover_url.strip_prefix(COVER_PUBLIC_PREFIX) else {
        return;
    };
    if filename.is_empty() || filename.contains('/') || filename.contains('\\') {
        return;
    }

    let path = Path::new(upload_dir).join(COVER_DIR_NAME).join(filename);
    if let Err(e) = fs::remove_file(path).await {
        if e.kind() != ErrorKind::NotFound {
            error!("Failed to remove previous room cover: {}", e);
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
    auth_user: CurrentUser,
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

    if !auth_user.is_admin() {
        let owned = live_room::Entity::find()
            .filter(live_room::Column::StreamId.eq(&stream_id))
            .filter(live_room::Column::UserId.eq(auth_user.user_id))
            .one(&state.db)
            .await;
        match owned {
            Ok(Some(_)) => {}
            Ok(None) => {
                return (
                    StatusCode::FORBIDDEN,
                    error_response(403, "you can only query your own streams"),
                );
            }
            Err(e) => {
                error!("Failed to load live room for stream status: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_response(500, "failed to query stream status"),
                );
            }
        }
    }

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
        .filter(live_session::Column::Status.eq("active"))
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
    let viewer_counts = state.live_hub.viewer_counts(&stream_ids).await;
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

    let rooms = build_public_live_rooms(streams, sessions, rooms, viewer_counts);
    (StatusCode::OK, success_response(rooms))
}

fn build_public_live_rooms(
    streams: Vec<SrsStream>,
    sessions: Vec<live_session::Model>,
    rooms: Vec<live_room::Model>,
    viewer_counts: HashMap<String, usize>,
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
            let viewer_count = viewer_counts.get(&stream.name).copied().unwrap_or(0);
            PublicLiveRoom {
                title: room
                    .map(|room| public_room_title(&room.title, &stream.name))
                    .unwrap_or_else(|| stream.name.clone()),
                cover_url: room.map(|room| room.cover_url.clone()).unwrap_or_default(),
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
                require_login: room.map(|room| room.require_login).unwrap_or(false),
                has_password: room
                    .map(|room| !room.password_hash.is_empty())
                    .unwrap_or(false),
                viewer_count,
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
    use std::collections::HashMap;

    use super::*;
    use crate::auth::hash_password;
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
            cover_url: String::new(),
            stream_code: "stream-code".to_string(),
            enabled: true,
            require_login: false,
            password_hash: String::new(),
            access_revision: 0,
            created_at: now,
            updated_at: now,
        }
    }

    fn live_room_model_for_user(
        id: i32,
        user_id: i32,
        stream_id: &str,
        title: &str,
    ) -> live_room::Model {
        live_room::Model {
            id,
            user_id,
            ..live_room_model(stream_id, title)
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
            HashMap::new(),
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
            HashMap::new(),
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
            HashMap::new(),
        );

        assert_eq!(rooms[0].title, "dawu");
    }

    #[test]
    fn public_live_rooms_keep_private_rooms_and_use_hub_viewer_counts() {
        let started_at =
            NaiveDateTime::parse_from_str("2026-06-01 12:00:00", "%F %T").expect("valid timestamp");
        let mut room = live_room_model("private-room", "Private room");
        room.require_login = true;
        room.password_hash = hash_password("secret1");
        let mut stream = srs_stream("private-room", 60_000);
        stream.clients = 99;

        let rooms = build_public_live_rooms(
            vec![stream],
            vec![live_session("private-room", started_at)],
            vec![room],
            HashMap::from([("private-room".to_string(), 2)]),
        );

        assert_eq!(rooms.len(), 1);
        assert!(rooms[0].require_login);
        assert!(rooms[0].has_password);
        assert_eq!(rooms[0].viewer_count, 2);
    }

    #[test]
    fn own_rooms_include_stream_codes_and_live_status() {
        let rooms = build_own_live_room_responses(
            vec![
                live_room_model_for_user(1, 7, "default", "默认"),
                live_room_model_for_user(2, 7, "extra", "额外"),
            ],
            vec![live_session(
                "extra",
                NaiveDateTime::parse_from_str("2026-06-01 12:00:00", "%F %T")
                    .expect("valid timestamp"),
            )],
            "alice",
        );

        assert_eq!(rooms.len(), 2);
        assert_eq!(rooms[0].username, "alice");
        assert_eq!(rooms[0].stream_code, "stream-code");
        assert!(!rooms[0].require_login);
        assert!(!rooms[0].has_password);
        assert_eq!(rooms[0].status, "offline");
        assert_eq!(rooms[1].stream_id, "extra");
        assert_eq!(rooms[1].status, "live");
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

    #[test]
    fn cover_image_extension_detects_supported_formats_by_header() {
        assert_eq!(
            cover_image_extension(&[0xFF, 0xD8, 0xFF, 0xE0]),
            Some("jpg")
        );
        assert_eq!(
            cover_image_extension(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]),
            Some("png")
        );
        assert_eq!(cover_image_extension(b"RIFFxxxxWEBPVP8 "), Some("webp"));
    }

    #[test]
    fn cover_image_extension_rejects_unknown_formats() {
        assert_eq!(cover_image_extension(b"not an image"), None);
    }

    fn mock_state(db: sea_orm::DatabaseConnection) -> Arc<AppState> {
        use crate::config::{
            AppConfig, DbConfig, MetricsConfig, PlaybackConfig, PublishConfig, SrsConfig,
            StorageConfig, UserConfig,
        };
        use crate::live_hub::LiveHub;
        use crate::srs_client::SrsClient;

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
                    protocols: "flv".to_string(),
                },
                publish: PublishConfig {
                    protocols: "rtmp".to_string(),
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
            live_hub: Arc::new(LiveHub::new()),
        })
    }

    fn viewer(user_id: i32, role: &str) -> CurrentUser {
        CurrentUser {
            username: format!("user-{}", user_id),
            user_id,
            role: role.to_string(),
        }
    }

    #[tokio::test]
    async fn stream_status_rejects_users_who_do_not_own_the_stream() {
        use sea_orm::{DbBackend, MockDatabase};

        let state = mock_state(
            MockDatabase::new(DbBackend::Postgres)
                .append_query_results([Vec::<live_room::Model>::new()])
                .into_connection(),
        );

        let response = stream_status(
            State(state),
            viewer(2, "user"),
            Query(StreamStatusQuery {
                stream: Some("dawu".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn stream_status_requires_a_stream_parameter() {
        use sea_orm::{DbBackend, MockDatabase};

        let state = mock_state(MockDatabase::new(DbBackend::Postgres).into_connection());

        let response = stream_status(
            State(state),
            viewer(1, "admin"),
            Query(StreamStatusQuery { stream: None }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn stop_stream_ignores_ended_sessions_and_authorizes_against_the_room() {
        use sea_orm::{DbBackend, MockDatabase};

        // An ended session owned by user 2 exists, but the active-session filter must skip it,
        // so authorization falls through to the room, which is owned by user 1.
        let state = mock_state(
            MockDatabase::new(DbBackend::Postgres)
                .append_query_results([Vec::<live_session::Model>::new()])
                .append_query_results([vec![live_room_model("dawu", "大雾的游戏时间")]])
                .into_connection(),
        );

        let response = stop_stream(
            State(state.clone()),
            viewer(2, "user"),
            Json(StopStreamRequest {
                stream_id: "dawu".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let state = Arc::try_unwrap(state).unwrap_or_else(|_| panic!("state should be unique"));
        let session_query = format!(
            "{:?}",
            state
                .db
                .into_transaction_log()
                .first()
                .expect("the session lookup should be recorded")
        );
        assert!(
            session_query.contains("\"active\""),
            "the session lookup must bind the active status, got: {session_query}"
        );
    }
}
