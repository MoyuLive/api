use axum::{
    extract::{
        rejection::JsonRejection,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{SecondsFormat, Utc};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, sync::Arc, time::Instant};
use tokio::sync::broadcast::error::RecvError;
use tracing::error;

use crate::auth::{self, CurrentUser};
use crate::danmaku::{ClientMessage, ConnectionRateLimiter, DanmakuError};
use crate::entities::{live_room, live_session, user};
use crate::live_hub::RoomEvent;
use crate::response::{error_response, success_response};
use crate::room_access::{
    admit_room_ticket_with_account_check, evaluate_room_policy, guest_display_name,
    issue_room_ticket, normalize_guest_id, RoomAccessError, RoomTicketClaims, ViewerIdentity,
    ViewerKind,
};
use crate::room_privacy::{
    update_room_with_privacy_locked, LockedRoomUpdate, RoomPrivacyUpdateError, RoomUpdateActor,
};
use crate::AppState;

const MIN_PASSWORD_CHARS: usize = 6;
const MAX_PASSWORD_CHARS: usize = 64;

#[derive(Serialize)]
pub struct PublicRoomMetadata {
    pub stream_id: String,
    pub title: String,
    pub cover_url: String,
    pub status: String,
    pub require_login: bool,
    pub has_password: bool,
    pub viewer_count: usize,
}

#[derive(Deserialize)]
pub struct RoomAccessRequest {
    pub guest_id: String,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Serialize)]
pub struct RoomAccessResponse {
    pub ticket: String,
    pub expires_at: String,
    pub viewer: ViewerIdentity,
}

#[derive(Deserialize)]
pub struct RoomWsQuery {
    #[serde(default)]
    pub ticket: String,
}

#[derive(Deserialize)]
pub struct UpdateRoomPrivacyRequest {
    pub require_login: bool,
    pub password_enabled: bool,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Serialize)]
pub struct RoomPrivacyResponse {
    pub require_login: bool,
    pub has_password: bool,
}

// GET /api/live/rooms/:stream_id
pub async fn metadata(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
) -> impl IntoResponse {
    let room = match load_room(&state, &stream_id).await {
        Ok(room) => room,
        Err(response) => return response,
    };

    let active_session = match live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&room.stream_id))
        .filter(live_session::Column::Status.eq("active"))
        .one(&state.db)
        .await
    {
        Ok(session) => session.is_some(),
        Err(e) => {
            error!("Failed to load public room live session: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to load room metadata"),
            );
        }
    };

    let metadata = PublicRoomMetadata {
        stream_id: room.stream_id.clone(),
        title: public_room_title(&room.title, &room.stream_id),
        cover_url: room.cover_url,
        status: if room.enabled && active_session {
            "live".to_string()
        } else {
            "offline".to_string()
        },
        require_login: room.require_login,
        has_password: !room.password_hash.is_empty(),
        viewer_count: state.live_hub.viewer_count(&room.stream_id).await,
    };

    (StatusCode::OK, success_response(metadata))
}

// POST /api/live/rooms/:stream_id/access
pub async fn access(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
    headers: HeaderMap,
    request: Result<Json<RoomAccessRequest>, JsonRejection>,
) -> impl IntoResponse {
    let room = match load_room(&state, &stream_id).await {
        Ok(room) => room,
        Err(response) => return response,
    };

    if !room.enabled {
        return (
            StatusCode::FORBIDDEN,
            error_response(403, "room is disabled"),
        );
    }

    let request = match request {
        Ok(Json(request)) => request,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                error_response(400, "invalid room access request"),
            );
        }
    };

    let current_user = match optional_current_user(&headers, &state).await {
        Ok(current_user) => current_user,
        Err(response) => return response,
    };

    let (viewer_key, viewer, user_id, account_verified) = match current_user {
        Some(current_user) => (
            format!("user:{}", current_user.user_id),
            ViewerIdentity {
                kind: ViewerKind::User,
                name: current_user.username,
            },
            Some(current_user.user_id),
            true,
        ),
        None => {
            let guest_id = match normalize_guest_id(&request.guest_id) {
                Ok(guest_id) => guest_id,
                Err(error) => return room_access_error_response(error),
            };
            let viewer = ViewerIdentity {
                kind: ViewerKind::Guest,
                name: guest_display_name(&guest_id),
            };
            (format!("guest:{guest_id}"), viewer, None, false)
        }
    };

    let has_password = !room.password_hash.is_empty();
    let password_verified = if has_password {
        let password = match request.password.as_deref() {
            Some(password) if is_valid_access_password(password) => password,
            Some(_) => return room_access_error_response(RoomAccessError::MalformedPassword),
            None => return room_access_error_response(RoomAccessError::PasswordDenied),
        };
        match auth::verify_password(&room.password_hash, password) {
            Ok(verified) => verified,
            Err(_) => return room_access_error_response(RoomAccessError::Internal),
        }
    } else {
        false
    };

    if let Err(error) = evaluate_room_policy(
        room.require_login,
        has_password,
        account_verified,
        password_verified,
    ) {
        return room_access_error_response(error);
    }

    let issued = match issue_room_ticket(
        &room,
        viewer_key,
        viewer,
        user_id,
        account_verified,
        password_verified,
        &state.config.user.auth_secret,
        Utc::now(),
    ) {
        Ok(issued) => issued,
        Err(error) => return room_access_error_response(error),
    };

    (
        StatusCode::OK,
        success_response(RoomAccessResponse {
            ticket: issued.token,
            expires_at: issued.expires_at.to_rfc3339(),
            viewer: issued.viewer,
        }),
    )
}

// GET /api/live/rooms/:stream_id/ws
pub async fn websocket(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
    Query(query): Query<RoomWsQuery>,
) -> Response {
    ws.on_upgrade(move |socket| handle_websocket(socket, state, stream_id, query))
}

async fn handle_websocket(
    mut socket: WebSocket,
    state: Arc<AppState>,
    stream_id: String,
    query: RoomWsQuery,
) {
    let room = match live_room::Entity::find()
        .filter(live_room::Column::StreamId.eq(&stream_id))
        .one(&state.db)
        .await
    {
        Ok(Some(room)) if room.enabled => room,
        Ok(_) | Err(_) => {
            close_room_access_denied(&mut socket).await;
            return;
        }
    };
    let claims = match admit_websocket_ticket(
        &state.db,
        &query.ticket,
        &stream_id,
        &room,
        &state.config.user.auth_secret,
        Utc::now(),
    )
    .await
    {
        Ok(claims) => claims,
        Err(()) => {
            close_room_access_denied(&mut socket).await;
            return;
        }
    };

    let (viewer_count, mut events) = state.live_hub.subscribe(&stream_id).await;
    if !send_room_event(&mut socket, &initial_viewer_count_event(viewer_count)).await {
        return;
    }

    let mut rate_limiter = ConnectionRateLimiter::new();
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let Some(incoming) = incoming else {
                    return;
                };
                match incoming {
                    Ok(Message::Text(payload)) => {
                        let result = serde_json::from_str::<ClientMessage>(&payload)
                            .map_err(|_| DanmakuError::InvalidMessage)
                            .and_then(|message| match message {
                                ClientMessage::SendMessage { content } => rate_limiter.accept_message(
                                    &claims,
                                    &content,
                                    Instant::now(),
                                    auth::generate_random_string(16),
                                    Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true),
                                ),
                            });
                        match result {
                            Ok(event) => state.live_hub.broadcast_danmaku(&stream_id, event).await,
                            Err(error) if !send_danmaku_error(&mut socket, error).await => return,
                            Err(_) => {}
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        if !send_danmaku_error(&mut socket, DanmakuError::InvalidMessage).await {
                            return;
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            return;
                        }
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) | Err(_) => return,
                }
            }
            event = events.recv() => match event {
                Ok(event) if !send_room_event(&mut socket, &event).await => return,
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return,
            },
        }
    }
}

async fn admit_websocket_ticket(
    db: &DatabaseConnection,
    ticket: &str,
    stream_id: &str,
    room: &live_room::Model,
    auth_secret: &str,
    now: chrono::DateTime<Utc>,
) -> Result<RoomTicketClaims, ()> {
    if !room.enabled {
        return Err(());
    }
    admit_room_ticket_with_account_check(db, ticket, stream_id, room, auth_secret, now)
        .await
        .map_err(|_| ())
}

fn room_access_denied_close_frame() -> CloseFrame<'static> {
    CloseFrame {
        code: 1008,
        reason: Cow::Borrowed("room access denied"),
    }
}

async fn close_room_access_denied(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(room_access_denied_close_frame())))
        .await;
}

fn initial_viewer_count_event(count: usize) -> RoomEvent {
    RoomEvent::ViewerCount { count }
}

async fn send_room_event(socket: &mut WebSocket, event: &RoomEvent) -> bool {
    let Ok(payload) = serde_json::to_string(event) else {
        return false;
    };
    socket.send(Message::Text(payload)).await.is_ok()
}

#[derive(Serialize)]
struct DanmakuErrorEvent {
    #[serde(rename = "type")]
    event_type: &'static str,
    code: &'static str,
    message: &'static str,
}

async fn send_danmaku_error(socket: &mut WebSocket, error: DanmakuError) -> bool {
    let (code, message) = match error {
        DanmakuError::RateLimited => ("rate_limited", "发送太快"),
        DanmakuError::InvalidMessage => ("invalid_message", "弹幕必须为 1-100 个字符"),
    };
    let Ok(payload) = serde_json::to_string(&DanmakuErrorEvent {
        event_type: "error",
        code,
        message,
    }) else {
        return false;
    };
    socket.send(Message::Text(payload)).await.is_ok()
}

// PUT /api/live/rooms/:id/privacy
pub async fn update_owned_privacy(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
    request: Result<Json<UpdateRoomPrivacyRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(request) = match request {
        Ok(request) => request,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                error_response(400, "invalid room privacy request"),
            );
        }
    };
    let patch = LockedRoomUpdate {
        require_login: Some(request.require_login),
        password_enabled: Some(request.password_enabled),
        password: request.password,
        ..Default::default()
    };
    match update_room_with_privacy_locked(
        &state.db,
        id,
        RoomUpdateActor::Owner {
            user_id: auth_user.user_id,
        },
        patch,
        Utc::now(),
    )
    .await
    {
        Ok(room) => (
            StatusCode::OK,
            success_response(RoomPrivacyResponse {
                require_login: room.require_login,
                has_password: !room.password_hash.is_empty(),
            }),
        ),
        Err(RoomPrivacyUpdateError::NotFound) => {
            (StatusCode::NOT_FOUND, error_response(404, "room not found"))
        }
        Err(RoomPrivacyUpdateError::Forbidden) => (
            StatusCode::FORBIDDEN,
            error_response(403, "you can only manage your own live rooms"),
        ),
        Err(RoomPrivacyUpdateError::Invalid(RoomAccessError::MalformedPassword)) => (
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid password"),
        ),
        Err(RoomPrivacyUpdateError::Invalid(_)) => {
            error!("Failed to update room privacy");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update room privacy"),
            )
        }
        Err(RoomPrivacyUpdateError::Database(database_error)) => {
            let _ = database_error;
            error!("Failed to update room privacy");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update room privacy"),
            )
        }
    }
}

async fn load_room(
    state: &Arc<AppState>,
    stream_id: &str,
) -> Result<live_room::Model, (StatusCode, Response)> {
    match live_room::Entity::find()
        .filter(live_room::Column::StreamId.eq(stream_id))
        .one(&state.db)
        .await
    {
        Ok(Some(room)) => Ok(room),
        Ok(None) => Err((StatusCode::NOT_FOUND, error_response(404, "room not found"))),
        Err(e) => {
            error!("Failed to load public room: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to load room"),
            ))
        }
    }
}

async fn optional_current_user(
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<Option<CurrentUser>, (StatusCode, Response)> {
    let Some(authorization) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let token = authorization
        .to_str()
        .ok()
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty() && !token.chars().any(char::is_whitespace))
        .ok_or_else(unauthorized_response)?;
    let claims = auth::decode_jwt(token, &state.config.user.auth_secret)
        .map_err(|_| unauthorized_response())?;

    let db_user = user::Entity::find_by_id(claims.user_id)
        .one(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to load optional room access account: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to load account"),
            )
        })?;
    let db_user = db_user
        .filter(|user| user.enabled)
        .ok_or_else(unauthorized_response)?;

    Ok(Some(CurrentUser {
        username: db_user.username,
        user_id: db_user.id,
        role: db_user.role,
    }))
}

fn is_valid_access_password(password: &str) -> bool {
    (MIN_PASSWORD_CHARS..=MAX_PASSWORD_CHARS).contains(&password.chars().count())
}

#[allow(clippy::result_large_err)]
fn unauthorized_response() -> (StatusCode, Response) {
    (
        StatusCode::UNAUTHORIZED,
        error_response(401, "invalid authorization"),
    )
}

fn room_access_error_response(error: RoomAccessError) -> (StatusCode, Response) {
    match error {
        RoomAccessError::MalformedGuestId => (
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid guest id"),
        ),
        RoomAccessError::MalformedPassword => (
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid password"),
        ),
        RoomAccessError::AccountRequired => (
            StatusCode::UNAUTHORIZED,
            error_response(401, "account required"),
        ),
        RoomAccessError::PasswordDenied | RoomAccessError::StalePolicy => (
            StatusCode::FORBIDDEN,
            error_response(403, "room access denied"),
        ),
        RoomAccessError::InvalidTicket
        | RoomAccessError::ExpiredTicket
        | RoomAccessError::WrongRoom => (
            StatusCode::UNAUTHORIZED,
            error_response(401, "invalid room ticket"),
        ),
        RoomAccessError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to issue room access"),
        ),
    }
}

fn public_room_title(title: &str, fallback: &str) -> String {
    let title = title.trim();
    if title.is_empty() {
        fallback.to_string()
    } else {
        title.to_string()
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        extract::{FromRequest, Path, State},
        http::{HeaderMap, HeaderValue, Request as HttpRequest, StatusCode},
        response::IntoResponse,
        Json,
    };
    use chrono::{NaiveDateTime, Utc};
    use sea_orm::{ActiveModelTrait, Database, DbBackend, EntityTrait, MockDatabase, Set};
    use std::sync::Arc;

    use super::*;
    use crate::auth::{create_jwt, generate_random_string, hash_password};
    use crate::config::{
        AppConfig, DbConfig, MetricsConfig, PlaybackConfig, PublishConfig, SrsConfig,
        StorageConfig, UserConfig,
    };
    use crate::entities::{live_room, live_session, user};
    use crate::live_hub::LiveHub;
    use crate::srs_client::SrsClient;
    use crate::AppState;

    const AUTH_SECRET: &str = "room-handler-test-secret";
    const GUEST_ID: &str = "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB";

    fn test_state(db: sea_orm::DatabaseConnection, live_hub: Arc<LiveHub>) -> Arc<AppState> {
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
                    auth_secret: AUTH_SECRET.to_string(),
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

    fn room_model(stream_id: &str) -> live_room::Model {
        let now =
            NaiveDateTime::parse_from_str("2026-07-20 12:00:00", "%F %T").expect("valid time");
        live_room::Model {
            id: 7,
            user_id: 42,
            stream_id: stream_id.to_string(),
            title: "Room title".to_string(),
            cover_url: "/uploads/covers/room.jpg".to_string(),
            stream_code: "stream-code".to_string(),
            enabled: true,
            require_login: false,
            password_hash: String::new(),
            access_revision: 3,
            created_at: now,
            updated_at: now,
        }
    }

    fn active_session(stream_id: &str) -> live_session::Model {
        live_session::Model {
            id: 1,
            stream_id: stream_id.to_string(),
            app: "live".to_string(),
            vhost: "__defaultVhost__".to_string(),
            user_id: 42,
            client_id: "publisher".to_string(),
            server_id: "srs-1".to_string(),
            stream_url: format!("rtmp://localhost/live/{stream_id}"),
            status: "active".to_string(),
            video_codec: String::new(),
            audio_codec: String::new(),
            video_width: 0,
            video_height: 0,
            started_at: NaiveDateTime::parse_from_str("2026-07-20 12:00:00", "%F %T")
                .expect("valid time"),
            ended_at: None,
        }
    }

    fn user_model(enabled: bool) -> user::Model {
        user::Model {
            id: 42,
            username: "database-user".to_string(),
            password: "hash".to_string(),
            stream_code: "stream-code".to_string(),
            room_title: "Room title".to_string(),
            role: "user".to_string(),
            enabled,
        }
    }

    fn db_with_rooms(
        rooms: impl IntoIterator<Item = live_room::Model>,
    ) -> sea_orm::DatabaseConnection {
        MockDatabase::new(DbBackend::Postgres)
            .append_query_results([rooms])
            .into_connection()
    }

    fn guest_request(password: Option<&str>) -> RoomAccessRequest {
        RoomAccessRequest {
            guest_id: GUEST_ID.to_string(),
            password: password.map(str::to_string),
        }
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).expect("valid bearer header"),
        );
        headers
    }

    async fn response_json(response: impl IntoResponse) -> (StatusCode, serde_json::Value) {
        let response = response.into_response();
        let status = response.status();
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("response body should be readable");
        (
            status,
            serde_json::from_slice(&body).expect("response should be json"),
        )
    }

    #[tokio::test]
    async fn websocket_invalid_ticket_uses_policy_violation_close_without_leaking_details() {
        let room = room_model("room-one");
        let db = MockDatabase::new(DbBackend::Postgres).into_connection();

        assert!(admit_websocket_ticket(
            &db,
            "invalid-ticket",
            "room-one",
            &room,
            AUTH_SECRET,
            Utc::now(),
        )
        .await
        .is_err());
        let close_frame = room_access_denied_close_frame();
        assert_eq!(close_frame.code, 1008);
        assert_eq!(close_frame.reason, "room access denied");
    }

    #[tokio::test]
    async fn websocket_admission_rechecks_disabled_ticket_account() {
        let room = room_model("room-one");
        let ticket = issue_room_ticket(
            &room,
            "user:42".to_string(),
            ViewerIdentity {
                kind: ViewerKind::User,
                name: "database-user".to_string(),
            },
            Some(42),
            true,
            false,
            AUTH_SECRET,
            Utc::now(),
        )
        .expect("account ticket should be issued");
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[user_model(false)]])
            .into_connection();

        assert!(admit_websocket_ticket(
            &db,
            &ticket.token,
            &room.stream_id,
            &room,
            AUTH_SECRET,
            Utc::now(),
        )
        .await
        .is_err());
    }

    #[test]
    fn websocket_initial_event_is_the_current_viewer_count() {
        assert_eq!(
            initial_viewer_count_event(3),
            RoomEvent::ViewerCount { count: 3 }
        );
    }

    #[test]
    fn owner_privacy_request_requires_both_switches() {
        for body in [
            r#"{}"#,
            r#"{"require_login":true}"#,
            r#"{"password_enabled":false}"#,
        ] {
            assert!(
                serde_json::from_str::<UpdateRoomPrivacyRequest>(body).is_err(),
                "owner privacy request should reject {body}"
            );
        }

        assert!(serde_json::from_str::<UpdateRoomPrivacyRequest>(
            r#"{"require_login":true,"password_enabled":false}"#
        )
        .is_ok());
    }

    #[tokio::test]
    async fn owner_privacy_handler_maps_missing_switches_and_empty_body_to_bad_request() {
        for body in [
            "",
            r#"{}"#,
            r#"{"require_login":true}"#,
            r#"{"password_enabled":false}"#,
        ] {
            let request = HttpRequest::builder()
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("test request should build");
            let extraction = Json::<UpdateRoomPrivacyRequest>::from_request(request, &()).await;
            assert!(
                extraction.is_err(),
                "privacy extraction should reject {body}"
            );

            let (status, json) = response_json(
                update_owned_privacy(
                    State(test_state(
                        MockDatabase::new(DbBackend::Postgres).into_connection(),
                        Arc::new(LiveHub::new()),
                    )),
                    CurrentUser {
                        username: "owner".to_string(),
                        user_id: 42,
                        role: "user".to_string(),
                    },
                    Path(7),
                    extraction,
                )
                .await,
            )
            .await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(json["code"], 400);
        }
    }

    #[tokio::test]
    async fn metadata_returns_not_found_for_unknown_room() {
        let state = test_state(db_with_rooms([]), Arc::new(LiveHub::new()));

        let (status, json) =
            response_json(metadata(State(state), Path("unknown".to_string())).await).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json["code"], 404);
    }

    #[tokio::test]
    async fn metadata_returns_public_flags_hub_count_title_fallback_and_live_status() {
        let mut room = room_model("room-one");
        room.title = "   ".to_string();
        room.require_login = true;
        room.password_hash = hash_password("secret1");
        let hub = Arc::new(LiveHub::new());
        hub.play("room-one", "client-a", "guest:a").await;
        hub.play("room-one", "client-b", "guest:b").await;
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[room]])
            .append_query_results([[active_session("room-one")]])
            .into_connection();

        let (status, json) =
            response_json(metadata(State(test_state(db, hub)), Path("room-one".to_string())).await)
                .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["data"]["stream_id"], "room-one");
        assert_eq!(json["data"]["title"], "room-one");
        assert_eq!(json["data"]["status"], "live");
        assert_eq!(json["data"]["require_login"], true);
        assert_eq!(json["data"]["has_password"], true);
        assert_eq!(json["data"]["viewer_count"], 2);
        assert!(json["data"].get("password_hash").is_none());
    }

    #[tokio::test]
    async fn disabled_room_metadata_is_offline_but_access_is_forbidden() {
        let mut room = room_model("disabled-room");
        room.enabled = false;
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[room.clone()]])
            .append_query_results([[active_session("disabled-room")]])
            .into_connection();
        let (status, json) = response_json(
            metadata(
                State(test_state(db, Arc::new(LiveHub::new()))),
                Path("disabled-room".to_string()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["data"]["status"], "offline");

        let access_db = db_with_rooms([room]);
        let (status, json) = response_json(
            access(
                State(test_state(access_db, Arc::new(LiveHub::new()))),
                Path("disabled-room".to_string()),
                HeaderMap::new(),
                Ok(Json(guest_request(None))),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(json["code"], 403);
    }

    #[tokio::test]
    async fn access_allows_public_guest_and_issues_guest_ticket() {
        let state = test_state(
            db_with_rooms([room_model("room-one")]),
            Arc::new(LiveHub::new()),
        );

        let (status, json) = response_json(
            access(
                State(state),
                Path("room-one".to_string()),
                HeaderMap::new(),
                Ok(Json(guest_request(None))),
            )
            .await,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(json["data"]["ticket"]
            .as_str()
            .is_some_and(|ticket| !ticket.is_empty()));
        assert!(json["data"]["expires_at"].as_str().is_some());
        assert_eq!(json["data"]["viewer"]["kind"], "guest");
        assert_eq!(json["data"]["viewer"]["name"], "游客-ABCD");
    }

    #[tokio::test]
    async fn access_requires_login_without_a_jwt() {
        let mut room = room_model("login-only");
        room.require_login = true;
        let state = test_state(db_with_rooms([room]), Arc::new(LiveHub::new()));

        let (status, json) = response_json(
            access(
                State(state),
                Path("login-only".to_string()),
                HeaderMap::new(),
                Ok(Json(guest_request(None))),
            )
            .await,
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(json["code"], 401);
    }

    #[tokio::test]
    async fn password_only_room_rejects_missing_and_incorrect_password() {
        let mut room = room_model("password-only");
        room.password_hash = hash_password("secret1");

        for password in [None, Some("incorrect")] {
            let state = test_state(db_with_rooms([room.clone()]), Arc::new(LiveHub::new()));
            let (status, json) = response_json(
                access(
                    State(state),
                    Path("password-only".to_string()),
                    HeaderMap::new(),
                    Ok(Json(guest_request(password))),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert_eq!(json["code"], 403);
        }
    }

    #[tokio::test]
    async fn access_rejects_malformed_password() {
        let mut room = room_model("password-only");
        room.password_hash = hash_password("secret1");
        let state = test_state(db_with_rooms([room]), Arc::new(LiveHub::new()));

        let (status, json) = response_json(
            access(
                State(state),
                Path("password-only".to_string()),
                HeaderMap::new(),
                Ok(Json(guest_request(Some("short")))),
            )
            .await,
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["code"], 400);
    }

    #[tokio::test]
    async fn access_rejects_malformed_guest_id() {
        let state = test_state(
            db_with_rooms([room_model("room-one")]),
            Arc::new(LiveHub::new()),
        );
        let request = RoomAccessRequest {
            guest_id: "not-a-guest-id".to_string(),
            password: None,
        };

        let (status, json) = response_json(
            access(
                State(state),
                Path("room-one".to_string()),
                HeaderMap::new(),
                Ok(Json(request)),
            )
            .await,
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["code"], 400);
    }

    #[tokio::test]
    async fn valid_logged_in_user_uses_database_identity_and_ignores_malformed_guest_id() {
        let token = create_jwt(42, "stale-claim", "admin", AUTH_SECRET).expect("valid token");
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[room_model("room-one")]])
            .append_query_results([[user_model(true)]])
            .into_connection();
        let request = RoomAccessRequest {
            guest_id: "malformed-but-ignored".to_string(),
            password: None,
        };

        let (status, json) = response_json(
            access(
                State(test_state(db, Arc::new(LiveHub::new()))),
                Path("room-one".to_string()),
                bearer(&token),
                Ok(Json(request)),
            )
            .await,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["data"]["viewer"]["kind"], "user");
        assert_eq!(json["data"]["viewer"]["name"], "database-user");
        let ticket = json["data"]["ticket"].as_str().expect("ticket is returned");
        let claims = crate::room_access::admit_room_ticket(
            ticket,
            "room-one",
            &room_model("room-one"),
            AUTH_SECRET,
            Utc::now(),
        )
        .expect("ticket is signed");
        assert_eq!(claims.viewer_key, "user:42");
    }

    #[tokio::test]
    async fn access_rejects_invalid_jwt_and_disabled_account() {
        let state = test_state(
            db_with_rooms([room_model("room-one")]),
            Arc::new(LiveHub::new()),
        );
        let (status, json) = response_json(
            access(
                State(state),
                Path("room-one".to_string()),
                bearer("invalid-token"),
                Ok(Json(guest_request(None))),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(json["code"], 401);

        let token = create_jwt(42, "database-user", "user", AUTH_SECRET).expect("valid token");
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[room_model("room-one")]])
            .append_query_results([[user_model(false)]])
            .into_connection();
        let (status, json) = response_json(
            access(
                State(test_state(db, Arc::new(LiveHub::new()))),
                Path("room-one".to_string()),
                bearer(&token),
                Ok(Json(guest_request(None))),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(json["code"], 401);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn owned_privacy_update_rejects_non_owner_and_allows_owner() {
        let Ok(database_url) = std::env::var("YANTUBE_TEST_DATABASE_URL") else {
            eprintln!("skipping postgres room handler test; YANTUBE_TEST_DATABASE_URL is not set");
            return;
        };
        let db = Database::connect(&database_url)
            .await
            .expect("test database should be reachable");
        let suffix = generate_random_string(16);
        let now = Utc::now().naive_utc();
        let owner = user::ActiveModel {
            username: Set(format!("room_privacy_owner_{suffix}")),
            password: Set("fixture-password".to_string()),
            stream_code: Set("fixture-code".to_string()),
            room_title: Set(String::new()),
            role: Set("user".to_string()),
            enabled: Set(true),
            ..Default::default()
        }
        .insert(&db)
        .await
        .expect("fixture owner should be created");
        let room = live_room::ActiveModel {
            user_id: Set(owner.id),
            stream_id: Set(format!("room-privacy-owner-{suffix}")),
            title: Set("Privacy fixture".to_string()),
            cover_url: Set(String::new()),
            stream_code: Set("fixture-stream-code".to_string()),
            enabled: Set(true),
            require_login: Set(false),
            password_hash: Set(String::new()),
            access_revision: Set(0),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&db)
        .await
        .expect("fixture room should be created");
        let state_db = Database::connect(&database_url)
            .await
            .expect("handler test connection should be reachable");
        let state = test_state(state_db, Arc::new(LiveHub::new()));

        let result = async {
            let (status, json) = response_json(
                update_owned_privacy(
                    State(state.clone()),
                    CurrentUser {
                        username: "not-owner".to_string(),
                        user_id: owner.id + 1,
                        role: "user".to_string(),
                    },
                    Path(room.id),
                    Ok(Json(UpdateRoomPrivacyRequest {
                        require_login: true,
                        password_enabled: false,
                        password: None,
                    })),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert_eq!(json["code"], 403);

            let (status, json) = response_json(
                update_owned_privacy(
                    State(state),
                    CurrentUser {
                        username: owner.username.clone(),
                        user_id: owner.id,
                        role: "user".to_string(),
                    },
                    Path(room.id),
                    Ok(Json(UpdateRoomPrivacyRequest {
                        require_login: true,
                        password_enabled: false,
                        password: None,
                    })),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(json["data"]["require_login"], true);
            assert_eq!(json["data"]["has_password"], false);
            assert!(json["data"].get("password_hash").is_none());
        }
        .await;

        let _ = live_room::Entity::delete_by_id(room.id).exec(&db).await;
        let _ = user::Entity::delete_by_id(owner.id).exec(&db).await;
        result
    }
}
