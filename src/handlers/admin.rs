use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, Set, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tracing::{error, info};

use crate::auth::{generate_random_string, hash_password, normalize_role, CurrentUser};
use crate::entities::{live_room, live_session, live_stream_state, user};
use crate::handlers::account::{validate_password, validate_username};
use crate::handlers::live::normalize_room_title;
use crate::response::{error_response, success_response};
use crate::AppState;

#[derive(Debug, Serialize)]
pub struct AdminMeResponse {
    pub id: i32,
    pub username: String,
    pub role: String,
    pub is_admin: bool,
    pub is_super_admin: bool,
}

#[derive(Debug, Serialize)]
pub struct AdminUserResponse {
    pub id: i32,
    pub username: String,
    pub role: String,
    pub enabled: bool,
    pub room_count: usize,
}

#[derive(Debug, Serialize)]
pub struct AdminRoomResponse {
    pub id: i32,
    pub user_id: i32,
    pub username: String,
    pub stream_id: String,
    pub title: String,
    pub stream_code: String,
    pub enabled: bool,
    pub status: String,
    pub live_session: Option<live_session::Model>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    pub role: String,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct CreateRoomRequest {
    pub user_id: i32,
    pub stream_id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateRoomRequest {
    #[serde(default)]
    pub user_id: Option<i32>,
    #[serde(default)]
    pub stream_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[allow(clippy::result_large_err)]
fn require_admin(auth_user: &CurrentUser) -> Result<(), (StatusCode, Response)> {
    if auth_user.is_admin() {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, error_response(403, "admin required")))
    }
}

#[allow(clippy::result_large_err)]
fn require_super_admin(auth_user: &CurrentUser) -> Result<(), (StatusCode, Response)> {
    if auth_user.is_super_admin() {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            error_response(403, "super admin required"),
        ))
    }
}

#[allow(clippy::result_large_err)]
fn validate_stream_id(stream_id: &str) -> Result<String, (StatusCode, Response)> {
    let stream_id = stream_id.trim();
    if stream_id.len() < 3 || stream_id.len() > 64 {
        return Err((
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid stream id"),
        ));
    }

    if !stream_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err((
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid stream id"),
        ));
    }

    Ok(stream_id.to_string())
}

#[allow(clippy::result_large_err)]
async fn ensure_user_exists(
    db: &DatabaseConnection,
    user_id: i32,
) -> Result<user::Model, (StatusCode, Response)> {
    match user::Entity::find_by_id(user_id).one(db).await {
        Ok(Some(user)) => Ok(user),
        Ok(None) => Err((
            StatusCode::BAD_REQUEST,
            error_response(400, "user not found"),
        )),
        Err(e) => {
            error!("Failed to find user: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to find user"),
            ))
        }
    }
}

async fn has_other_enabled_super_admin(
    db: &DatabaseConnection,
    user_id: i32,
) -> Result<bool, sea_orm::DbErr> {
    let count = user::Entity::find()
        .filter(user::Column::Role.eq(crate::auth::ROLE_SUPER_ADMIN))
        .filter(user::Column::Enabled.eq(true))
        .filter(user::Column::Id.ne(user_id))
        .count(db)
        .await?;

    Ok(count > 0)
}

#[allow(clippy::result_large_err)]
async fn assert_not_last_enabled_super_admin_change(
    db: &DatabaseConnection,
    target: &user::Model,
    next_role: Option<&str>,
    next_enabled: Option<bool>,
) -> Result<(), (StatusCode, Response)> {
    let would_stop_being_enabled_super_admin = target.role == crate::auth::ROLE_SUPER_ADMIN
        && target.enabled
        && (next_role
            .map(|role| role != crate::auth::ROLE_SUPER_ADMIN)
            .unwrap_or(false)
            || next_enabled == Some(false));

    if !would_stop_being_enabled_super_admin {
        return Ok(());
    }

    match has_other_enabled_super_admin(db, target.id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err((
            StatusCode::BAD_REQUEST,
            error_response(400, "at least one enabled super admin is required"),
        )),
        Err(e) => {
            error!("Failed to check super admin guard: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to check super admin guard"),
            ))
        }
    }
}

#[allow(clippy::result_large_err)]
async fn room_response(
    db: &DatabaseConnection,
    room: live_room::Model,
) -> Result<AdminRoomResponse, (StatusCode, Response)> {
    let owner = ensure_user_exists(db, room.user_id).await?;
    let session = match live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&room.stream_id))
        .filter(live_session::Column::Status.eq("active"))
        .one(db)
        .await
    {
        Ok(session) => session,
        Err(e) => {
            error!("Failed to load room live session: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to load live session"),
            ));
        }
    };

    Ok(AdminRoomResponse {
        id: room.id,
        user_id: room.user_id,
        username: owner.username,
        stream_id: room.stream_id,
        title: room.title,
        stream_code: room.stream_code,
        enabled: room.enabled,
        status: if session.is_some() { "live" } else { "offline" }.to_string(),
        live_session: session,
        created_at: room.created_at,
        updated_at: room.updated_at,
    })
}

async fn sync_legacy_user_room_fields(db: &DatabaseConnection, room: &live_room::Model) {
    let owner = match user::Entity::find_by_id(room.user_id).one(db).await {
        Ok(Some(owner)) => owner,
        Ok(None) => return,
        Err(e) => {
            error!("Failed to load legacy room owner: {}", e);
            return;
        }
    };

    if owner.username != room.stream_id {
        return;
    }

    if let Err(e) = user::Entity::update_many()
        .filter(user::Column::Id.eq(owner.id))
        .col_expr(
            user::Column::StreamCode,
            sea_orm::sea_query::Expr::value(room.stream_code.clone()),
        )
        .col_expr(
            user::Column::RoomTitle,
            sea_orm::sea_query::Expr::value(room.title.clone()),
        )
        .exec(db)
        .await
    {
        error!("Failed to sync legacy user room fields: {}", e);
    }
}

pub async fn me(auth_user: CurrentUser) -> impl IntoResponse {
    (
        StatusCode::OK,
        success_response(AdminMeResponse {
            id: auth_user.user_id,
            username: auth_user.username.clone(),
            role: auth_user.role.clone(),
            is_admin: auth_user.is_admin(),
            is_super_admin: auth_user.is_super_admin(),
        }),
    )
}

pub async fn list_users(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    if let Err(response) = require_super_admin(&auth_user) {
        return response;
    }

    let users = match user::Entity::find()
        .order_by_asc(user::Column::Id)
        .all(&state.db)
        .await
    {
        Ok(users) => users,
        Err(e) => {
            error!("Failed to list users: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to list users"),
            );
        }
    };

    let rooms = match live_room::Entity::find().all(&state.db).await {
        Ok(rooms) => rooms,
        Err(e) => {
            error!("Failed to count user rooms: {}", e);
            Vec::new()
        }
    };
    let mut room_counts: HashMap<i32, usize> = HashMap::new();
    for room in rooms {
        *room_counts.entry(room.user_id).or_default() += 1;
    }

    let data: Vec<AdminUserResponse> = users
        .into_iter()
        .map(|user| AdminUserResponse {
            id: user.id,
            username: user.username,
            role: user.role,
            enabled: user.enabled,
            room_count: room_counts.get(&user.id).copied().unwrap_or(0),
        })
        .collect();

    (StatusCode::OK, success_response(data))
}

pub async fn create_user(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if let Err(response) = require_super_admin(&auth_user) {
        return response;
    }
    if let Err(response) = validate_username(&req.username) {
        return response;
    }
    if let Err(response) = validate_password(&req.password) {
        return response;
    }
    let Some(role) = normalize_role(&req.role) else {
        return (StatusCode::BAD_REQUEST, error_response(400, "invalid role"));
    };

    match user::Entity::find()
        .filter(user::Column::Username.eq(&req.username))
        .one(&state.db)
        .await
    {
        Ok(Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                error_response(400, "username already exists"),
            );
        }
        Ok(None) => {}
        Err(e) => {
            error!("Failed to check username duplicate: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to check username"),
            );
        }
    }

    let stream_code = generate_random_string(16);
    let txn = match state.db.begin().await {
        Ok(txn) => txn,
        Err(e) => {
            error!("Failed to begin create admin user transaction: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to create user"),
            );
        }
    };

    let active = user::ActiveModel {
        username: Set(req.username.clone()),
        password: Set(hash_password(&req.password)),
        stream_code: Set(stream_code.clone()),
        room_title: Set(String::new()),
        role: Set(role.to_string()),
        enabled: Set(req.enabled.unwrap_or(true)),
        ..Default::default()
    };

    let created = match active.insert(&txn).await {
        Ok(user) => user,
        Err(e) => {
            error!("Failed to create admin user: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to create user"),
            );
        }
    };

    let room = live_room::ActiveModel {
        user_id: Set(created.id),
        stream_id: Set(created.username.clone()),
        title: Set(String::new()),
        stream_code: Set(stream_code),
        enabled: Set(created.enabled),
        ..Default::default()
    };
    if let Err(e) = room.insert(&txn).await {
        error!("Failed to create default room for admin user: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to create user room"),
        );
    }

    if let Err(e) = txn.commit().await {
        error!("Failed to commit create admin user transaction: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to create user"),
        );
    }

    info!(user_id = created.id, "Admin user created");
    (
        StatusCode::OK,
        success_response(AdminUserResponse {
            id: created.id,
            username: created.username,
            role: created.role,
            enabled: created.enabled,
            room_count: 1,
        }),
    )
}

pub async fn update_user(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
    Json(req): Json<UpdateUserRequest>,
) -> impl IntoResponse {
    if let Err(response) = require_super_admin(&auth_user) {
        return response;
    }

    let target = match ensure_user_exists(&state.db, id).await {
        Ok(user) => user,
        Err(response) => return response,
    };

    let next_role = match req.role.as_deref() {
        Some(role) => match normalize_role(role) {
            Some(role) => Some(role),
            None => return (StatusCode::BAD_REQUEST, error_response(400, "invalid role")),
        },
        None => None,
    };

    if let Err(response) =
        assert_not_last_enabled_super_admin_change(&state.db, &target, next_role, req.enabled).await
    {
        return response;
    }

    if let Some(username) = req.username.as_deref() {
        if let Err(response) = validate_username(username) {
            return response;
        }
        match user::Entity::find()
            .filter(user::Column::Username.eq(username))
            .filter(user::Column::Id.ne(id))
            .one(&state.db)
            .await
        {
            Ok(Some(_)) => {
                return (
                    StatusCode::BAD_REQUEST,
                    error_response(400, "username already exists"),
                );
            }
            Ok(None) => {}
            Err(e) => {
                error!("Failed to check username duplicate: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_response(500, "failed to check username"),
                );
            }
        }
    }

    if let Some(password) = req.password.as_deref() {
        if let Err(response) = validate_password(password) {
            return response;
        }
    }

    let mut active: user::ActiveModel = target.into();
    if let Some(username) = req.username {
        active.username = Set(username);
    }
    if let Some(password) = req.password {
        active.password = Set(hash_password(&password));
    }
    if let Some(role) = next_role {
        active.role = Set(role.to_string());
    }
    if let Some(enabled) = req.enabled {
        active.enabled = Set(enabled);
    }

    match active.update(&state.db).await {
        Ok(updated) => (
            StatusCode::OK,
            success_response(AdminUserResponse {
                id: updated.id,
                username: updated.username,
                role: updated.role,
                enabled: updated.enabled,
                room_count: live_room::Entity::find()
                    .filter(live_room::Column::UserId.eq(updated.id))
                    .count(&state.db)
                    .await
                    .unwrap_or(0) as usize,
            }),
        ),
        Err(e) => {
            error!("Failed to update user: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update user"),
            )
        }
    }
}

pub async fn delete_user(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
) -> impl IntoResponse {
    if let Err(response) = require_super_admin(&auth_user) {
        return response;
    }

    let target = match ensure_user_exists(&state.db, id).await {
        Ok(user) => user,
        Err(response) => return response,
    };

    if let Err(response) =
        assert_not_last_enabled_super_admin_change(&state.db, &target, Some("user"), Some(false))
            .await
    {
        return response;
    }

    let active_count = match live_session::Entity::find()
        .filter(live_session::Column::UserId.eq(id))
        .filter(live_session::Column::Status.eq("active"))
        .count(&state.db)
        .await
    {
        Ok(count) => count,
        Err(e) => {
            error!(
                "Failed to check active sessions before deleting user: {}",
                e
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to delete user"),
            );
        }
    };
    if active_count > 0 {
        return (
            StatusCode::BAD_REQUEST,
            error_response(400, "stop active streams before deleting user"),
        );
    }

    let txn = match state.db.begin().await {
        Ok(txn) => txn,
        Err(e) => {
            error!("Failed to begin delete user transaction: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to delete user"),
            );
        }
    };

    if let Err(e) = live_session::Entity::delete_many()
        .filter(live_session::Column::UserId.eq(id))
        .exec(&txn)
        .await
    {
        error!("Failed to delete user sessions: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to delete user"),
        );
    }
    if let Err(e) = live_stream_state::Entity::delete_many()
        .filter(live_stream_state::Column::UserId.eq(id))
        .exec(&txn)
        .await
    {
        error!("Failed to delete user stream states: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to delete user"),
        );
    }
    if let Err(e) = live_room::Entity::delete_many()
        .filter(live_room::Column::UserId.eq(id))
        .exec(&txn)
        .await
    {
        error!("Failed to delete user rooms: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to delete user"),
        );
    }
    if let Err(e) = user::Entity::delete_by_id(id).exec(&txn).await {
        error!("Failed to delete user: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to delete user"),
        );
    }
    if let Err(e) = txn.commit().await {
        error!("Failed to commit delete user transaction: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "failed to delete user"),
        );
    }

    (
        StatusCode::OK,
        success_response(serde_json::json!({"deleted": true})),
    )
}

pub async fn list_rooms(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    if let Err(response) = require_admin(&auth_user) {
        return response;
    }

    let rooms = match live_room::Entity::find()
        .order_by_asc(live_room::Column::Id)
        .all(&state.db)
        .await
    {
        Ok(rooms) => rooms,
        Err(e) => {
            error!("Failed to list rooms: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to list rooms"),
            );
        }
    };

    let user_ids: Vec<i32> = rooms.iter().map(|room| room.user_id).collect();
    let users = match user::Entity::find()
        .filter(user::Column::Id.is_in(user_ids))
        .all(&state.db)
        .await
    {
        Ok(users) => users,
        Err(e) => {
            error!("Failed to load room owners: {}", e);
            Vec::new()
        }
    };
    let users_by_id: HashMap<i32, user::Model> =
        users.into_iter().map(|user| (user.id, user)).collect();

    let stream_ids: Vec<String> = rooms.iter().map(|room| room.stream_id.clone()).collect();
    let sessions = match live_session::Entity::find()
        .filter(live_session::Column::StreamId.is_in(stream_ids))
        .filter(live_session::Column::Status.eq("active"))
        .all(&state.db)
        .await
    {
        Ok(sessions) => sessions,
        Err(e) => {
            error!("Failed to load active room sessions: {}", e);
            Vec::new()
        }
    };
    let sessions_by_stream_id: HashMap<String, live_session::Model> = sessions
        .into_iter()
        .map(|session| (session.stream_id.clone(), session))
        .collect();

    let data: Vec<AdminRoomResponse> = rooms
        .into_iter()
        .map(|room| {
            let session = sessions_by_stream_id.get(&room.stream_id).cloned();
            AdminRoomResponse {
                id: room.id,
                user_id: room.user_id,
                username: users_by_id
                    .get(&room.user_id)
                    .map(|user| user.username.clone())
                    .unwrap_or_default(),
                stream_id: room.stream_id,
                title: room.title,
                stream_code: room.stream_code,
                enabled: room.enabled,
                status: if session.is_some() { "live" } else { "offline" }.to_string(),
                live_session: session,
                created_at: room.created_at,
                updated_at: room.updated_at,
            }
        })
        .collect();

    (StatusCode::OK, success_response(data))
}

pub async fn create_room(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Json(req): Json<CreateRoomRequest>,
) -> impl IntoResponse {
    if let Err(response) = require_super_admin(&auth_user) {
        return response;
    }

    let owner = match ensure_user_exists(&state.db, req.user_id).await {
        Ok(user) => user,
        Err(response) => return response,
    };
    let stream_id = match validate_stream_id(&req.stream_id) {
        Ok(stream_id) => stream_id,
        Err(response) => return response,
    };
    let title = match normalize_room_title(&req.title) {
        Ok(title) => title,
        Err(message) => return (StatusCode::BAD_REQUEST, error_response(400, message)),
    };

    match live_room::Entity::find()
        .filter(live_room::Column::StreamId.eq(&stream_id))
        .one(&state.db)
        .await
    {
        Ok(Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                error_response(400, "stream id already exists"),
            );
        }
        Ok(None) => {}
        Err(e) => {
            error!("Failed to check room duplicate: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to check room"),
            );
        }
    }

    let active = live_room::ActiveModel {
        user_id: Set(owner.id),
        stream_id: Set(stream_id),
        title: Set(title),
        stream_code: Set(generate_random_string(16)),
        enabled: Set(req.enabled.unwrap_or(true)),
        ..Default::default()
    };

    match active.insert(&state.db).await {
        Ok(room) => match room_response(&state.db, room).await {
            Ok(response) => (StatusCode::OK, success_response(response)),
            Err(response) => response,
        },
        Err(e) => {
            error!("Failed to create room: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to create room"),
            )
        }
    }
}

pub async fn update_room(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
    Json(req): Json<UpdateRoomRequest>,
) -> impl IntoResponse {
    if let Err(response) = require_admin(&auth_user) {
        return response;
    }

    let room = match live_room::Entity::find_by_id(id).one(&state.db).await {
        Ok(Some(room)) => room,
        Ok(None) => return (StatusCode::NOT_FOUND, error_response(404, "room not found")),
        Err(e) => {
            error!("Failed to find room: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to find room"),
            );
        }
    };

    let super_only_change =
        req.user_id.is_some() || req.stream_id.is_some() || req.enabled.is_some();
    if super_only_change && !auth_user.is_super_admin() {
        return (
            StatusCode::FORBIDDEN,
            error_response(403, "super admin required"),
        );
    }

    let mut active: live_room::ActiveModel = room.into();
    if let Some(user_id) = req.user_id {
        let owner = match ensure_user_exists(&state.db, user_id).await {
            Ok(user) => user,
            Err(response) => return response,
        };
        active.user_id = Set(owner.id);
    }
    if let Some(stream_id) = req.stream_id {
        let stream_id = match validate_stream_id(&stream_id) {
            Ok(stream_id) => stream_id,
            Err(response) => return response,
        };
        match live_room::Entity::find()
            .filter(live_room::Column::StreamId.eq(&stream_id))
            .filter(live_room::Column::Id.ne(id))
            .one(&state.db)
            .await
        {
            Ok(Some(_)) => {
                return (
                    StatusCode::BAD_REQUEST,
                    error_response(400, "stream id already exists"),
                );
            }
            Ok(None) => {}
            Err(e) => {
                error!("Failed to check room duplicate: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_response(500, "failed to check room"),
                );
            }
        }
        active.stream_id = Set(stream_id);
    }
    if let Some(title) = req.title {
        let title = match normalize_room_title(&title) {
            Ok(title) => title,
            Err(message) => return (StatusCode::BAD_REQUEST, error_response(400, message)),
        };
        active.title = Set(title);
    }
    if let Some(enabled) = req.enabled {
        active.enabled = Set(enabled);
    }
    active.updated_at = Set(Utc::now().naive_utc());

    match active.update(&state.db).await {
        Ok(room) => {
            sync_legacy_user_room_fields(&state.db, &room).await;
            match room_response(&state.db, room).await {
                Ok(response) => (StatusCode::OK, success_response(response)),
                Err(response) => response,
            }
        }
        Err(e) => {
            error!("Failed to update room: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update room"),
            )
        }
    }
}

pub async fn reset_room_stream_code(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
) -> impl IntoResponse {
    if let Err(response) = require_admin(&auth_user) {
        return response;
    }

    let room = match live_room::Entity::find_by_id(id).one(&state.db).await {
        Ok(Some(room)) => room,
        Ok(None) => return (StatusCode::NOT_FOUND, error_response(404, "room not found")),
        Err(e) => {
            error!("Failed to find room: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to find room"),
            );
        }
    };

    let mut active: live_room::ActiveModel = room.into();
    active.stream_code = Set(generate_random_string(16));
    active.updated_at = Set(Utc::now().naive_utc());

    match active.update(&state.db).await {
        Ok(room) => {
            sync_legacy_user_room_fields(&state.db, &room).await;
            match room_response(&state.db, room).await {
                Ok(response) => (StatusCode::OK, success_response(response)),
                Err(response) => response,
            }
        }
        Err(e) => {
            error!("Failed to reset room stream code: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to reset stream code"),
            )
        }
    }
}

pub async fn delete_room(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
) -> impl IntoResponse {
    if let Err(response) = require_super_admin(&auth_user) {
        return response;
    }

    let room = match live_room::Entity::find_by_id(id).one(&state.db).await {
        Ok(Some(room)) => room,
        Ok(None) => return (StatusCode::NOT_FOUND, error_response(404, "room not found")),
        Err(e) => {
            error!("Failed to find room: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to find room"),
            );
        }
    };

    let active_count = match live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&room.stream_id))
        .filter(live_session::Column::Status.eq("active"))
        .count(&state.db)
        .await
    {
        Ok(count) => count,
        Err(e) => {
            error!(
                "Failed to check active sessions before deleting room: {}",
                e
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to delete room"),
            );
        }
    };
    if active_count > 0 {
        return (
            StatusCode::BAD_REQUEST,
            error_response(400, "stop active stream before deleting room"),
        );
    }

    match live_room::Entity::delete_by_id(id).exec(&state.db).await {
        Ok(_) => (
            StatusCode::OK,
            success_response(serde_json::json!({"deleted": true})),
        ),
        Err(e) => {
            error!("Failed to delete room: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to delete room"),
            )
        }
    }
}

pub async fn stop_stream(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(stream_id): Path<String>,
) -> impl IntoResponse {
    if let Err(response) = require_admin(&auth_user) {
        return response;
    }

    let room = match live_room::Entity::find()
        .filter(live_room::Column::StreamId.eq(&stream_id))
        .one(&state.db)
        .await
    {
        Ok(Some(room)) => room,
        Ok(None) => return (StatusCode::NOT_FOUND, error_response(404, "room not found")),
        Err(e) => {
            error!("Failed to find room before stopping stream: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to find room"),
            );
        }
    };

    let session = match live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&room.stream_id))
        .filter(live_session::Column::Status.eq("active"))
        .one(&state.db)
        .await
    {
        Ok(session) => session,
        Err(e) => {
            error!(
                "Failed to load active session before stopping stream: {}",
                e
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get live session"),
            );
        }
    };

    let client_id = session
        .as_ref()
        .map(|session| session.client_id.as_str())
        .filter(|client_id| !client_id.is_empty())
        .unwrap_or(&room.stream_id);

    match state.srs_client.kick_client(client_id).await {
        Ok(_) => (
            StatusCode::OK,
            success_response(serde_json::json!({
                "stream_id": room.stream_id,
                "stopped": true,
            })),
        ),
        Err(e) => {
            error!("Failed to stop stream: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to stop stream"),
            )
        }
    }
}
