use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};

use crate::auth::{hash_password, verify_password, create_jwt, generate_random_string};
use crate::entities::user;
use crate::response::{success_response, error_response};
use crate::AppState;

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if !state.config.user.allow_register {
        return (StatusCode::FORBIDDEN, error_response(403, "register is not allowed"));
    }

    let hashed = hash_password(&req.password);
    let stream_code = generate_random_string(16);

    let user = user::ActiveModel {
        username: Set(req.username),
        password: Set(hashed),
        stream_code: Set(stream_code),
        ..Default::default()
    };

    match user.insert(&state.db).await {
        Ok(_) => {
            info!("User created");
            (StatusCode::OK, success_response(serde_json::Value::Null))
        }
        Err(e) => {
            error!("Failed to create user: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, error_response(500, "server save user failed"))
        }
    }
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(serde::Serialize)]
struct LoginResponse {
    token: String,
}

pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    let user = match user::Entity::find()
        .filter(user::Column::Username.eq(&req.username))
        .one(&state.db)
        .await
    {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (StatusCode::BAD_REQUEST, error_response(400, "user not exists"));
        }
        Err(e) => {
            error!("Failed to get user: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, error_response(500, "server get user failed"));
        }
    };

    match verify_password(&user.password, &req.password) {
        Ok(true) => {}
        Ok(false) => {
            return (StatusCode::BAD_REQUEST, error_response(400, "password incorrect"));
        }
        Err(e) => {
            error!("Password verification error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "server can't validate password"),
            );
        }
    }

    let token = match create_jwt(&user.username, &state.config.user.auth_secret) {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to create JWT: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, error_response(500, "failed to create token"));
        }
    };

    (StatusCode::OK, success_response(LoginResponse { token }))
}

pub async fn refresh(
    State(state): State<Arc<AppState>>,
    auth_user: crate::auth::CurrentUser,
) -> impl IntoResponse {
    let token = match create_jwt(&auth_user.username, &state.config.user.auth_secret) {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to refresh JWT: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to refresh token"),
            );
        }
    };

    (StatusCode::OK, success_response(LoginResponse { token }))
}

pub async fn logout() -> impl IntoResponse {
    // Stateless JWT - no session to invalidate
    (StatusCode::OK, success_response(serde_json::Value::Null))
}
