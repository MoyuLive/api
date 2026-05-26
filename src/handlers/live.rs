use axum::{extract::{State, Query}, http::StatusCode, response::IntoResponse, Json};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};

use crate::auth::{CurrentUser, generate_random_string};
use crate::entities::{live_session, user};
use crate::response::{success_response, error_response};
use crate::AppState;

// GET /api/live/stream/code
pub async fn stream_code(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    let user = match user::Entity::find()
        .filter(user::Column::Username.eq(&auth_user.username))
        .one(&state.db)
        .await
    {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (StatusCode::UNAUTHORIZED, error_response(401, "unauthorized"));
        }
        Err(e) => {
            error!("Failed to get user: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get user"),
            );
        }
    };

    (
        StatusCode::OK,
        success_response(serde_json::json!({"stream_code": user.stream_code})),
    )
}

// POST /api/live/stream/code/reset
pub async fn reset_stream_code(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    let new_code = generate_random_string(16);

    let result = user::Entity::update_many()
        .filter(user::Column::Username.eq(&auth_user.username))
        .col_expr(user::Column::StreamCode, sea_orm::sea_query::Expr::value(new_code.clone()))
        .exec(&state.db)
        .await;

    match result {
        Ok(_) => {
            (StatusCode::OK, success_response(serde_json::json!({"stream_code": new_code})))
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
            return (StatusCode::BAD_REQUEST, error_response(400, "missing stream parameter"));
        }
    };

    match state.srs_client.get_stream(&stream_id).await {
        Ok(Some(stream)) => {
            (StatusCode::OK, success_response(serde_json::json!({
                "stream_id": stream_id,
                "online": true,
                "stream": stream,
            })))
        }
        Ok(None) => {
            (StatusCode::OK, success_response(serde_json::json!({
                "stream_id": stream_id,
                "online": false,
            })))
        }
        Err(e) => {
            (StatusCode::OK, success_response(serde_json::json!({
                "stream_id": stream_id,
                "online": false,
                "error": e,
            })))
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
    Json(req): Json<StopStreamRequest>,
) -> impl IntoResponse {
    // Get the live session to find the client ID
    let session = live_session::Entity::find()
        .filter(live_session::Column::StreamId.eq(&req.stream_id))
        .one(&state.db)
        .await;

    match session {
        Ok(Some(s)) => {
            let client_id = if s.client_id.is_empty() {
                req.stream_id.clone()
            } else {
                s.client_id
            };

            match state.srs_client.kick_client(&client_id).await {
                Ok(_) => {
                    info!("Stream stopped: {}", req.stream_id);
                    (StatusCode::OK, success_response(serde_json::json!({
                        "stream_id": req.stream_id,
                        "stopped": true,
                    })))
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
            // Try kicking via stream name as fallback
            match state.srs_client.kick_client(&req.stream_id).await {
                Ok(_) => {
                    (StatusCode::OK, success_response(serde_json::json!({
                        "stream_id": req.stream_id,
                        "stopped": true,
                    })))
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
) -> impl IntoResponse {
    let sessions = live_session::Entity::find()
        .filter(live_session::Column::Status.eq("active"))
        .all(&state.db)
        .await;

    match sessions {
        Ok(list) => {
            (StatusCode::OK, success_response(list))
        }
        Err(e) => {
            error!("Failed to get active sessions: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get active sessions"),
            )
        }
    }
}
