use axum::{extract::State, http::StatusCode, response::IntoResponse};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};
use serde::Serialize;
use std::sync::Arc;
use tracing::error;

use crate::auth::CurrentUser;
use crate::entities::srs_server;
use crate::response::{error_response, success_response};
use crate::AppState;

#[derive(Serialize)]
pub struct ServerStatusResp {
    pub device_id: String,
    pub ip: String,
    pub cpu_usage: f32,
    pub mem_usage: f32,
    pub uptime_seconds: i64,
    pub last_heartbeat: String,
}

// GET /api/system/status
pub async fn status(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
) -> impl IntoResponse {
    if !auth_user.is_admin() {
        return (StatusCode::FORBIDDEN, error_response(403, "admin required"));
    }

    let servers = srs_server::Entity::find()
        .filter(srs_server::Column::IsActive.eq(true))
        .order_by_desc(srs_server::Column::LastHeartbeat)
        .all(&state.db)
        .await;

    match servers {
        Ok(list) => {
            let result: Vec<ServerStatusResp> = list
                .into_iter()
                .map(|s| ServerStatusResp {
                    device_id: s.device_id,
                    ip: s.ip,
                    cpu_usage: s.cpu_usage,
                    mem_usage: s.mem_usage,
                    uptime_seconds: s.uptime_seconds,
                    last_heartbeat: s.last_heartbeat.format("%Y-%m-%d %H:%M:%S").to_string(),
                })
                .collect();
            (StatusCode::OK, success_response(result))
        }
        Err(e) => {
            error!("Failed to get active servers: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to get server status"),
            )
        }
    }
}
