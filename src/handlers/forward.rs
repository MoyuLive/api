use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use serde::Deserialize;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use tracing::{error, info};
use url::Url;

use crate::entities::forward_rule;
use crate::response::{error_response, success_response};
use crate::AppState;

// GET /api/live/forward/rules
pub async fn list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rules = forward_rule::Entity::find().all(&state.db).await;

    match rules {
        Ok(list) => (StatusCode::OK, success_response(list)),
        Err(e) => {
            error!("Failed to list forward rules: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to list forward rules"),
            )
        }
    }
}

#[derive(Deserialize)]
pub struct AddForwardRuleRequest {
    pub stream_filter: String,
    pub target_url: String,
}

fn validate_forward_url(url_str: &str) -> Result<(), String> {
    let parsed = Url::parse(url_str).map_err(|e| format!("invalid URL: {}", e))?;
    let host = parsed.host_str().ok_or("URL must have a host")?;

    // Reject localhost and zero-address
    if host == "localhost" || host == "0.0.0.0" {
        return Err("forward URL cannot point to localhost".into());
    }

    // Reject private, loopback, and link-local IPs
    if let Ok(ipv4) = host.parse::<Ipv4Addr>() {
        if ipv4.is_private() || ipv4.is_loopback() || ipv4.is_link_local() {
            return Err("forward URL cannot point to a private or loopback address".into());
        }
    }
    if let Ok(ipv6) = host.parse::<Ipv6Addr>() {
        if ipv6.is_loopback() {
            return Err("forward URL cannot point to a loopback address".into());
        }
    }

    // Only allow RTMP/HTTP(S) schemes
    match parsed.scheme() {
        "rtmp" | "rtmps" | "http" | "https" => {}
        _ => return Err(format!("unsupported forward scheme: {}", parsed.scheme())),
    }

    Ok(())
}

// POST /api/live/forward/rules
pub async fn add(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddForwardRuleRequest>,
) -> impl IntoResponse {
    // Validate target_url to prevent SSRF
    if let Err(msg) = validate_forward_url(&req.target_url) {
        return (StatusCode::BAD_REQUEST, error_response(400, msg));
    }

    let now = chrono::Utc::now().naive_utc();

    let rule = forward_rule::ActiveModel {
        stream_filter: Set(req.stream_filter),
        target_url: Set(req.target_url),
        enabled: Set(true),
        created_at: Set(now),
        updated_at: Set(now),
        ..Default::default()
    };

    match rule.insert(&state.db).await {
        Ok(r) => {
            info!("Forward rule created: {:?}", r.id);
            (StatusCode::OK, success_response(r))
        }
        Err(e) => {
            error!("Failed to create forward rule: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to create forward rule"),
            )
        }
    }
}

// DELETE /api/live/forward/rules/:id
pub async fn delete(State(state): State<Arc<AppState>>, Path(id): Path<i32>) -> impl IntoResponse {
    let result = forward_rule::Entity::delete_by_id(id).exec(&state.db).await;

    match result {
        Ok(res) => {
            if res.rows_affected == 0 {
                return (
                    StatusCode::BAD_REQUEST,
                    error_response(400, "forward rule not found"),
                );
            }
            (
                StatusCode::OK,
                success_response(serde_json::json!({"deleted": true})),
            )
        }
        Err(e) => {
            error!("Failed to delete forward rule: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to delete forward rule"),
            )
        }
    }
}
