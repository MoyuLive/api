use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, Set,
};
use serde::Deserialize;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use tracing::{error, info};
use url::Url;

use crate::auth::CurrentUser;
use crate::entities::forward_rule;
use crate::response::{error_response, success_response};
use crate::AppState;

// GET /api/live/forward/rules
pub async fn list(State(state): State<Arc<AppState>>, auth_user: CurrentUser) -> impl IntoResponse {
    if !auth_user.is_admin() {
        return (StatusCode::FORBIDDEN, error_response(403, "admin required"));
    }

    let rules = forward_rule::Entity::find()
        .order_by_asc(forward_rule::Column::Id)
        .all(&state.db)
        .await;

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
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Deserialize)]
pub struct UpdateForwardRuleRequest {
    pub stream_filter: Option<String>,
    pub target_url: Option<String>,
    pub enabled: Option<bool>,
}

fn default_enabled() -> bool {
    true
}

fn normalize_stream_filter(filter: &str) -> Result<String, String> {
    let filter = filter.trim();
    if filter.is_empty() {
        return Err("stream filter cannot be empty".into());
    }
    if filter.len() > 256 {
        return Err("stream filter is too long".into());
    }
    if filter.chars().any(char::is_whitespace) {
        return Err("stream filter cannot contain whitespace".into());
    }
    if filter == "*" {
        return Ok(filter.to_string());
    }

    let parts: Vec<&str> = filter.split('/').collect();
    match parts.as_slice() {
        [stream] if is_valid_filter_segment(stream) => Ok(filter.to_string()),
        [app, stream]
            if is_valid_filter_segment(app)
                && (*stream == "*" || is_valid_filter_segment(stream)) =>
        {
            Ok(filter.to_string())
        }
        _ => Err("stream filter must be *, stream, app/*, or app/stream".into()),
    }
}

fn is_valid_filter_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

fn validate_forward_target_template(url_str: &str) -> Result<(), String> {
    if url_str.len() > 512 {
        return Err("forward URL is too long".into());
    }
    let without_placeholders = url_str.replace("{app}", "").replace("{stream}", "");
    if without_placeholders.contains('{') || without_placeholders.contains('}') {
        return Err("unsupported forward URL template placeholder".into());
    }

    let rendered = url_str
        .replace("{app}", "live")
        .replace("{stream}", "stream")
        .replace("[app]", "live")
        .replace("[stream]", "stream");
    validate_forward_url(&rendered)
}

fn validate_forward_url(url_str: &str) -> Result<(), String> {
    let parsed = Url::parse(url_str).map_err(|e| format!("invalid URL: {}", e))?;
    let host = parsed.host_str().ok_or("URL must have a host")?;
    let normalized_host = host.trim_end_matches('.').to_ascii_lowercase();

    // Reject localhost and zero-address
    if normalized_host == "localhost" || normalized_host == "0.0.0.0" {
        return Err("forward URL cannot point to localhost".into());
    }

    // Reject private, loopback, and link-local IPs
    if let Ok(ipv4) = normalized_host.parse::<Ipv4Addr>() {
        if ipv4.is_private() || ipv4.is_loopback() || ipv4.is_link_local() || ipv4.is_unspecified()
        {
            return Err("forward URL cannot point to a private or loopback address".into());
        }
    }
    if let Ok(ipv6) = normalized_host.parse::<Ipv6Addr>() {
        if ipv6.is_loopback()
            || ipv6.is_unspecified()
            || ipv6.is_unique_local()
            || ipv6.is_unicast_link_local()
        {
            return Err("forward URL cannot point to a loopback address".into());
        }
    }

    // SRS forward expects RTMP target URLs.
    match parsed.scheme() {
        "rtmp" => {}
        _ => return Err(format!("unsupported forward scheme: {}", parsed.scheme())),
    }

    Ok(())
}

async fn rule_exists(
    state: &AppState,
    stream_filter: &str,
    target_url: &str,
    except_id: Option<i32>,
) -> Result<bool, sea_orm::DbErr> {
    let mut query = forward_rule::Entity::find()
        .filter(forward_rule::Column::StreamFilter.eq(stream_filter))
        .filter(forward_rule::Column::TargetUrl.eq(target_url));

    if let Some(id) = except_id {
        query = query.filter(forward_rule::Column::Id.ne(id));
    }

    Ok(query.one(&state.db).await?.is_some())
}

// POST /api/live/forward/rules
pub async fn add(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Json(req): Json<AddForwardRuleRequest>,
) -> impl IntoResponse {
    if !auth_user.is_admin() {
        return (StatusCode::FORBIDDEN, error_response(403, "admin required"));
    }

    let stream_filter = match normalize_stream_filter(&req.stream_filter) {
        Ok(filter) => filter,
        Err(msg) => return (StatusCode::BAD_REQUEST, error_response(400, msg)),
    };
    let target_url = req.target_url.trim().to_string();

    if let Err(msg) = validate_forward_target_template(&target_url) {
        return (StatusCode::BAD_REQUEST, error_response(400, msg));
    }
    match rule_exists(&state, &stream_filter, &target_url, None).await {
        Ok(true) => {
            return (
                StatusCode::BAD_REQUEST,
                error_response(400, "forward rule already exists"),
            )
        }
        Ok(false) => {}
        Err(e) => {
            error!("Failed to check forward rule duplicate: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to check forward rule duplicate"),
            );
        }
    }

    let now = chrono::Utc::now().naive_utc();

    let rule = forward_rule::ActiveModel {
        stream_filter: Set(stream_filter),
        target_url: Set(target_url),
        enabled: Set(req.enabled),
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

// PUT /api/live/forward/rules/:id
pub async fn update(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
    Json(req): Json<UpdateForwardRuleRequest>,
) -> impl IntoResponse {
    if !auth_user.is_admin() {
        return (StatusCode::FORBIDDEN, error_response(403, "admin required"));
    }

    let Some(existing) = (match forward_rule::Entity::find_by_id(id).one(&state.db).await {
        Ok(rule) => rule,
        Err(e) => {
            error!("Failed to find forward rule: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to find forward rule"),
            );
        }
    }) else {
        return (
            StatusCode::BAD_REQUEST,
            error_response(400, "forward rule not found"),
        );
    };

    let stream_filter = match req.stream_filter {
        Some(filter) => match normalize_stream_filter(&filter) {
            Ok(filter) => filter,
            Err(msg) => return (StatusCode::BAD_REQUEST, error_response(400, msg)),
        },
        None => existing.stream_filter.clone(),
    };
    let target_url = match req.target_url {
        Some(url) => url.trim().to_string(),
        None => existing.target_url.clone(),
    };

    if let Err(msg) = validate_forward_target_template(&target_url) {
        return (StatusCode::BAD_REQUEST, error_response(400, msg));
    }
    match rule_exists(&state, &stream_filter, &target_url, Some(id)).await {
        Ok(true) => {
            return (
                StatusCode::BAD_REQUEST,
                error_response(400, "forward rule already exists"),
            )
        }
        Ok(false) => {}
        Err(e) => {
            error!("Failed to check forward rule duplicate: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to check forward rule duplicate"),
            );
        }
    }

    let mut rule = existing.into_active_model();
    rule.stream_filter = Set(stream_filter);
    rule.target_url = Set(target_url);
    if let Some(enabled) = req.enabled {
        rule.enabled = Set(enabled);
    }
    rule.updated_at = Set(chrono::Utc::now().naive_utc());

    match rule.update(&state.db).await {
        Ok(rule) => (StatusCode::OK, success_response(rule)),
        Err(e) => {
            error!("Failed to update forward rule: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to update forward rule"),
            )
        }
    }
}

// DELETE /api/live/forward/rules/:id
pub async fn delete(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
) -> impl IntoResponse {
    if !auth_user.is_admin() {
        return (StatusCode::FORBIDDEN, error_response(403, "admin required"));
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_supported_stream_filters() {
        for filter in ["*", "dawu", "live/*", "live/dawu", "live.room/room-1"] {
            assert_eq!(normalize_stream_filter(filter).unwrap(), filter);
        }
    }

    #[test]
    fn rejects_dead_stream_filters() {
        for filter in ["", "live/", "/dawu", "live/*/extra", "live room/dawu"] {
            assert!(normalize_stream_filter(filter).is_err(), "{filter}");
        }
    }

    #[test]
    fn validates_rtmp_template_urls() {
        assert!(validate_forward_target_template("rtmp://edge.example/live/{stream}").is_ok());
        assert!(validate_forward_target_template("rtmp://edge.example/{app}/{stream}").is_ok());
        assert!(validate_forward_target_template("https://edge.example/live/stream").is_err());
        assert!(validate_forward_target_template("rtmp://127.0.0.1/live/stream").is_err());
        assert!(validate_forward_target_template("rtmp://edge.example/live/{unknown}").is_err());
    }
}
