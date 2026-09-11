use axum::{extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use std::sync::Arc;

use crate::response::success_response;
use crate::AppState;

#[derive(Serialize)]
pub struct PlaybackProtocolsResp {
    pub protocols: Vec<String>,
}

#[derive(Serialize)]
pub struct PublishProtocolsResp {
    pub protocols: Vec<String>,
}

// GET /api/playback/protocols
pub async fn protocols(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        success_response(PlaybackProtocolsResp {
            protocols: state.config.playback.protocols(),
        }),
    )
}

// GET /api/publish/protocols
pub async fn publish_protocols(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        success_response(PublishProtocolsResp {
            protocols: state.config.publish.protocols(),
        }),
    )
}
