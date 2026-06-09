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

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, extract::State, response::IntoResponse};
    use sea_orm::{DbBackend, MockDatabase};
    use std::sync::Arc;

    use super::*;
    use crate::config::{
        AppConfig, DbConfig, MetricsConfig, PlaybackConfig, PublishConfig, SrsConfig, UserConfig,
    };
    use crate::srs_client::SrsClient;
    use crate::AppState;

    fn test_state(protocols: &str) -> Arc<AppState> {
        test_state_with_publish(protocols, "rtmp,whip")
    }

    fn test_state_with_publish(protocols: &str, publish_protocols: &str) -> Arc<AppState> {
        Arc::new(AppState {
            db: MockDatabase::new(DbBackend::Postgres).into_connection(),
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
                    protocols: protocols.to_string(),
                },
                publish: PublishConfig {
                    protocols: publish_protocols.to_string(),
                },
                metrics: MetricsConfig { enabled: false },
                cors_origins: vec!["http://localhost:5173".to_string()],
            }),
            srs_client: Arc::new(SrsClient::new(
                "http://srs:1985".to_string(),
                "admin".to_string(),
                "password".to_string(),
            )),
        })
    }

    #[tokio::test]
    async fn protocols_returns_configured_playback_protocols() {
        let response = protocols(State(test_state("webrtc,flv,hls")))
            .await
            .into_response();
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("response body should be readable");
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("response should be json");

        assert_eq!(json["code"], 0);
        assert_eq!(
            json["data"]["protocols"],
            serde_json::json!(["webrtc", "flv", "hls"])
        );
    }

    #[tokio::test]
    async fn publish_protocols_returns_configured_publish_protocols() {
        let response = publish_protocols(State(test_state_with_publish("webrtc,hls", "rtmp,srt")))
            .await
            .into_response();
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("response body should be readable");
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("response should be json");

        assert_eq!(json["code"], 0);
        assert_eq!(
            json["data"]["protocols"],
            serde_json::json!(["rtmp", "srt"])
        );
    }
}
