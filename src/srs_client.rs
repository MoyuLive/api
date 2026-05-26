use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SrsClient {
    base_url: String,
    http_client: Client,
    auth_header: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SrsApiResponse {
    pub code: i32,
    #[serde(default)]
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SrsStream {
    pub id: String,
    pub name: String,
    pub vhost: String,
    pub app: String,
    pub live_ms: i64,
    pub clients: i32,
    pub frames: i64,
    pub send_bytes: i64,
    pub recv_bytes: i64,
    #[serde(default)]
    pub kbps: Option<SrsStreamKbps>,
    #[serde(default)]
    pub audio: Option<SrsStreamAudio>,
    #[serde(default)]
    pub video: Option<SrsStreamVideo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SrsStreamKbps {
    pub recv_30s: i32,
    pub send_30s: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SrsStreamAudio {
    pub codec: String,
    pub profile: String,
    pub sample_rate: i32,
    pub channels: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SrsStreamVideo {
    pub codec: String,
    pub profile: String,
    pub level: String,
    pub width: i32,
    pub height: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SrsStreamsData {
    pub streams: Vec<SrsStream>,
}

impl SrsClient {
    pub fn new(base_url: String, username: String, password: String) -> Self {
        let auth = format!("{}:{}", username, password);
        let auth_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, auth.as_bytes());
        let auth_header = format!("Basic {}", auth_b64);

        SrsClient {
            base_url,
            http_client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            auth_header,
        }
    }

    async fn do_request(&self, method: &str, path: &str) -> Result<String, String> {
        let url = format!("{}{}", self.base_url, path);
        let req = self
            .http_client
            .request(method.parse().unwrap(), &url)
            .header("Authorization", &self.auth_header);

        let resp = req.send().await.map_err(|e| format!("request failed: {}", e))?;
        let body = resp.text().await.map_err(|e| format!("read body failed: {}", e))?;
        Ok(body)
    }

    pub async fn list_streams(&self, start: i32, count: i32) -> Result<Vec<SrsStream>, String> {
        let path = format!("/api/v1/streams/?start={}&count={}", start, count);
        let body = self.do_request("GET", &path).await?;

        #[derive(Deserialize)]
        struct Wrapper {
            code: i32,
            data: Option<SrsStreamsData>,
        }

        let resp: Wrapper = serde_json::from_str(&body).map_err(|e| format!("parse error: {}", e))?;
        if resp.code != 0 {
            return Err(format!("SRS API error: code={}", resp.code));
        }

        Ok(resp.data.map(|d| d.streams).unwrap_or_default())
    }

    pub async fn get_stream(&self, stream_id: &str) -> Result<Option<SrsStream>, String> {
        let path = format!("/api/v1/streams/{}/", stream_id);
        let body = self.do_request("GET", &path).await?;

        #[derive(Deserialize)]
        struct Wrapper {
            code: i32,
            data: Option<SrsStream>,
        }

        let resp: Wrapper = serde_json::from_str(&body).map_err(|e| format!("parse error: {}", e))?;
        if resp.code != 0 {
            return Err(format!("SRS API error: code={}", resp.code));
        }

        Ok(resp.data)
    }

    pub async fn kick_client(&self, client_id: &str) -> Result<(), String> {
        let path = format!("/api/v1/clients/{}/", client_id);
        let body = self.do_request("DELETE", &path).await?;

        let resp: SrsApiResponse =
            serde_json::from_str(&body).map_err(|e| format!("parse error: {}", e))?;
        if resp.code != 0 {
            return Err(format!("SRS API error: code={}", resp.code));
        }

        Ok(())
    }
}
