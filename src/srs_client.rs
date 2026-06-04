use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
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
    #[serde(default)]
    pub publish: Option<SrsStreamPublish>,
}

impl SrsStream {
    pub fn is_publishing(&self) -> bool {
        self.publish
            .as_ref()
            .map(|publish| publish.active)
            .unwrap_or(false)
    }
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
    #[serde(alias = "channel")]
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
pub struct SrsStreamPublish {
    pub active: bool,
    #[serde(default)]
    pub cid: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SrsStreamsData {
    pub streams: Vec<SrsStream>,
}

#[derive(Deserialize)]
struct SrsStreamsResponse {
    code: i32,
    #[serde(default)]
    streams: Vec<SrsStream>,
    data: Option<SrsStreamsData>,
}

fn parse_streams_response(body: &str) -> Result<Vec<SrsStream>, String> {
    let resp: SrsStreamsResponse =
        serde_json::from_str(body).map_err(|e| format!("parse error: {}", e))?;
    if resp.code != 0 {
        return Err(format!("SRS API error: code={}", resp.code));
    }

    if !resp.streams.is_empty() {
        return Ok(resp.streams);
    }

    Ok(resp.data.map(|data| data.streams).unwrap_or_default())
}

impl SrsClient {
    pub fn new(base_url: String, username: String, password: String) -> Self {
        let auth = format!("{}:{}", username, password);
        let auth_b64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, auth.as_bytes());
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
            .request(
                reqwest::Method::from_bytes(method.as_bytes()).expect("valid HTTP method"),
                &url,
            )
            .header("Authorization", &self.auth_header);

        let resp = req
            .send()
            .await
            .map_err(|e| format!("request failed: {}", e))?;
        let body = resp
            .text()
            .await
            .map_err(|e| format!("read body failed: {}", e))?;
        Ok(body)
    }

    pub async fn list_streams(&self, start: i32, count: i32) -> Result<Vec<SrsStream>, String> {
        let path = format!("/api/v1/streams/?start={}&count={}", start, count);
        let body = self.do_request("GET", &path).await?;
        parse_streams_response(&body)
    }

    pub async fn list_all_streams(&self) -> Result<Vec<SrsStream>, String> {
        const PAGE_SIZE: i32 = 100;

        let mut start = 0;
        let mut all_streams = Vec::new();
        loop {
            let streams = self.list_streams(start, PAGE_SIZE).await?;
            let stream_count = streams.len();
            all_streams.extend(streams);

            if stream_count < PAGE_SIZE as usize {
                break;
            }

            start += PAGE_SIZE;
        }

        Ok(all_streams)
    }

    pub async fn get_stream(&self, stream_id: &str) -> Result<Option<SrsStream>, String> {
        let encoded_id: String = percent_encode(stream_id.as_bytes(), NON_ALPHANUMERIC).collect();
        let path = format!("/api/v1/streams/{}/", encoded_id);
        let body = self.do_request("GET", &path).await?;

        #[derive(Deserialize)]
        struct Wrapper {
            code: i32,
            data: Option<SrsStream>,
        }

        let resp: Wrapper =
            serde_json::from_str(&body).map_err(|e| format!("parse error: {}", e))?;
        if resp.code != 0 {
            return Err(format!("SRS API error: code={}", resp.code));
        }

        Ok(resp.data)
    }

    pub async fn kick_client(&self, client_id: &str) -> Result<(), String> {
        let encoded_id: String = percent_encode(client_id.as_bytes(), NON_ALPHANUMERIC).collect();
        let path = format!("/api/v1/clients/{}/", encoded_id);
        let body = self.do_request("DELETE", &path).await?;

        let resp: SrsApiResponse =
            serde_json::from_str(&body).map_err(|e| format!("parse error: {}", e))?;
        if resp.code != 0 {
            return Err(format!("SRS API error: code={}", resp.code));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_top_level_streams_response_from_srs() {
        let body = r#"{
            "code": 0,
            "streams": [{
                "id": "vid-1",
                "name": "ytb",
                "vhost": "__defaultVhost__",
                "app": "live",
                "live_ms": 1780321583369,
                "clients": 2,
                "frames": 15220,
                "send_bytes": 242278001,
                "recv_bytes": 453287163,
                "kbps": {"recv_30s": 14173, "send_30s": 0},
                "publish": {"active": true, "cid": "qv616x10"},
                "video": {
                    "codec": "H264",
                    "profile": "High",
                    "level": "Other",
                    "width": 1920,
                    "height": 1080
                },
                "audio": {
                    "codec": "AAC",
                    "sample_rate": 44100,
                    "channel": 2,
                    "profile": "LC"
                }
            }]
        }"#;

        let streams = parse_streams_response(body).expect("SRS response should parse");

        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "ytb");
        assert!(streams[0].is_publishing());
        assert_eq!(
            streams[0].audio.as_ref().map(|audio| audio.channels),
            Some(2)
        );
    }

    #[test]
    fn parses_nested_data_streams_response() {
        let body = r#"{
            "code": 0,
            "data": {
                "streams": [{
                    "id": "vid-1",
                    "name": "ytb",
                    "vhost": "__defaultVhost__",
                    "app": "live",
                    "live_ms": 3000,
                    "clients": 1,
                    "frames": 10,
                    "send_bytes": 20,
                    "recv_bytes": 30,
                    "publish": {"active": false}
                }]
            }
        }"#;

        let streams = parse_streams_response(body).expect("SRS response should parse");

        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "ytb");
        assert!(!streams[0].is_publishing());
    }
}
