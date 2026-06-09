use clap::Parser;
use config::{Config as ConfigRs, ConfigError, File};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "yantube-api")]
#[command(about = "Yantube streaming API server")]
pub struct Cli {
    #[arg(long, default_value = "config.toml")]
    pub config: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub http_port: u16,
    pub db: DbConfig,
    pub user: UserConfig,
    pub srs: SrsConfig,
    #[serde(default = "default_playback_config")]
    pub playback: PlaybackConfig,
    #[serde(default = "default_publish_config")]
    pub publish: PublishConfig,
    #[serde(default = "default_storage_config")]
    pub storage: StorageConfig,
    pub metrics: MetricsConfig,
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbConfig {
    pub dsn: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserConfig {
    #[serde(default = "default_allow_register")]
    pub allow_register: bool,
    #[serde(default = "default_auth_realm")]
    pub auth_realm: String,
    pub auth_secret: String,
}

fn default_allow_register() -> bool {
    true
}
fn default_auth_realm() -> String {
    "stream api".into()
}

#[derive(Debug, Deserialize, Clone)]
pub struct SrsConfig {
    pub api_url: String,
    #[serde(default = "default_admin")]
    pub api_user: String,
    #[serde(default = "default_admin")]
    pub api_password: String,
    pub callback_secret: String,
}

fn default_admin() -> String {
    "admin".into()
}

#[derive(Debug, Deserialize, Clone)]
pub struct PlaybackConfig {
    #[serde(default = "default_playback_protocols")]
    pub protocols: String,
}

impl PlaybackConfig {
    pub fn protocols(&self) -> Vec<String> {
        parse_playback_protocols(&self.protocols)
    }
}

fn default_playback_config() -> PlaybackConfig {
    PlaybackConfig {
        protocols: default_playback_protocols(),
    }
}

fn default_playback_protocols() -> String {
    "webrtc,hls".into()
}

pub fn parse_playback_protocols(raw: &str) -> Vec<String> {
    const SUPPORTED: &[&str] = &["webrtc", "hls", "flv"];
    parse_protocol_list(raw, SUPPORTED, &default_playback_protocols())
}

#[derive(Debug, Deserialize, Clone)]
pub struct PublishConfig {
    #[serde(default = "default_publish_protocols")]
    pub protocols: String,
}

impl PublishConfig {
    pub fn protocols(&self) -> Vec<String> {
        parse_publish_protocols(&self.protocols)
    }
}

fn default_publish_config() -> PublishConfig {
    PublishConfig {
        protocols: default_publish_protocols(),
    }
}

fn default_publish_protocols() -> String {
    "rtmp,whip".into()
}

pub fn parse_publish_protocols(raw: &str) -> Vec<String> {
    const SUPPORTED: &[&str] = &["rtmp", "whip", "srt"];
    parse_protocol_list(raw, SUPPORTED, &default_publish_protocols())
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_upload_dir")]
    pub upload_dir: String,
}

fn default_storage_config() -> StorageConfig {
    StorageConfig {
        upload_dir: default_upload_dir(),
    }
}

fn default_upload_dir() -> String {
    "uploads".into()
}

fn parse_protocol_list(raw: &str, supported: &[&str], fallback: &str) -> Vec<String> {
    let mut protocols = Vec::new();

    for protocol in raw.split(',').map(|p| p.trim().to_ascii_lowercase()) {
        if supported.contains(&protocol.as_str()) && !protocols.contains(&protocol) {
            protocols.push(protocol);
        }
    }

    if protocols.is_empty() {
        parse_protocol_list(fallback, supported, fallback)
    } else {
        protocols
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

fn default_cors_origins() -> Vec<String> {
    vec![
        "http://localhost:5173".to_string(),
        "http://127.0.0.1:5173".to_string(),
        "http://localhost:5174".to_string(),
        "http://127.0.0.1:5174".to_string(),
    ]
}

pub fn load_config(config_path: &str) -> Result<Arc<AppConfig>, ConfigError> {
    let settings = ConfigRs::builder()
        .add_source(File::with_name(config_path).required(false))
        .add_source(
            config::Environment::with_prefix("STREAM_API")
                .prefix_separator("_")
                .separator("__")
                .try_parsing(true),
        )
        .build()?;

    let cfg: AppConfig = settings.try_deserialize()?;
    Ok(Arc::new(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const TEST_ENV_KEYS: &[&str] = &[
        "STREAM_API_HTTP_PORT",
        "STREAM_API_DB__DSN",
        "STREAM_API_USER__ALLOW_REGISTER",
        "STREAM_API_USER__AUTH_SECRET",
        "STREAM_API_SRS__API_URL",
        "STREAM_API_SRS__API_USER",
        "STREAM_API_SRS__API_PASSWORD",
        "STREAM_API_SRS__CALLBACK_SECRET",
        "STREAM_API_PLAYBACK__PROTOCOLS",
        "STREAM_API_PUBLISH__PROTOCOLS",
        "STREAM_API_STORAGE__UPLOAD_DIR",
        "STREAM_API_METRICS__ENABLED",
    ];

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for key in TEST_ENV_KEYS {
                env::remove_var(key);
            }
        }
    }

    fn with_clean_env() -> EnvGuard {
        let guard = ENV_LOCK.lock().expect("env lock poisoned");
        for key in TEST_ENV_KEYS {
            env::remove_var(key);
        }
        EnvGuard { _lock: guard }
    }

    #[test]
    fn loads_nested_config_from_double_underscore_env_vars() {
        let _guard = with_clean_env();

        env::set_var("STREAM_API_HTTP_PORT", "19081");
        env::set_var(
            "STREAM_API_DB__DSN",
            "host=postgres user=yantube password=secret dbname=yantube port=5432 sslmode=disable",
        );
        env::set_var("STREAM_API_USER__ALLOW_REGISTER", "false");
        env::set_var("STREAM_API_USER__AUTH_SECRET", "jwt-secret");
        env::set_var("STREAM_API_SRS__API_URL", "http://srs:1985");
        env::set_var("STREAM_API_SRS__API_USER", "srs-admin");
        env::set_var("STREAM_API_SRS__API_PASSWORD", "srs-password");
        env::set_var("STREAM_API_SRS__CALLBACK_SECRET", "callback-secret");
        env::set_var(
            "STREAM_API_PLAYBACK__PROTOCOLS",
            "webrtc, flv, unknown, hls, flv",
        );
        env::set_var(
            "STREAM_API_PUBLISH__PROTOCOLS",
            "rtmp, srt, unknown, whip, srt",
        );
        env::set_var("STREAM_API_METRICS__ENABLED", "false");

        let config = load_config("/private/tmp/moyulive-missing-config.toml")
            .expect("double underscore env vars should load config");

        assert_eq!(config.http_port, 19081);
        assert_eq!(
            config.db.dsn,
            "host=postgres user=yantube password=secret dbname=yantube port=5432 sslmode=disable"
        );
        assert!(!config.user.allow_register);
        assert_eq!(config.user.auth_secret, "jwt-secret");
        assert_eq!(config.srs.api_url, "http://srs:1985");
        assert_eq!(config.srs.api_user, "srs-admin");
        assert_eq!(config.srs.api_password, "srs-password");
        assert_eq!(config.srs.callback_secret, "callback-secret");
        assert_eq!(config.playback.protocols(), vec!["webrtc", "flv", "hls"]);
        assert_eq!(config.publish.protocols(), vec!["rtmp", "srt", "whip"]);
        assert!(!config.metrics.enabled);
    }

    #[test]
    fn playback_protocols_default_to_conservative_supported_list() {
        let _guard = with_clean_env();

        env::set_var("STREAM_API_HTTP_PORT", "19081");
        env::set_var(
            "STREAM_API_DB__DSN",
            "host=postgres user=yantube password=secret dbname=yantube port=5432 sslmode=disable",
        );
        env::set_var("STREAM_API_USER__AUTH_SECRET", "jwt-secret");
        env::set_var("STREAM_API_SRS__API_URL", "http://srs:1985");
        env::set_var("STREAM_API_SRS__CALLBACK_SECRET", "callback-secret");
        env::set_var("STREAM_API_METRICS__ENABLED", "false");

        let config = load_config("/private/tmp/moyulive-missing-config.toml")
            .expect("config should load with default playback protocols");

        assert_eq!(config.playback.protocols(), vec!["webrtc", "hls"]);
        assert_eq!(config.publish.protocols(), vec!["rtmp", "whip"]);
    }

    #[test]
    fn publish_protocols_reject_unknown_values_and_deduplicate() {
        assert_eq!(
            parse_publish_protocols("srt, unknown, rtmp, srt"),
            vec!["srt", "rtmp"]
        );
        assert_eq!(parse_publish_protocols("unknown"), vec!["rtmp", "whip"]);
    }
}
