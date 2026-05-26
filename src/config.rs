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
    pub metrics: MetricsConfig,
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
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

pub fn load_config(config_path: &str) -> Result<Arc<AppConfig>, ConfigError> {
    let settings = ConfigRs::builder()
        .add_source(File::with_name(config_path).required(false))
        .add_source(
            config::Environment::with_prefix("STREAM_API")
                .separator("__"),
        )
        .build()?;

    let cfg: AppConfig = settings.try_deserialize()?;
    Ok(Arc::new(cfg))
}
