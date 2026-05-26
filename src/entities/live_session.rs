use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "live_session")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    #[sea_orm(unique)]
    pub stream_id: String,
    pub app: String,
    pub vhost: String,
    pub user_id: i32,
    pub client_id: String,
    pub server_id: String,
    pub stream_url: String,
    pub status: String,
    pub video_codec: String,
    pub audio_codec: String,
    pub video_width: i32,
    pub video_height: i32,
    pub started_at: DateTime,
    pub ended_at: Option<DateTime>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
