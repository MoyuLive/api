use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::{collections::HashMap, sync::Arc};
use tracing::error;

use crate::entities::{live_room, live_stream_state};
use crate::AppState;

const LIVE_FEED_MIN_LIVE_SECONDS: i64 = 60;

#[derive(Debug, Clone, PartialEq)]
struct LiveFeedItem {
    title: String,
    link: String,
    guid: String,
    pub_date: String,
    description: String,
    started_at_ms: i64,
}

// GET /feeds/live.xml
pub async fn live_rss_feed(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    live_feed_response(state, headers, None).await
}

// GET /feeds/live/:stream_id
pub async fn live_room_rss_feed(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    live_feed_response(state, headers, Some(stream_id)).await
}

async fn live_feed_response(
    state: Arc<AppState>,
    headers: HeaderMap,
    stream_id: Option<String>,
) -> Response {
    let mut query =
        live_stream_state::Entity::find().filter(live_stream_state::Column::Status.eq("active"));
    if let Some(stream_id) = stream_id.as_deref() {
        query = query.filter(live_stream_state::Column::StreamId.eq(stream_id));
    }

    let states = match query.all(&state.db).await {
        Ok(states) => states,
        Err(e) => {
            error!("Failed to load live stream states for RSS: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load live feed",
            )
                .into_response();
        }
    };

    let stream_ids: Vec<String> = states.iter().map(|state| state.stream_id.clone()).collect();
    let rooms = if stream_ids.is_empty() {
        Vec::new()
    } else {
        match live_room::Entity::find()
            .filter(live_room::Column::StreamId.is_in(stream_ids))
            .all(&state.db)
            .await
        {
            Ok(rooms) => rooms,
            Err(e) => {
                error!("Failed to load live feed room metadata: {}", e);
                Vec::new()
            }
        }
    };

    let now = Utc::now().naive_utc();
    let base_url = request_base_url(&headers);
    let items = build_live_feed_items(states, rooms, now, &base_url, stream_id.as_deref());
    let xml = build_live_rss_xml(&items, now, &base_url, stream_id.as_deref());

    (
        [
            (header::CONTENT_TYPE, "application/rss+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        xml,
    )
        .into_response()
}

fn build_live_feed_items(
    states: Vec<live_stream_state::Model>,
    rooms: Vec<live_room::Model>,
    now: NaiveDateTime,
    base_url: &str,
    stream_filter: Option<&str>,
) -> Vec<LiveFeedItem> {
    let rooms_by_stream_id: HashMap<String, live_room::Model> = rooms
        .into_iter()
        .map(|room| (room.stream_id.clone(), room))
        .collect();
    let mut items: Vec<LiveFeedItem> = states
        .into_iter()
        .filter(|state| state.status == "active")
        .filter(|state| {
            stream_filter
                .map(|stream_id| state.stream_id == stream_id)
                .unwrap_or(true)
        })
        .filter(|state| (now - state.updated_at).num_seconds() >= LIVE_FEED_MIN_LIVE_SECONDS)
        .map(|state| {
            let title = rooms_by_stream_id
                .get(&state.stream_id)
                .map(|room| live_room_title(&room.title, &state.stream_id))
                .unwrap_or_else(|| state.stream_id.clone());
            let encoded_stream_id =
                utf8_percent_encode(&state.stream_id, NON_ALPHANUMERIC).to_string();
            let episode_started_at =
                DateTime::<Utc>::from_naive_utc_and_offset(state.episode_started_at, Utc);
            LiveFeedItem {
                title: title.clone(),
                link: format!(
                    "{}/live/{}",
                    base_url.trim_end_matches('/'),
                    encoded_stream_id
                ),
                guid: format!(
                    "moyulive:live:{}:{}",
                    state.stream_id,
                    episode_started_at.timestamp_millis()
                ),
                pub_date: episode_started_at.to_rfc2822(),
                description: format!("{} 正在直播", title),
                started_at_ms: episode_started_at.timestamp_millis(),
            }
        })
        .collect();

    items.sort_by(|a, b| {
        b.started_at_ms
            .cmp(&a.started_at_ms)
            .then_with(|| a.guid.cmp(&b.guid))
    });
    items
}

fn build_live_rss_xml(
    items: &[LiveFeedItem],
    now: NaiveDateTime,
    base_url: &str,
    stream_filter: Option<&str>,
) -> String {
    let now = DateTime::<Utc>::from_naive_utc_and_offset(now, Utc);
    let channel_title = stream_filter
        .map(|stream_id| format!("MoyuLive {} 开播提醒", stream_id))
        .unwrap_or_else(|| "MoyuLive 开播提醒".to_string());
    let channel_description = stream_filter
        .map(|stream_id| format!("{} 直播间的开播提醒", stream_id))
        .unwrap_or_else(|| "当前正在直播的 MoyuLive 房间".to_string());
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<rss version=\"2.0\">\n");
    xml.push_str("  <channel>\n");
    xml.push_str(&format!(
        "    <title>{}</title>\n",
        escape_xml(&channel_title)
    ));
    xml.push_str(&format!("    <link>{}</link>\n", escape_xml(base_url)));
    xml.push_str(&format!(
        "    <description>{}</description>\n",
        escape_xml(&channel_description)
    ));
    xml.push_str("    <ttl>5</ttl>\n");
    xml.push_str(&format!(
        "    <lastBuildDate>{}</lastBuildDate>\n",
        now.to_rfc2822()
    ));

    for item in items {
        xml.push_str("    <item>\n");
        xml.push_str(&format!(
            "      <title>{}</title>\n",
            escape_xml(&item.title)
        ));
        xml.push_str(&format!("      <link>{}</link>\n", escape_xml(&item.link)));
        xml.push_str(&format!(
            "      <guid isPermaLink=\"false\">{}</guid>\n",
            escape_xml(&item.guid)
        ));
        xml.push_str(&format!("      <pubDate>{}</pubDate>\n", item.pub_date));
        xml.push_str(&format!(
            "      <description>{}</description>\n",
            escape_xml(&item.description)
        ));
        xml.push_str("    </item>\n");
    }

    xml.push_str("  </channel>\n");
    xml.push_str("</rss>\n");
    xml
}

fn request_base_url(headers: &HeaderMap) -> String {
    let scheme = header_str(headers, "x-forwarded-proto")
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("http");
    let host = header_str(headers, "x-forwarded-host")
        .or_else(|| header_str(headers, header::HOST.as_str()))
        .unwrap_or("localhost:5173");

    format!("{}://{}", scheme, host)
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn escape_xml(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn live_room_title(room_title: &str, fallback: &str) -> String {
    let title = room_title.trim();
    if title.is_empty() {
        format!("{}的直播间", fallback)
    } else {
        format!("{} - {}", title, fallback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp(value: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(value, "%F %T").expect("valid timestamp")
    }

    fn live_state(stream_id: &str, episode_started_at: &str) -> live_stream_state::Model {
        live_state_with_user_and_updated_at(stream_id, 1, episode_started_at, episode_started_at)
    }

    fn live_state_with_updated_at(
        stream_id: &str,
        episode_started_at: &str,
        updated_at: &str,
    ) -> live_stream_state::Model {
        live_state_with_user_and_updated_at(stream_id, 1, episode_started_at, updated_at)
    }

    fn live_state_with_user_and_updated_at(
        stream_id: &str,
        user_id: i32,
        episode_started_at: &str,
        updated_at: &str,
    ) -> live_stream_state::Model {
        live_stream_state::Model {
            id: 1,
            stream_id: stream_id.to_string(),
            user_id,
            status: "active".to_string(),
            episode_started_at: timestamp(episode_started_at),
            last_unpublished_at: None,
            updated_at: timestamp(updated_at),
        }
    }

    fn live_room_model(id: i32, stream_id: &str, title: &str) -> live_room::Model {
        live_room::Model {
            id,
            user_id: id,
            stream_id: stream_id.to_string(),
            title: title.to_string(),
            cover_url: String::new(),
            stream_code: "stream-code".to_string(),
            enabled: true,
            created_at: timestamp("2026-06-04 00:00:00"),
            updated_at: timestamp("2026-06-04 00:00:00"),
        }
    }

    #[test]
    fn feed_items_skip_streams_before_minimum_live_age() {
        let items = build_live_feed_items(
            vec![live_state("dawu", "2026-06-04 12:00:01")],
            vec![live_room_model(1, "dawu", "大雾的游戏时间")],
            timestamp("2026-06-04 12:01:00"),
            "https://live.example.test",
            None,
        );

        assert!(items.is_empty());
    }

    #[test]
    fn feed_items_use_episode_start_as_stable_guid() {
        let items = build_live_feed_items(
            vec![live_state("dawu", "2026-06-04 12:00:00")],
            vec![live_room_model(1, "dawu", "大雾的游戏时间")],
            timestamp("2026-06-04 12:01:00"),
            "https://live.example.test",
            None,
        );

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "大雾的游戏时间 - dawu");
        assert_eq!(items[0].link, "https://live.example.test/live/dawu");
        assert_eq!(items[0].guid, "moyulive:live:dawu:1780574400000");
    }

    #[test]
    fn feed_items_wait_for_current_publish_age_after_reconnect() {
        let items = build_live_feed_items(
            vec![live_state_with_updated_at(
                "dawu",
                "2026-06-04 12:00:00",
                "2026-06-04 12:05:30",
            )],
            vec![live_room_model(1, "dawu", "大雾的游戏时间")],
            timestamp("2026-06-04 12:06:00"),
            "https://live.example.test",
            None,
        );

        assert!(items.is_empty());
    }

    #[test]
    fn feed_items_can_filter_to_one_stream() {
        let items = build_live_feed_items(
            vec![
                live_state_with_user_and_updated_at(
                    "dawu",
                    1,
                    "2026-06-04 12:00:00",
                    "2026-06-04 12:00:00",
                ),
                live_state_with_user_and_updated_at(
                    "ytb",
                    2,
                    "2026-06-04 12:05:00",
                    "2026-06-04 12:05:00",
                ),
            ],
            vec![
                live_room_model(1, "dawu", "大雾的游戏时间"),
                live_room_model(2, "ytb", "YTB"),
            ],
            timestamp("2026-06-04 12:06:30"),
            "https://live.example.test",
            Some("dawu"),
        );

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "大雾的游戏时间 - dawu");
        assert_eq!(items[0].link, "https://live.example.test/live/dawu");
    }

    #[test]
    fn feed_items_use_username_room_when_title_is_empty() {
        let items = build_live_feed_items(
            vec![live_state("dawu", "2026-06-04 12:00:00")],
            vec![live_room_model(1, "dawu", "")],
            timestamp("2026-06-04 12:01:00"),
            "https://live.example.test",
            None,
        );

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "dawu的直播间");
        assert_eq!(items[0].description, "dawu的直播间 正在直播");
    }
}
