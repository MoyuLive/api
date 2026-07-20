use std::collections::HashMap;

use serde::Serialize;
use tokio::sync::{broadcast, RwLock};

use crate::room_access::ViewerIdentity;

const ROOM_EVENT_CAPACITY: usize = 256;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RoomEvent {
    ViewerCount {
        count: usize,
    },
    Danmaku {
        id: String,
        sender: ViewerIdentity,
        content: String,
        sent_at: String,
    },
}

pub struct LiveHub {
    rooms: RwLock<HashMap<String, RoomState>>,
}

struct RoomState {
    clients: HashMap<String, String>,
    viewer_sessions: HashMap<String, usize>,
    events: broadcast::Sender<RoomEvent>,
}

impl RoomState {
    fn new() -> Self {
        let (events, _) = broadcast::channel(ROOM_EVENT_CAPACITY);
        Self {
            clients: HashMap::new(),
            viewer_sessions: HashMap::new(),
            events,
        }
    }

    fn viewer_count(&self) -> usize {
        self.viewer_sessions.len()
    }

    fn remove_viewer_session(&mut self, viewer_key: &str) {
        let remove_viewer = match self.viewer_sessions.get_mut(viewer_key) {
            Some(sessions) if *sessions > 1 => {
                *sessions -= 1;
                false
            }
            Some(_) => true,
            None => false,
        };

        if remove_viewer {
            self.viewer_sessions.remove(viewer_key);
        }
    }

    fn add_viewer_session(&mut self, viewer_key: String) {
        *self.viewer_sessions.entry(viewer_key).or_insert(0) += 1;
    }
}

impl LiveHub {
    pub fn new() -> Self {
        Self {
            rooms: RwLock::new(HashMap::new()),
        }
    }

    pub async fn viewer_count(&self, stream_id: &str) -> usize {
        self.rooms
            .read()
            .await
            .get(stream_id)
            .map(RoomState::viewer_count)
            .unwrap_or(0)
    }

    pub async fn viewer_counts(&self, stream_ids: &[String]) -> HashMap<String, usize> {
        let rooms = self.rooms.read().await;
        stream_ids
            .iter()
            .map(|stream_id| {
                let count = rooms
                    .get(stream_id)
                    .map(RoomState::viewer_count)
                    .unwrap_or(0);
                (stream_id.clone(), count)
            })
            .collect()
    }

    pub async fn play(&self, stream_id: &str, client_id: &str, viewer_key: &str) -> usize {
        let (count, event_sender) = {
            let mut rooms = self.rooms.write().await;
            let room = rooms
                .entry(stream_id.to_string())
                .or_insert_with(RoomState::new);
            let previous_count = room.viewer_count();
            let previous_viewer = room
                .clients
                .insert(client_id.to_string(), viewer_key.to_string());

            match previous_viewer {
                Some(previous_viewer) if previous_viewer == viewer_key => {}
                Some(previous_viewer) => {
                    room.remove_viewer_session(&previous_viewer);
                    room.add_viewer_session(viewer_key.to_string());
                }
                None => room.add_viewer_session(viewer_key.to_string()),
            }

            let count = room.viewer_count();
            let event_sender = (count != previous_count).then(|| room.events.clone());
            (count, event_sender)
        };

        if let Some(event_sender) = event_sender {
            let _ = event_sender.send(RoomEvent::ViewerCount { count });
        }
        count
    }

    pub async fn stop(&self, stream_id: &str, client_id: &str) -> usize {
        let Some((count, event_sender)) = ({
            let mut rooms = self.rooms.write().await;
            let Some(room) = rooms.get_mut(stream_id) else {
                return 0;
            };
            let previous_count = room.viewer_count();
            let Some(viewer_key) = room.clients.remove(client_id) else {
                return previous_count;
            };

            room.remove_viewer_session(&viewer_key);
            let count = room.viewer_count();
            let event_sender = (count != previous_count).then(|| room.events.clone());
            Some((count, event_sender))
        }) else {
            return 0;
        };

        if let Some(event_sender) = event_sender {
            let _ = event_sender.send(RoomEvent::ViewerCount { count });
        }
        count
    }

    pub async fn clear_stream(&self, stream_id: &str) -> usize {
        let event_sender = {
            let mut rooms = self.rooms.write().await;
            let Some(room) = rooms.get_mut(stream_id) else {
                return 0;
            };
            room.clients.clear();
            room.viewer_sessions.clear();
            room.events.clone()
        };

        let _ = event_sender.send(RoomEvent::ViewerCount { count: 0 });
        0
    }

    pub async fn subscribe(&self, stream_id: &str) -> (usize, broadcast::Receiver<RoomEvent>) {
        let mut rooms = self.rooms.write().await;
        let room = rooms
            .entry(stream_id.to_string())
            .or_insert_with(RoomState::new);
        (room.viewer_count(), room.events.subscribe())
    }

    pub async fn broadcast_danmaku(&self, stream_id: &str, event: RoomEvent) {
        let event_sender = self
            .rooms
            .read()
            .await
            .get(stream_id)
            .map(|room| room.events.clone());

        if let Some(event_sender) = event_sender {
            let _ = event_sender.send(event);
        }
    }
}

impl Default for LiveHub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast::error::TryRecvError;

    use super::{LiveHub, RoomEvent};
    use crate::room_access::{ViewerIdentity, ViewerKind};

    fn viewer(name: &str) -> ViewerIdentity {
        ViewerIdentity {
            kind: ViewerKind::Guest,
            name: name.to_string(),
        }
    }

    fn expect_viewer_count(
        receiver: &mut tokio::sync::broadcast::Receiver<RoomEvent>,
        count: usize,
    ) {
        assert_eq!(
            receiver
                .try_recv()
                .expect("viewer count event should be sent"),
            RoomEvent::ViewerCount { count }
        );
    }

    #[tokio::test]
    async fn same_client_and_viewer_is_idempotent() {
        let hub = LiveHub::new();
        let (_, mut receiver) = hub.subscribe("room").await;

        assert_eq!(hub.play("room", "client-a", "viewer-a").await, 1);
        expect_viewer_count(&mut receiver, 1);
        assert_eq!(hub.play("room", "client-a", "viewer-a").await, 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn viewer_with_multiple_clients_counts_once_until_last_stop() {
        let hub = LiveHub::new();
        let (_, mut receiver) = hub.subscribe("room").await;

        assert_eq!(hub.play("room", "client-a", "viewer-a").await, 1);
        expect_viewer_count(&mut receiver, 1);
        assert_eq!(hub.play("room", "client-b", "viewer-a").await, 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(hub.stop("room", "client-a").await, 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(hub.stop("room", "client-b").await, 0);
        expect_viewer_count(&mut receiver, 0);
    }

    #[tokio::test]
    async fn second_viewer_increments_count() {
        let hub = LiveHub::new();
        let (_, mut receiver) = hub.subscribe("room").await;

        assert_eq!(hub.play("room", "client-a", "viewer-a").await, 1);
        expect_viewer_count(&mut receiver, 1);
        assert_eq!(hub.play("room", "client-b", "viewer-b").await, 2);
        expect_viewer_count(&mut receiver, 2);
    }

    #[tokio::test]
    async fn duplicate_and_unknown_stops_do_not_change_count_or_broadcast() {
        let hub = LiveHub::new();
        let (_, mut receiver) = hub.subscribe("room").await;

        assert_eq!(hub.stop("room", "unknown").await, 0);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        hub.play("room", "client-a", "viewer-a").await;
        expect_viewer_count(&mut receiver, 1);
        assert_eq!(hub.stop("room", "client-a").await, 0);
        expect_viewer_count(&mut receiver, 0);
        assert_eq!(hub.stop("room", "client-a").await, 0);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn changing_a_clients_viewer_updates_refcounts_and_only_broadcasts_final_count() {
        let hub = LiveHub::new();
        let (_, mut receiver) = hub.subscribe("room").await;

        hub.play("room", "client-a", "viewer-a").await;
        expect_viewer_count(&mut receiver, 1);
        assert_eq!(hub.play("room", "client-a", "viewer-b").await, 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(hub.play("room", "client-b", "viewer-a").await, 2);
        expect_viewer_count(&mut receiver, 2);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(hub.stop("room", "client-b").await, 1);
        expect_viewer_count(&mut receiver, 1);
        assert_eq!(hub.stop("room", "client-a").await, 0);
        expect_viewer_count(&mut receiver, 0);
    }

    #[tokio::test]
    async fn clear_stream_removes_presence_and_always_broadcasts_zero() {
        let hub = LiveHub::new();
        let (_, mut receiver) = hub.subscribe("room").await;

        hub.play("room", "client-a", "viewer-a").await;
        expect_viewer_count(&mut receiver, 1);
        hub.play("room", "client-b", "viewer-b").await;
        expect_viewer_count(&mut receiver, 2);

        assert_eq!(hub.clear_stream("room").await, 0);
        expect_viewer_count(&mut receiver, 0);
        assert_eq!(hub.stop("room", "client-a").await, 0);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(hub.clear_stream("room").await, 0);
        expect_viewer_count(&mut receiver, 0);
    }

    #[tokio::test]
    async fn dropped_receivers_do_not_prevent_presence_updates() {
        let hub = LiveHub::new();
        let (_, receiver) = hub.subscribe("room").await;
        drop(receiver);

        assert_eq!(hub.play("room", "client-a", "viewer-a").await, 1);
        assert_eq!(hub.viewer_count("room").await, 1);
        assert_eq!(hub.clear_stream("room").await, 0);
        assert_eq!(hub.viewer_count("room").await, 0);
    }

    #[tokio::test]
    async fn viewer_counts_includes_unknown_rooms_and_subscribe_does_not_count() {
        let hub = LiveHub::new();
        let (count, _) = hub.subscribe("empty").await;
        assert_eq!(count, 0);
        assert_eq!(hub.viewer_count("empty").await, 0);

        hub.play("active", "client-a", "viewer-a").await;
        let stream_ids = vec![
            "active".to_string(),
            "empty".to_string(),
            "unknown".to_string(),
        ];
        let counts = hub.viewer_counts(&stream_ids).await;
        assert_eq!(counts.get("active"), Some(&1));
        assert_eq!(counts.get("empty"), Some(&0));
        assert_eq!(counts.get("unknown"), Some(&0));
    }

    #[tokio::test]
    async fn broadcast_danmaku_forwards_event_without_affecting_presence() {
        let hub = LiveHub::new();
        let (_, mut receiver) = hub.subscribe("room").await;
        let event = RoomEvent::Danmaku {
            id: "message-1".to_string(),
            sender: viewer("Guest"),
            content: "hello".to_string(),
            sent_at: "2026-07-20T00:00:00Z".to_string(),
        };

        hub.broadcast_danmaku("room", event.clone()).await;

        assert_eq!(
            receiver.try_recv().expect("danmaku event should be sent"),
            event
        );
        assert_eq!(hub.viewer_count("room").await, 0);
    }
}
