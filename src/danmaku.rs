use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::live_hub::RoomEvent;
use crate::room_access::{RoomTicketClaims, ViewerIdentity, ViewerKind};

const DANMAKU_RATE_LIMIT: Duration = Duration::from_secs(1);

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientMessage {
    SendMessage { content: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DanmakuError {
    InvalidMessage,
    RateLimited,
}

#[derive(Debug, Default)]
pub struct ConnectionRateLimiter {
    last_accepted: Option<Instant>,
}

impl ConnectionRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accept_message(
        &mut self,
        claims: &RoomTicketClaims,
        content: &str,
        now: Instant,
        id: String,
        sent_at: String,
    ) -> Result<RoomEvent, DanmakuError> {
        let content = content.trim();
        if !(1..=100).contains(&content.chars().count()) {
            return Err(DanmakuError::InvalidMessage);
        }

        if self.last_accepted.is_some_and(|last_accepted| {
            now.saturating_duration_since(last_accepted) < DANMAKU_RATE_LIMIT
        }) {
            return Err(DanmakuError::RateLimited);
        }

        self.last_accepted = Some(now);
        let sender = ViewerIdentity {
            kind: if claims.user_id.is_some() && claims.account_verified {
                ViewerKind::User
            } else {
                ViewerKind::Guest
            },
            name: claims.display_name.clone(),
        };
        Ok(RoomEvent::Danmaku {
            id,
            sender,
            content: content.to_string(),
            sent_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{ClientMessage, ConnectionRateLimiter, DanmakuError};
    use crate::live_hub::RoomEvent;
    use crate::room_access::RoomTicketClaims;

    fn claims(
        user_id: Option<i32>,
        account_verified: bool,
        display_name: &str,
    ) -> RoomTicketClaims {
        RoomTicketClaims {
            kind: "room_access".to_string(),
            stream_id: "room-one".to_string(),
            viewer_key: user_id
                .map(|id| format!("user:{id}"))
                .unwrap_or_else(|| "guest:abcdefab-cdef-abcd-efab-cdefabcdefab".to_string()),
            display_name: display_name.to_string(),
            user_id,
            account_verified,
            password_verified: false,
            access_revision: 1,
            iat: 0,
            exp: 1,
        }
    }

    fn accept(
        limiter: &mut ConnectionRateLimiter,
        claims: &RoomTicketClaims,
        content: &str,
        now: Instant,
    ) -> Result<RoomEvent, DanmakuError> {
        limiter.accept_message(
            claims,
            content,
            now,
            "server-message-id".to_string(),
            "2026-07-20T12:00:00+00:00".to_string(),
        )
    }

    #[test]
    fn guest_sender_is_derived_from_ticket_claims() {
        let mut limiter = ConnectionRateLimiter::new();
        let event = accept(
            &mut limiter,
            &claims(None, false, "游客-ABCD"),
            "hello",
            Instant::now(),
        )
        .expect("guest danmaku is accepted");

        assert_eq!(
            event,
            RoomEvent::Danmaku {
                id: "server-message-id".to_string(),
                sender: crate::room_access::ViewerIdentity {
                    kind: crate::room_access::ViewerKind::Guest,
                    name: "游客-ABCD".to_string(),
                },
                content: "hello".to_string(),
                sent_at: "2026-07-20T12:00:00+00:00".to_string(),
            }
        );
    }

    #[test]
    fn verified_user_sender_is_derived_from_ticket_claims() {
        let mut limiter = ConnectionRateLimiter::new();
        let event = accept(
            &mut limiter,
            &claims(Some(42), true, "alice"),
            "hello",
            Instant::now(),
        )
        .expect("user danmaku is accepted");

        assert!(matches!(
            event,
            RoomEvent::Danmaku {
                sender: crate::room_access::ViewerIdentity {
                    kind: crate::room_access::ViewerKind::User,
                    name,
                },
                ..
            } if name == "alice"
        ));
    }

    #[test]
    fn blank_and_101_character_messages_are_invalid_without_consuming_rate_limit() {
        let mut limiter = ConnectionRateLimiter::new();
        let claims = claims(None, false, "guest");
        let now = Instant::now();

        assert_eq!(
            accept(&mut limiter, &claims, "  \t\n", now),
            Err(DanmakuError::InvalidMessage)
        );
        assert_eq!(
            accept(&mut limiter, &claims, &"界".repeat(101), now),
            Err(DanmakuError::InvalidMessage)
        );
        assert!(accept(&mut limiter, &claims, "first", now).is_ok());
    }

    #[test]
    fn valid_messages_trim_content_and_preserve_literal_markup() {
        let mut limiter = ConnectionRateLimiter::new();
        let event = accept(
            &mut limiter,
            &claims(None, false, "guest"),
            "  <b>literal</b>  ",
            Instant::now(),
        )
        .expect("valid message");

        assert!(matches!(
            event,
            RoomEvent::Danmaku { content, .. } if content == "<b>literal</b>"
        ));
    }

    #[test]
    fn successful_message_uses_server_id_and_time_and_enforces_one_second_interval() {
        let mut limiter = ConnectionRateLimiter::new();
        let claims = claims(None, false, "guest");
        let now = Instant::now();

        let event = accept(&mut limiter, &claims, "first", now).expect("first succeeds");
        assert!(matches!(
            event,
            RoomEvent::Danmaku { id, sent_at, .. }
                if id == "server-message-id" && sent_at == "2026-07-20T12:00:00+00:00"
        ));
        assert_eq!(
            accept(
                &mut limiter,
                &claims,
                "second",
                now + Duration::from_millis(999)
            ),
            Err(DanmakuError::RateLimited)
        );
        assert!(accept(&mut limiter, &claims, "third", now + Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn boundary_of_100_unicode_scalar_characters_is_accepted() {
        let mut limiter = ConnectionRateLimiter::new();
        let event = accept(
            &mut limiter,
            &claims(None, false, "guest"),
            &"界".repeat(100),
            Instant::now(),
        )
        .expect("100 unicode scalar characters are accepted");

        assert!(
            matches!(event, RoomEvent::Danmaku { content, .. } if content.chars().count() == 100)
        );
    }

    #[test]
    fn client_message_is_tagged_and_only_accepts_content() {
        assert!(matches!(
            serde_json::from_str::<ClientMessage>(r#"{"type":"send_message","content":"hello"}"#),
            Ok(ClientMessage::SendMessage {
                content,
            }) if content == "hello"
        ));
        assert!(serde_json::from_str::<ClientMessage>(
            r#"{"type":"send_message","content":"hello","id":"client-id"}"#
        )
        .is_err());
    }
}
