use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use sea_orm::{DatabaseConnection, EntityTrait};
use serde::{Deserialize, Serialize};

use crate::{
    auth,
    entities::{live_room, user},
};

pub const ROOM_TICKET_KIND: &str = "room_access";
pub const ROOM_TICKET_TTL_SECONDS: i64 = 15 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewerKind {
    User,
    Guest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ViewerIdentity {
    pub kind: ViewerKind,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RoomTicketClaims {
    pub kind: String,
    pub stream_id: String,
    pub viewer_key: String,
    pub display_name: String,
    pub user_id: Option<i32>,
    pub account_verified: bool,
    pub password_verified: bool,
    pub access_revision: i32,
    pub iat: i64,
    pub exp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IssuedRoomTicket {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub viewer: ViewerIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RoomPrivacyInput {
    pub require_login: bool,
    pub password_enabled: bool,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PrivacyMutation {
    pub require_login: bool,
    pub password_hash: String,
    pub access_revision: i32,
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomAccessError {
    MalformedGuestId,
    MalformedPassword,
    AccountRequired,
    PasswordDenied,
    InvalidTicket,
    ExpiredTicket,
    WrongRoom,
    StalePolicy,
    Internal,
}

// The ticket format is a fixed cross-handler contract and needs these independent claims.
#[allow(clippy::too_many_arguments)]
pub fn issue_room_ticket(
    room: &live_room::Model,
    viewer_key: String,
    viewer: ViewerIdentity,
    user_id: Option<i32>,
    account_verified: bool,
    password_verified: bool,
    secret: &str,
    now: DateTime<Utc>,
) -> Result<IssuedRoomTicket, RoomAccessError> {
    let viewer_key = normalize_viewer_identity(viewer_key, viewer.kind, user_id, account_verified)?;
    let expires_at = now + Duration::seconds(ROOM_TICKET_TTL_SECONDS);
    let claims = RoomTicketClaims {
        kind: ROOM_TICKET_KIND.to_string(),
        stream_id: room.stream_id.clone(),
        viewer_key,
        display_name: viewer.name.clone(),
        user_id,
        account_verified,
        password_verified,
        access_revision: room.access_revision,
        iat: now.timestamp(),
        exp: expires_at.timestamp(),
    };
    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|_| RoomAccessError::Internal)?;

    Ok(IssuedRoomTicket {
        token,
        expires_at,
        viewer,
    })
}

#[allow(dead_code)]
pub fn admit_room_ticket(
    token: &str,
    expected_stream_id: &str,
    room: &live_room::Model,
    secret: &str,
    now: DateTime<Utc>,
) -> Result<RoomTicketClaims, RoomAccessError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = false;
    validation.required_spec_claims.clear();
    let claims = decode::<RoomTicketClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|_| RoomAccessError::InvalidTicket)?
    .claims;

    if claims.exp <= now.timestamp() {
        return Err(RoomAccessError::ExpiredTicket);
    }
    if claims.kind != ROOM_TICKET_KIND {
        return Err(RoomAccessError::InvalidTicket);
    }
    if claims.stream_id != expected_stream_id || expected_stream_id != room.stream_id {
        return Err(RoomAccessError::WrongRoom);
    }
    if claims.access_revision != room.access_revision {
        return Err(RoomAccessError::StalePolicy);
    }

    normalize_viewer_identity(
        claims.viewer_key.clone(),
        viewer_kind_from_key(&claims.viewer_key)?,
        claims.user_id,
        claims.account_verified,
    )?;
    evaluate_room_policy(
        room.require_login,
        !room.password_hash.is_empty(),
        claims.account_verified,
        claims.password_verified,
    )?;

    Ok(claims)
}

pub async fn admit_room_ticket_with_account_check(
    db: &DatabaseConnection,
    token: &str,
    expected_stream_id: &str,
    room: &live_room::Model,
    secret: &str,
    now: DateTime<Utc>,
) -> Result<RoomTicketClaims, RoomAccessError> {
    let claims = admit_room_ticket(token, expected_stream_id, room, secret, now)?;
    if !claims.account_verified {
        return Ok(claims);
    }

    let user_id = claims.user_id.ok_or(RoomAccessError::InvalidTicket)?;
    match user::Entity::find_by_id(user_id).one(db).await {
        Ok(Some(user)) if user.enabled => Ok(claims),
        Ok(Some(_)) | Ok(None) => Err(RoomAccessError::InvalidTicket),
        Err(_) => Err(RoomAccessError::Internal),
    }
}

pub fn evaluate_room_policy(
    require_login: bool,
    has_password: bool,
    account_verified: bool,
    password_verified: bool,
) -> Result<(), RoomAccessError> {
    if require_login && !account_verified {
        return Err(RoomAccessError::AccountRequired);
    }
    if has_password && !password_verified {
        return Err(RoomAccessError::PasswordDenied);
    }
    Ok(())
}

pub fn normalize_guest_id(guest_id: &str) -> Result<String, RoomAccessError> {
    let bytes = guest_id.as_bytes();
    if bytes.len() != 36 {
        return Err(RoomAccessError::MalformedGuestId);
    }

    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return Err(RoomAccessError::MalformedGuestId);
            }
        } else if !byte.is_ascii_hexdigit() {
            return Err(RoomAccessError::MalformedGuestId);
        }
    }

    Ok(guest_id.to_ascii_lowercase())
}

pub fn guest_display_name(guest_id: &str) -> String {
    let prefix: String = guest_id
        .chars()
        .filter(|character| *character != '-')
        .take(4)
        .collect();
    format!("游客-{}", prefix.to_ascii_uppercase())
}

pub fn prepare_privacy_update(
    room: &live_room::Model,
    input: RoomPrivacyInput,
) -> Result<PrivacyMutation, RoomAccessError> {
    let current_has_password = !room.password_hash.is_empty();
    let password_hash = if !input.password_enabled {
        String::new()
    } else if let Some(password) = input.password.filter(|password| !password.is_empty()) {
        let password_length = password.chars().count();
        if !(6..=64).contains(&password_length) {
            return Err(RoomAccessError::MalformedPassword);
        }

        if current_has_password {
            match auth::verify_password(&room.password_hash, &password) {
                Ok(true) => room.password_hash.clone(),
                Ok(false) => auth::hash_password(&password),
                Err(_) => return Err(RoomAccessError::Internal),
            }
        } else {
            auth::hash_password(&password)
        }
    } else if current_has_password {
        room.password_hash.clone()
    } else {
        return Err(RoomAccessError::MalformedPassword);
    };

    let changed = room.require_login != input.require_login || room.password_hash != password_hash;
    let access_revision = if changed {
        room.access_revision
            .checked_add(1)
            .ok_or(RoomAccessError::Internal)?
    } else {
        room.access_revision
    };

    Ok(PrivacyMutation {
        require_login: input.require_login,
        password_hash,
        access_revision,
        changed,
    })
}

#[allow(dead_code)]
fn viewer_kind_from_key(viewer_key: &str) -> Result<ViewerKind, RoomAccessError> {
    if viewer_key.starts_with("user:") {
        Ok(ViewerKind::User)
    } else if viewer_key.starts_with("guest:") {
        Ok(ViewerKind::Guest)
    } else {
        Err(RoomAccessError::InvalidTicket)
    }
}

fn normalize_viewer_identity(
    viewer_key: String,
    viewer_kind: ViewerKind,
    user_id: Option<i32>,
    account_verified: bool,
) -> Result<String, RoomAccessError> {
    match viewer_kind {
        ViewerKind::User => {
            let user_id = user_id.ok_or(RoomAccessError::InvalidTicket)?;
            if !account_verified || viewer_key != format!("user:{user_id}") {
                return Err(RoomAccessError::InvalidTicket);
            }
            Ok(viewer_key)
        }
        ViewerKind::Guest => {
            if user_id.is_some() || account_verified {
                return Err(RoomAccessError::InvalidTicket);
            }
            let guest_id = viewer_key
                .strip_prefix("guest:")
                .ok_or(RoomAccessError::InvalidTicket)?;
            let canonical_id =
                normalize_guest_id(guest_id).map_err(|_| RoomAccessError::InvalidTicket)?;
            let canonical_key = format!("guest:{canonical_id}");
            if viewer_key != canonical_key {
                return Err(RoomAccessError::InvalidTicket);
            }
            Ok(canonical_key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use sea_orm::{DbBackend, DbErr, MockDatabase};

    const SECRET: &str = "room-access-test-secret";

    fn now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 20, 12, 0, 0)
            .single()
            .expect("valid test timestamp")
    }

    fn room() -> crate::entities::live_room::Model {
        crate::entities::live_room::Model {
            id: 1,
            user_id: 2,
            stream_id: "room-one".into(),
            title: "Room one".into(),
            cover_url: String::new(),
            stream_code: "stream-code".into(),
            enabled: true,
            require_login: false,
            password_hash: String::new(),
            access_revision: 7,
            created_at: now().naive_utc(),
            updated_at: now().naive_utc(),
        }
    }

    fn user_viewer() -> ViewerIdentity {
        ViewerIdentity {
            kind: ViewerKind::User,
            name: "alice".into(),
        }
    }

    fn issue_user(room: &crate::entities::live_room::Model) -> IssuedRoomTicket {
        issue_room_ticket(
            room,
            "user:42".into(),
            user_viewer(),
            Some(42),
            true,
            !room.password_hash.is_empty(),
            SECRET,
            now(),
        )
        .expect("user ticket")
    }

    fn valid_claims(room: &crate::entities::live_room::Model) -> RoomTicketClaims {
        RoomTicketClaims {
            kind: ROOM_TICKET_KIND.into(),
            stream_id: room.stream_id.clone(),
            viewer_key: "user:42".into(),
            display_name: "alice".into(),
            user_id: Some(42),
            account_verified: true,
            password_verified: !room.password_hash.is_empty(),
            access_revision: room.access_revision,
            iat: now().timestamp(),
            exp: now().timestamp() + ROOM_TICKET_TTL_SECONDS,
        }
    }

    fn encode_claims(claims: &RoomTicketClaims, secret: &str) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("encode test ticket")
    }

    fn ticket_account(enabled: bool) -> crate::entities::user::Model {
        crate::entities::user::Model {
            id: 42,
            username: "ticket-account".to_string(),
            password: "hash".to_string(),
            stream_code: "stream-code".to_string(),
            room_title: String::new(),
            role: crate::auth::ROLE_USER.to_string(),
            enabled,
        }
    }

    #[test]
    fn policy_allows_everyone_without_password() {
        assert_eq!(evaluate_room_policy(false, false, false, false), Ok(()));
    }

    #[test]
    fn policy_requires_password_without_login() {
        assert_eq!(
            evaluate_room_policy(false, true, false, false),
            Err(RoomAccessError::PasswordDenied)
        );
        assert_eq!(evaluate_room_policy(false, true, false, true), Ok(()));
    }

    #[test]
    fn policy_requires_login_before_password() {
        assert_eq!(
            evaluate_room_policy(true, false, false, false),
            Err(RoomAccessError::AccountRequired)
        );
        assert_eq!(
            evaluate_room_policy(true, true, false, false),
            Err(RoomAccessError::AccountRequired)
        );
        assert_eq!(
            evaluate_room_policy(true, true, true, false),
            Err(RoomAccessError::PasswordDenied)
        );
        assert_eq!(evaluate_room_policy(true, true, true, true), Ok(()));
    }

    #[test]
    fn ticket_is_valid_until_but_not_at_fifteen_minute_expiry() {
        let room = room();
        let issued = issue_user(&room);
        assert_eq!(
            issued.expires_at,
            now() + chrono::Duration::seconds(ROOM_TICKET_TTL_SECONDS)
        );
        assert!(admit_room_ticket(
            &issued.token,
            &room.stream_id,
            &room,
            SECRET,
            now() + chrono::Duration::seconds(ROOM_TICKET_TTL_SECONDS - 1),
        )
        .is_ok());
        assert_eq!(
            admit_room_ticket(
                &issued.token,
                &room.stream_id,
                &room,
                SECRET,
                now() + chrono::Duration::seconds(ROOM_TICKET_TTL_SECONDS),
            ),
            Err(RoomAccessError::ExpiredTicket)
        );
    }

    #[test]
    fn ticket_rejects_wrong_signature_kind_room_and_revision() {
        let room = room();
        let issued = issue_user(&room);
        assert_eq!(
            admit_room_ticket(&issued.token, &room.stream_id, &room, "wrong-secret", now()),
            Err(RoomAccessError::InvalidTicket)
        );

        let mut wrong_kind = valid_claims(&room);
        wrong_kind.kind = "account_access".into();
        assert_eq!(
            admit_room_ticket(
                &encode_claims(&wrong_kind, SECRET),
                &room.stream_id,
                &room,
                SECRET,
                now(),
            ),
            Err(RoomAccessError::InvalidTicket)
        );

        let mut other_room = room.clone();
        other_room.stream_id = "other-room".into();
        assert_eq!(
            admit_room_ticket(
                &issued.token,
                &other_room.stream_id,
                &other_room,
                SECRET,
                now(),
            ),
            Err(RoomAccessError::WrongRoom)
        );

        let mut changed_room = room.clone();
        changed_room.access_revision += 1;
        assert_eq!(
            admit_room_ticket(
                &issued.token,
                &changed_room.stream_id,
                &changed_room,
                SECRET,
                now(),
            ),
            Err(RoomAccessError::StalePolicy)
        );
    }

    #[test]
    fn ticket_rechecks_account_and_password_attestations() {
        let mut login_room = room();
        login_room.require_login = true;
        let mut missing_account = valid_claims(&login_room);
        missing_account.account_verified = false;
        missing_account.user_id = None;
        missing_account.viewer_key = "guest:abcdefab-cdef-abcd-efab-cdefabcdefab".into();
        assert_eq!(
            admit_room_ticket(
                &encode_claims(&missing_account, SECRET),
                &login_room.stream_id,
                &login_room,
                SECRET,
                now(),
            ),
            Err(RoomAccessError::AccountRequired)
        );

        let mut password_room = room();
        password_room.password_hash = crate::auth::hash_password("secret1");
        let mut missing_password = valid_claims(&password_room);
        missing_password.password_verified = false;
        assert_eq!(
            admit_room_ticket(
                &encode_claims(&missing_password, SECRET),
                &password_room.stream_id,
                &password_room,
                SECRET,
                now(),
            ),
            Err(RoomAccessError::PasswordDenied)
        );
    }

    #[tokio::test]
    async fn account_ticket_admission_rechecks_account_state_but_skips_guests() {
        let room = room();
        let ticket = issue_user(&room);

        let enabled_db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[ticket_account(true)]])
            .into_connection();
        assert!(admit_room_ticket_with_account_check(
            &enabled_db,
            &ticket.token,
            &room.stream_id,
            &room,
            SECRET,
            now(),
        )
        .await
        .is_ok());

        let disabled_db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[ticket_account(false)]])
            .into_connection();
        assert_eq!(
            admit_room_ticket_with_account_check(
                &disabled_db,
                &ticket.token,
                &room.stream_id,
                &room,
                SECRET,
                now(),
            )
            .await,
            Err(RoomAccessError::InvalidTicket)
        );

        let deleted_db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([Vec::<crate::entities::user::Model>::new()])
            .into_connection();
        assert_eq!(
            admit_room_ticket_with_account_check(
                &deleted_db,
                &ticket.token,
                &room.stream_id,
                &room,
                SECRET,
                now(),
            )
            .await,
            Err(RoomAccessError::InvalidTicket)
        );

        let error_db = MockDatabase::new(DbBackend::Postgres)
            .append_query_errors([DbErr::Custom("database unavailable".to_string())])
            .into_connection();
        assert_eq!(
            admit_room_ticket_with_account_check(
                &error_db,
                &ticket.token,
                &room.stream_id,
                &room,
                SECRET,
                now(),
            )
            .await,
            Err(RoomAccessError::Internal)
        );

        let guest_ticket = issue_room_ticket(
            &room,
            "guest:abcdefab-cdef-abcd-efab-cdefabcdefab".to_string(),
            ViewerIdentity {
                kind: ViewerKind::Guest,
                name: "guest".to_string(),
            },
            None,
            false,
            false,
            SECRET,
            now(),
        )
        .expect("guest ticket");
        let guest_db = MockDatabase::new(DbBackend::Postgres).into_connection();
        assert!(admit_room_ticket_with_account_check(
            &guest_db,
            &guest_ticket.token,
            &room.stream_id,
            &room,
            SECRET,
            now(),
        )
        .await
        .is_ok());
    }

    #[test]
    fn ticket_rejects_inconsistent_viewer_identity() {
        let room = room();
        let mut claims = valid_claims(&room);
        claims.viewer_key = "user:99".into();
        assert_eq!(
            admit_room_ticket(
                &encode_claims(&claims, SECRET),
                &room.stream_id,
                &room,
                SECRET,
                now(),
            ),
            Err(RoomAccessError::InvalidTicket)
        );

        let mut guest_claims = valid_claims(&room);
        guest_claims.viewer_key = "guest:abcdefab-cdef-abcd-efab-cdefabcdefab".into();
        guest_claims.user_id = None;
        guest_claims.account_verified = true;
        assert_eq!(
            admit_room_ticket(
                &encode_claims(&guest_claims, SECRET),
                &room.stream_id,
                &room,
                SECRET,
                now(),
            ),
            Err(RoomAccessError::InvalidTicket)
        );
    }

    #[test]
    fn normalizes_canonical_uppercase_guest_uuid_and_formats_display_name() {
        let id = normalize_guest_id("ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB")
            .expect("canonical UUID accepts uppercase ASCII hex");
        assert_eq!(id, "abcdefab-cdef-abcd-efab-cdefabcdefab");
        assert_eq!(guest_display_name(&id), "游客-ABCD");
        assert_eq!(
            normalize_guest_id("abcdefabcdefabcdefabcdefabcdefab"),
            Err(RoomAccessError::MalformedGuestId)
        );
    }

    #[test]
    fn privacy_rejects_passwords_outside_unicode_scalar_range() {
        let room = room();
        for password in ["12345", &"界".repeat(65)] {
            assert_eq!(
                prepare_privacy_update(
                    &room,
                    RoomPrivacyInput {
                        require_login: false,
                        password_enabled: true,
                        password: Some(password.to_string()),
                    },
                ),
                Err(RoomAccessError::MalformedPassword)
            );
        }

        for password in ["123456", &"界".repeat(64)] {
            assert!(prepare_privacy_update(
                &room,
                RoomPrivacyInput {
                    require_login: false,
                    password_enabled: true,
                    password: Some(password.to_string()),
                },
            )
            .is_ok());
        }
    }

    #[test]
    fn privacy_preserves_rejects_disables_and_tracks_real_changes() {
        let base_room = room();
        assert_eq!(
            prepare_privacy_update(
                &base_room,
                RoomPrivacyInput {
                    require_login: false,
                    password_enabled: true,
                    password: Some(String::new()),
                },
            ),
            Err(RoomAccessError::MalformedPassword)
        );

        let login_required = prepare_privacy_update(
            &base_room,
            RoomPrivacyInput {
                require_login: true,
                password_enabled: false,
                password: None,
            },
        )
        .expect("enable login requirement");
        assert_eq!(
            login_required.access_revision,
            base_room.access_revision + 1
        );
        assert!(login_required.changed);

        let mut protected_room = room();
        protected_room.password_hash = crate::auth::hash_password("secret1");
        let preserved = prepare_privacy_update(
            &protected_room,
            RoomPrivacyInput {
                require_login: false,
                password_enabled: true,
                password: Some(String::new()),
            },
        )
        .expect("preserve password");
        assert_eq!(preserved.password_hash, protected_room.password_hash);
        assert_eq!(preserved.access_revision, protected_room.access_revision);
        assert!(!preserved.changed);

        let disabled = prepare_privacy_update(
            &protected_room,
            RoomPrivacyInput {
                require_login: false,
                password_enabled: false,
                password: None,
            },
        )
        .expect("disable password");
        assert!(disabled.password_hash.is_empty());
        assert_eq!(disabled.access_revision, protected_room.access_revision + 1);
        assert!(disabled.changed);

        let same = prepare_privacy_update(
            &protected_room,
            RoomPrivacyInput {
                require_login: false,
                password_enabled: true,
                password: Some("secret1".into()),
            },
        )
        .expect("same password");
        assert_eq!(same.password_hash, protected_room.password_hash);
        assert_eq!(same.access_revision, protected_room.access_revision);
        assert!(!same.changed);

        let different = prepare_privacy_update(
            &protected_room,
            RoomPrivacyInput {
                require_login: true,
                password_enabled: true,
                password: Some("different1".into()),
            },
        )
        .expect("change privacy");
        assert_ne!(different.password_hash, protected_room.password_hash);
        assert_eq!(
            different.access_revision,
            protected_room.access_revision + 1
        );
        assert!(different.changed);
    }

    #[test]
    fn privacy_rejects_revision_overflow() {
        let mut room = room();
        room.access_revision = i32::MAX;
        assert_eq!(
            prepare_privacy_update(
                &room,
                RoomPrivacyInput {
                    require_login: true,
                    password_enabled: false,
                    password: None,
                },
            ),
            Err(RoomAccessError::Internal)
        );
    }
}
