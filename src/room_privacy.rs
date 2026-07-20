use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveModelTrait, DatabaseConnection, DatabaseTransaction, DbErr, EntityTrait, QuerySelect,
    Set, TransactionTrait,
};

use crate::{
    entities::live_room,
    room_access::{prepare_privacy_update, RoomAccessError, RoomPrivacyInput},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomUpdateActor {
    Owner { user_id: i32 },
    Admin,
}

#[derive(Debug, Default)]
pub struct LockedRoomUpdate {
    pub require_login: Option<bool>,
    pub password_enabled: Option<bool>,
    pub password: Option<String>,
    pub title: Option<String>,
    pub user_id: Option<i32>,
    pub stream_id: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug)]
pub enum RoomPrivacyUpdateError {
    NotFound,
    Forbidden,
    Invalid(RoomAccessError),
    Database(DbErr),
}

pub async fn update_room_with_privacy_locked(
    db: &DatabaseConnection,
    room_id: i32,
    actor: RoomUpdateActor,
    patch: LockedRoomUpdate,
    now: DateTime<Utc>,
) -> Result<live_room::Model, RoomPrivacyUpdateError> {
    let transaction = db.begin().await.map_err(RoomPrivacyUpdateError::Database)?;

    let update_result = async {
        let room = live_room::Entity::find_by_id(room_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(RoomPrivacyUpdateError::Database)?
            .ok_or(RoomPrivacyUpdateError::NotFound)?;

        if matches!(actor, RoomUpdateActor::Owner { user_id } if user_id != room.user_id) {
            return Err(RoomPrivacyUpdateError::Forbidden);
        }

        let privacy = prepare_privacy_update(
            &room,
            RoomPrivacyInput {
                require_login: patch.require_login.unwrap_or(room.require_login),
                password_enabled: patch
                    .password_enabled
                    .unwrap_or(!room.password_hash.is_empty()),
                password: patch.password,
            },
        )
        .map_err(RoomPrivacyUpdateError::Invalid)?;

        let mut active: live_room::ActiveModel = room.into();
        active.require_login = Set(privacy.require_login);
        active.password_hash = Set(privacy.password_hash);
        active.access_revision = Set(privacy.access_revision);
        if let Some(title) = patch.title {
            active.title = Set(title);
        }
        if let Some(user_id) = patch.user_id {
            active.user_id = Set(user_id);
        }
        if let Some(stream_id) = patch.stream_id {
            active.stream_id = Set(stream_id);
        }
        if let Some(enabled) = patch.enabled {
            active.enabled = Set(enabled);
        }
        active.updated_at = Set(now.naive_utc());

        active
            .update(&transaction)
            .await
            .map_err(RoomPrivacyUpdateError::Database)
    }
    .await;

    match update_result {
        Ok(room) => transaction
            .commit()
            .await
            .map(|_| room)
            .map_err(RoomPrivacyUpdateError::Database),
        Err(error) => rollback_error(transaction, error).await,
    }
}

async fn rollback_error(
    transaction: DatabaseTransaction,
    original_error: RoomPrivacyUpdateError,
) -> Result<live_room::Model, RoomPrivacyUpdateError> {
    match transaction.rollback().await {
        Ok(_) => Err(original_error),
        Err(rollback_error) => Err(RoomPrivacyUpdateError::Database(rollback_error)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use sea_orm::{ActiveModelTrait, Database, DatabaseConnection, EntityTrait, Set};
    use tokio::sync::Barrier;

    use super::{
        update_room_with_privacy_locked, LockedRoomUpdate, RoomPrivacyUpdateError, RoomUpdateActor,
    };
    use crate::{
        auth::generate_random_string,
        entities::{live_room, user},
        room_access::{admit_room_ticket, issue_room_ticket, ViewerIdentity, ViewerKind},
    };

    const TICKET_SECRET: &str = "room-privacy-postgres-test-secret";

    async fn test_database() -> Option<(String, DatabaseConnection)> {
        let Ok(database_url) = std::env::var("YANTUBE_TEST_DATABASE_URL") else {
            eprintln!("skipping postgres room privacy test; YANTUBE_TEST_DATABASE_URL is not set");
            return None;
        };

        let db = Database::connect(&database_url)
            .await
            .expect("test database should be reachable");
        Some((database_url, db))
    }

    async fn create_fixture(
        db: &DatabaseConnection,
        revision: i32,
        password_hash: String,
    ) -> live_room::Model {
        let suffix = generate_random_string(16);
        let username = format!("privacy_fixture_{suffix}");
        let now = Utc::now().naive_utc();
        let owner = user::ActiveModel {
            username: Set(username.clone()),
            password: Set("fixture-password".to_string()),
            stream_code: Set("fixture-code".to_string()),
            room_title: Set(String::new()),
            role: Set("user".to_string()),
            enabled: Set(true),
            ..Default::default()
        }
        .insert(db)
        .await
        .expect("fixture owner should be created");

        live_room::ActiveModel {
            user_id: Set(owner.id),
            stream_id: Set(format!("privacy-room-{suffix}")),
            title: Set("Privacy fixture".to_string()),
            cover_url: Set(String::new()),
            stream_code: Set("fixture-stream-code".to_string()),
            enabled: Set(true),
            require_login: Set(false),
            password_hash: Set(password_hash),
            access_revision: Set(revision),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(db)
        .await
        .expect("fixture room should be created")
    }

    async fn cleanup_fixture(db: &DatabaseConnection, room: &live_room::Model) {
        let _ = live_room::Entity::delete_by_id(room.id).exec(db).await;
        let _ = user::Entity::delete_by_id(room.user_id).exec(db).await;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn concurrent_privacy_updates_are_serialized() {
        let Some((database_url, db)) = test_database().await else {
            return;
        };
        let room = create_fixture(&db, 11, String::new()).await;
        let barrier = Arc::new(Barrier::new(2));
        let room_id = room.id;
        let owner_id = room.user_id;

        let first_db = Database::connect(&database_url)
            .await
            .expect("first test connection should be reachable");
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            update_room_with_privacy_locked(
                &first_db,
                room_id,
                RoomUpdateActor::Owner { user_id: owner_id },
                LockedRoomUpdate {
                    require_login: Some(true),
                    password_enabled: Some(false),
                    ..Default::default()
                },
                Utc::now(),
            )
            .await
        });

        let second_db = Database::connect(&database_url)
            .await
            .expect("second test connection should be reachable");
        let second_barrier = barrier.clone();
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            update_room_with_privacy_locked(
                &second_db,
                room_id,
                RoomUpdateActor::Admin,
                LockedRoomUpdate {
                    require_login: Some(true),
                    password_enabled: Some(true),
                    password: Some("concurrent-pass".to_string()),
                    ..Default::default()
                },
                Utc::now(),
            )
            .await
        });

        let result = async {
            first
                .await
                .expect("first privacy task should not panic")
                .expect("first privacy update should commit");
            second
                .await
                .expect("second privacy task should not panic")
                .expect("second privacy update should commit");
            let final_room = live_room::Entity::find_by_id(room.id)
                .one(&db)
                .await
                .expect("final room query should succeed")
                .expect("fixture room should remain");
            assert_eq!(final_room.access_revision, 13);
        }
        .await;

        cleanup_fixture(&db, &room).await;
        result
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn each_committed_privacy_change_stales_the_previous_ticket() {
        let Some((_database_url, db)) = test_database().await else {
            return;
        };
        let room = create_fixture(&db, 23, String::new()).await;

        let result = async {
            let ticket_at_initial_revision = issue_room_ticket(
                &room,
                format!("user:{}", room.user_id),
                ViewerIdentity {
                    kind: ViewerKind::User,
                    name: "fixture owner".to_string(),
                },
                Some(room.user_id),
                true,
                false,
                TICKET_SECRET,
                Utc::now(),
            )
            .expect("initial ticket should be created");

            let after_first = update_room_with_privacy_locked(
                &db,
                room.id,
                RoomUpdateActor::Owner {
                    user_id: room.user_id,
                },
                LockedRoomUpdate {
                    require_login: Some(true),
                    password_enabled: Some(false),
                    ..Default::default()
                },
                Utc::now(),
            )
            .await
            .expect("first privacy change should commit");
            assert!(matches!(
                admit_room_ticket(
                    &ticket_at_initial_revision.token,
                    &after_first.stream_id,
                    &after_first,
                    TICKET_SECRET,
                    Utc::now(),
                ),
                Err(crate::room_access::RoomAccessError::StalePolicy)
            ));

            let ticket_at_first_revision = issue_room_ticket(
                &after_first,
                format!("user:{}", after_first.user_id),
                ViewerIdentity {
                    kind: ViewerKind::User,
                    name: "fixture owner".to_string(),
                },
                Some(after_first.user_id),
                true,
                false,
                TICKET_SECRET,
                Utc::now(),
            )
            .expect("ticket after first change should be created");
            let after_second = update_room_with_privacy_locked(
                &db,
                room.id,
                RoomUpdateActor::Admin,
                LockedRoomUpdate {
                    password_enabled: Some(true),
                    password: Some("second-pass".to_string()),
                    ..Default::default()
                },
                Utc::now(),
            )
            .await
            .expect("second privacy change should commit");
            assert!(matches!(
                admit_room_ticket(
                    &ticket_at_first_revision.token,
                    &after_second.stream_id,
                    &after_second,
                    TICKET_SECRET,
                    Utc::now(),
                ),
                Err(crate::room_access::RoomAccessError::StalePolicy)
            ));
            assert_eq!(after_second.access_revision, 25);

            let forbidden = update_room_with_privacy_locked(
                &db,
                room.id,
                RoomUpdateActor::Owner {
                    user_id: room.user_id + 1,
                },
                LockedRoomUpdate {
                    require_login: Some(false),
                    ..Default::default()
                },
                Utc::now(),
            )
            .await;
            assert!(matches!(forbidden, Err(RoomPrivacyUpdateError::Forbidden)));
        }
        .await;

        cleanup_fixture(&db, &room).await;
        result
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn locked_updates_preserve_clear_and_do_not_bump_unchanged_revisions() {
        let Some((_database_url, db)) = test_database().await else {
            return;
        };
        let room = create_fixture(&db, 31, crate::auth::hash_password("preserve-pass")).await;

        let result = async {
            let preserved = update_room_with_privacy_locked(
                &db,
                room.id,
                RoomUpdateActor::Owner {
                    user_id: room.user_id,
                },
                LockedRoomUpdate {
                    password_enabled: Some(true),
                    password: Some(String::new()),
                    ..Default::default()
                },
                Utc::now(),
            )
            .await
            .expect("owner should be allowed to preserve an existing password");
            assert_eq!(preserved.password_hash, room.password_hash);
            assert_eq!(preserved.access_revision, 31);

            let cleared = update_room_with_privacy_locked(
                &db,
                room.id,
                RoomUpdateActor::Admin,
                LockedRoomUpdate {
                    password_enabled: Some(false),
                    ..Default::default()
                },
                Utc::now(),
            )
            .await
            .expect("admin should be allowed to clear a password");
            assert!(cleared.password_hash.is_empty());
            assert_eq!(cleared.access_revision, 32);

            let unchanged = update_room_with_privacy_locked(
                &db,
                room.id,
                RoomUpdateActor::Admin,
                LockedRoomUpdate::default(),
                Utc::now(),
            )
            .await
            .expect("unchanged privacy update should commit");
            assert_eq!(unchanged.access_revision, 32);
        }
        .await;

        cleanup_fixture(&db, &room).await;
        result
    }
}
