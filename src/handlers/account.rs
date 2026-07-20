use axum::{extract::State, http::StatusCode, response::IntoResponse, response::Response, Json};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait,
    QueryFilter, Set, Statement, TransactionTrait,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};

use crate::auth::{
    create_jwt, generate_random_string, hash_password, verify_password, ROLE_SUPER_ADMIN, ROLE_USER,
};
use crate::entities::{live_room, user};
use crate::response::{error_response, success_response};
use crate::AppState;

const REGISTRATION_ADVISORY_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(521351665736640)";

#[allow(clippy::result_large_err)]
pub(crate) fn validate_username(username: &str) -> Result<(), (StatusCode, Response)> {
    // Username: 3-32 chars, alphanumeric + underscore only
    if username.len() < 3 || username.len() > 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid credentials"),
        ));
    }
    if !username.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err((
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid credentials"),
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
pub(crate) fn validate_password(password: &str) -> Result<(), (StatusCode, Response)> {
    // Password: minimum 6 characters
    if password.len() < 6 {
        return Err((
            StatusCode::BAD_REQUEST,
            error_response(400, "invalid credentials"),
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
pub(crate) fn validate_credentials(
    username: &str,
    password: &str,
) -> Result<(), (StatusCode, Response)> {
    validate_username(username)?;
    validate_password(password)?;
    Ok(())
}

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
}

fn registration_defaults(existing_users: u64) -> (&'static str, bool) {
    if existing_users == 0 {
        (ROLE_SUPER_ADMIN, true)
    } else {
        (ROLE_USER, false)
    }
}

#[derive(serde::Serialize)]
struct AuthTokenResponse {
    token: String,
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if let Err(e) = validate_credentials(&req.username, &req.password) {
        return e;
    }

    if !state.config.user.allow_register {
        return (
            StatusCode::FORBIDDEN,
            error_response(403, "register is not allowed"),
        );
    }

    let hashed = hash_password(&req.password);
    let stream_code = generate_random_string(16);
    let txn = match state.db.begin().await {
        Ok(txn) => txn,
        Err(e) => {
            error!("Failed to begin create user transaction: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "server save user failed"),
            );
        }
    };

    let created_user = async {
        txn.query_one(Statement::from_string(
            DbBackend::Postgres,
            REGISTRATION_ADVISORY_LOCK_SQL.to_string(),
        ))
        .await?;

        let existing_users = user::Entity::find().count(&txn).await?;
        let (role, create_default_room) = registration_defaults(existing_users);
        let user = user::ActiveModel {
            username: Set(req.username.clone()),
            password: Set(hashed),
            stream_code: Set(stream_code.clone()),
            role: Set(role.to_string()),
            enabled: Set(true),
            ..Default::default()
        };
        let created_user = user.insert(&txn).await?;

        if create_default_room {
            live_room::ActiveModel {
                user_id: Set(created_user.id),
                stream_id: Set(created_user.username.clone()),
                title: Set(String::new()),
                stream_code: Set(stream_code),
                enabled: Set(true),
                ..Default::default()
            }
            .insert(&txn)
            .await?;
        }

        Ok::<_, sea_orm::DbErr>(created_user)
    }
    .await;

    let created_user = match created_user {
        Ok(created_user) => created_user,
        Err(e) => {
            error!("Failed to create user: {}", e);
            if let Err(rollback_error) = txn.rollback().await {
                error!(
                    "Failed to roll back create user transaction: {}",
                    rollback_error
                );
            }
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "server save user failed"),
            );
        }
    };

    if let Err(e) = txn.commit().await {
        error!("Failed to commit create user transaction: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            error_response(500, "server save user failed"),
        );
    }

    let token = match create_jwt(
        created_user.id,
        &created_user.username,
        &created_user.role,
        &state.config.user.auth_secret,
    ) {
        Ok(token) => token,
        Err(e) => {
            error!("Failed to create registration JWT: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to create token"),
            );
        }
    };

    info!("User created");
    (
        StatusCode::OK,
        success_response(AuthTokenResponse { token }),
    )
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    if let Err(e) = validate_credentials(&req.username, &req.password) {
        return e;
    }

    let user = match user::Entity::find()
        .filter(user::Column::Username.eq(&req.username))
        .one(&state.db)
        .await
    {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (
                StatusCode::UNAUTHORIZED,
                error_response(401, "invalid username or password"),
            );
        }
        Err(e) => {
            error!("Failed to get user: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "server get user failed"),
            );
        }
    };

    if !user.enabled {
        return (
            StatusCode::UNAUTHORIZED,
            error_response(401, "invalid username or password"),
        );
    }

    match verify_password(&user.password, &req.password) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::UNAUTHORIZED,
                error_response(401, "invalid username or password"),
            );
        }
        Err(e) => {
            error!("Password verification error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "server can't validate password"),
            );
        }
    }

    let token = match create_jwt(
        user.id,
        &user.username,
        &user.role,
        &state.config.user.auth_secret,
    ) {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to create JWT: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to create token"),
            );
        }
    };

    (
        StatusCode::OK,
        success_response(AuthTokenResponse { token }),
    )
}

pub async fn refresh(
    State(state): State<Arc<AppState>>,
    auth_user: crate::auth::CurrentUser,
) -> impl IntoResponse {
    let token = match create_jwt(
        auth_user.user_id,
        &auth_user.username,
        &auth_user.role,
        &state.config.user.auth_secret,
    ) {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to refresh JWT: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_response(500, "failed to refresh token"),
            );
        }
    };

    (
        StatusCode::OK,
        success_response(AuthTokenResponse { token }),
    )
}

pub async fn logout() -> impl IntoResponse {
    // JWT is stateless - logout is handled client-side by discarding the token.
    // No server-side session invalidation is needed.
    (StatusCode::OK, success_response(serde_json::Value::Null))
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, response::IntoResponse};
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, QueryFilter,
    };
    use tokio::sync::Barrier;

    use super::*;
    use crate::config::{
        AppConfig, DbConfig, MetricsConfig, PlaybackConfig, PublishConfig, SrsConfig,
        StorageConfig, UserConfig,
    };
    use crate::live_hub::LiveHub;
    use crate::srs_client::SrsClient;

    #[test]
    fn registration_defaults_bootstraps_first_user() {
        assert_eq!(registration_defaults(0), (ROLE_SUPER_ADMIN, true),);
    }

    #[test]
    fn registration_defaults_leaves_later_users_without_rooms() {
        assert_eq!(registration_defaults(1), (ROLE_USER, false));
        assert_eq!(registration_defaults(2), (ROLE_USER, false));
    }

    #[test]
    fn registration_uses_a_fixed_transaction_advisory_lock() {
        assert_eq!(
            REGISTRATION_ADVISORY_LOCK_SQL,
            "SELECT pg_advisory_xact_lock(521351665736640)"
        );
    }

    const TEST_MIGRATIONS: &[&str] = &[
        include_str!("../../migrations/01_create_users.sql"),
        include_str!("../../migrations/02_create_srs_servers.sql"),
        include_str!("../../migrations/03_create_live_sessions.sql"),
        include_str!("../../migrations/04_create_forward_rules.sql"),
        include_str!("../../migrations/05_add_user_room_title.sql"),
        include_str!("../../migrations/06_create_live_stream_states.sql"),
        include_str!("../../migrations/07_create_platform_admin_and_live_rooms.sql"),
        include_str!("../../migrations/08_add_live_room_cover_url.sql"),
        include_str!("../../migrations/09_add_live_room_privacy.sql"),
    ];

    fn database_url_for_name(database_url: &str, database_name: &str) -> String {
        let mut url = url::Url::parse(database_url).expect("test database URL must be a URL");
        url.set_path(&format!("/{database_name}"));
        url.to_string()
    }

    async fn run_bundled_migrations(db: &DatabaseConnection) -> Result<(), sea_orm::DbErr> {
        for migration in TEST_MIGRATIONS {
            db.execute_unprepared(migration).await?;
        }
        Ok(())
    }

    fn registration_test_state(db: DatabaseConnection) -> Arc<AppState> {
        Arc::new(AppState {
            db,
            config: Arc::new(AppConfig {
                http_port: 9081,
                db: DbConfig {
                    dsn: "mock".to_string(),
                },
                user: UserConfig {
                    allow_register: true,
                    auth_realm: "stream api".to_string(),
                    auth_secret: "account-registration-test-secret".to_string(),
                },
                srs: SrsConfig {
                    api_url: "http://srs:1985".to_string(),
                    api_user: "admin".to_string(),
                    api_password: "password".to_string(),
                    callback_secret: "callback-secret".to_string(),
                },
                playback: PlaybackConfig {
                    protocols: "webrtc,hls".to_string(),
                },
                publish: PublishConfig {
                    protocols: "rtmp,whip".to_string(),
                },
                storage: StorageConfig {
                    upload_dir: "uploads-test".to_string(),
                },
                metrics: MetricsConfig { enabled: false },
                cors_origins: vec!["http://localhost:5173".to_string()],
            }),
            srs_client: Arc::new(SrsClient::new(
                "http://srs:1985".to_string(),
                "admin".to_string(),
                "password".to_string(),
            )),
            live_hub: Arc::new(LiveHub::new()),
        })
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL and YANTUBE_TEST_DATABASE_URL"]
    async fn concurrent_empty_database_registrations_create_one_super_admin_and_room() {
        let Ok(base_url) = std::env::var("YANTUBE_TEST_DATABASE_URL") else {
            eprintln!("skipping postgres registration test; YANTUBE_TEST_DATABASE_URL is not set");
            return;
        };
        let database_name = format!(
            "yantube_registration_test_{}",
            generate_random_string(16).to_ascii_lowercase()
        );
        let admin_url = database_url_for_name(&base_url, "postgres");
        let test_url = database_url_for_name(&base_url, &database_name);
        let admin = Database::connect(&admin_url)
            .await
            .expect("Postgres admin database should be reachable");
        admin
            .execute_unprepared(&format!("CREATE DATABASE \"{database_name}\""))
            .await
            .expect("isolated test database should be created");

        let db = match Database::connect(&test_url).await {
            Ok(db) => db,
            Err(error) => {
                let _ = admin
                    .execute_unprepared(&format!("DROP DATABASE \"{database_name}\" WITH (FORCE)"))
                    .await;
                panic!("isolated test database should be reachable: {error}");
            }
        };
        if let Err(error) = run_bundled_migrations(&db).await {
            let _ = admin
                .execute_unprepared(&format!("DROP DATABASE \"{database_name}\" WITH (FORCE)"))
                .await;
            panic!("bundled migrations should succeed in isolated database: {error}");
        }

        let barrier = Arc::new(Barrier::new(2));
        let first_state = registration_test_state(
            Database::connect(&test_url)
                .await
                .expect("first registration connection should be reachable"),
        );
        let first_barrier = barrier.clone();
        let first = async move {
            first_barrier.wait().await;
            create(
                State(first_state),
                Json(CreateUserRequest {
                    username: "concurrent_account_a".to_string(),
                    password: "password1".to_string(),
                }),
            )
            .await
            .into_response()
            .status()
        };
        let second_state = registration_test_state(
            Database::connect(&test_url)
                .await
                .expect("second registration connection should be reachable"),
        );
        let second_barrier = barrier;
        let second = async move {
            second_barrier.wait().await;
            create(
                State(second_state),
                Json(CreateUserRequest {
                    username: "concurrent_account_b".to_string(),
                    password: "password1".to_string(),
                }),
            )
            .await
            .into_response()
            .status()
        };
        let (first_status, second_status) = tokio::join!(first, second);

        let result = async {
            if first_status != StatusCode::OK || second_status != StatusCode::OK {
                return Err(format!(
                    "both concurrent registrations must succeed, got {first_status} and {second_status}"
                ));
            }

            let users = user::Entity::find()
                .all(&db)
                .await
                .map_err(|error| format!("users should be queryable: {error}"))?;
            let super_admins: Vec<_> = users
                .iter()
                .filter(|user| user.role == ROLE_SUPER_ADMIN)
                .collect();
            if super_admins.len() != 1 || users.len() != 2 {
                return Err(format!(
                    "expected two users with one super admin, found {} users and {} super admins",
                    users.len(),
                    super_admins.len()
                ));
            }
            if users.iter().filter(|user| user.role == ROLE_USER).count() != 1 {
                return Err("exactly one concurrent registration must remain a regular user".to_string());
            }

            let rooms = live_room::Entity::find()
                .all(&db)
                .await
                .map_err(|error| format!("rooms should be queryable: {error}"))?;
            if rooms.len() != 1 || rooms[0].user_id != super_admins[0].id {
                return Err("the super admin must own the only default room".to_string());
            }
            let regular_user = users
                .iter()
                .find(|user| user.role == ROLE_USER)
                .expect("regular user checked above");
            let regular_user_room_count = live_room::Entity::find()
                .filter(live_room::Column::UserId.eq(regular_user.id))
                .count(&db)
                .await
                .map_err(|error| format!("regular user rooms should be queryable: {error}"))?;
            if regular_user_room_count != 0 {
                return Err("the regular user must not receive a default room".to_string());
            }

            Ok::<_, String>(())
        }
        .await;

        admin
            .execute_unprepared(&format!("DROP DATABASE \"{database_name}\" WITH (FORCE)"))
            .await
            .expect("isolated test database should be dropped");
        result.expect("concurrent registration contract should hold");
    }

    #[tokio::test]
    async fn auth_token_response_is_exposed_in_success_data() {
        let response = success_response(AuthTokenResponse {
            token: "signed-jwt".to_string(),
        });
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("response body should be readable");
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("response should be JSON");

        assert_eq!(json["code"], 0);
        assert_eq!(json["data"]["token"], "signed-jwt");
        assert!(json["data"]["token"]
            .as_str()
            .is_some_and(|token| !token.is_empty()));
    }
}
