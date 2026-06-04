use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};
use tracing::info;

const MIGRATIONS: &[&str] = &[
    include_str!("../migrations/01_create_users.sql"),
    include_str!("../migrations/02_create_srs_servers.sql"),
    include_str!("../migrations/03_create_live_sessions.sql"),
    include_str!("../migrations/04_create_forward_rules.sql"),
    include_str!("../migrations/05_add_user_room_title.sql"),
    include_str!("../migrations/06_create_live_stream_states.sql"),
];

pub async fn init_db(dsn: &str) -> DatabaseConnection {
    info!("Connecting to database...");
    let url = to_postgres_url(dsn);

    let db = Database::connect(&url)
        .await
        .expect("Failed to connect sea-orm");

    run_migrations(&db).await;

    db.ping().await.expect("Failed to ping database");
    info!("Database connected successfully");
    db
}

async fn run_migrations(db: &DatabaseConnection) {
    for (i, sql) in MIGRATIONS.iter().enumerate() {
        db.execute_unprepared(sql)
            .await
            .unwrap_or_else(|e| panic!("Migration {} failed: {}", i + 1, e));
    }
    info!("Database migration complete");
}

fn to_postgres_url(dsn: &str) -> String {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        return dsn.to_string();
    }
    let mut host = "localhost";
    let mut port = "5432";
    let mut user = "";
    let mut password = "";
    let mut dbname = "";
    for part in dsn.split_whitespace() {
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        match key {
            "host" => host = value,
            "port" => port = value,
            "user" => user = value,
            "password" => password = value,
            "dbname" => dbname = value,
            _ => {}
        }
    }
    if password.is_empty() {
        format!("postgres://{}@{}:{}/{}", user, host, port, dbname)
    } else {
        let encoded_pw: String = percent_encode(password.as_bytes(), NON_ALPHANUMERIC).collect();
        format!(
            "postgres://{}:{}@{}:{}/{}",
            user, encoded_pw, host, port, dbname
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bundled_migrations_execute_against_postgres() {
        let Ok(database_url) = std::env::var("YANTUBE_TEST_DATABASE_URL") else {
            eprintln!("skipping postgres migration test; YANTUBE_TEST_DATABASE_URL is not set");
            return;
        };

        let db = Database::connect(&database_url)
            .await
            .expect("test database should be reachable");

        run_migrations(&db).await;
    }
}
