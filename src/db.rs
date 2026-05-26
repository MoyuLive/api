use sea_orm::{Database, DatabaseConnection};
use sqlx::migrate::Migrator;
use tracing::info;

static MIGRATOR: Migrator = sqlx::migrate!();

pub async fn init_db(dsn: &str) -> DatabaseConnection {
    info!("Connecting to database...");
    let url = to_postgres_url(dsn);

    let pool = sqlx::PgPool::connect(&url).await.expect("Failed to create pool");
    MIGRATOR.run(&pool).await.expect("Failed to run migrations");
    info!("Database migration complete");

    let db = Database::connect(&url).await.expect("Failed to connect sea-orm");
    db.ping().await.expect("Failed to ping database");
    info!("Database connected successfully");
    db
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
        format!("postgres://{}:{}@{}:{}/{}", user, password, host, port, dbname)
    }
}
