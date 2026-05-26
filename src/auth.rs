use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    Json,
};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

const JWT_EXPIRATION_HOURS: i64 = 1;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub username: String,
    pub exp: usize,
    pub iat: usize,
}

#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub username: String,
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for CurrentUser
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));

        let token = match auth_header {
            Some(t) => t,
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "code": 401,
                        "msg": "missing authorization header",
                        "data": null
                    })),
                ));
            }
        };

        let secret = parts
            .extensions
            .get::<JwtSecret>()
            .map(|s| s.0.as_str())
            .unwrap_or("");

        let token_data =
            decode::<Claims>(token, &DecodingKey::from_secret(secret.as_bytes()), &Validation::default())
                .map_err(|_| {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(serde_json::json!({
                            "code": 401,
                            "msg": "invalid token",
                            "data": null
                        })),
                    )
                })?;

        Ok(CurrentUser {
            username: token_data.claims.username,
        })
    }
}

#[derive(Clone)]
pub struct JwtSecret(pub String);

pub fn create_jwt(username: &str, secret: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let now = Utc::now();
    let claims = Claims {
        username: username.to_string(),
        iat: now.timestamp() as usize,
        exp: (now + Duration::hours(JWT_EXPIRATION_HOURS)).timestamp() as usize,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

// PBKDF2 password hashing - compatible with Go implementation
// Format: sha256$32$100000$<salt_base64>$<hash_base64>

pub fn hash_password(password: &str) -> String {
    let mut salt = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut salt);

    let mut hash = [0u8; 32]; // SHA256 output
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, 100_000, &mut hash);

    let salt_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &salt);
    let hash_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &hash);

    format!("sha256$32$100000${}${}", salt_b64, hash_b64)
}

pub fn verify_password(stored: &str, password: &str) -> Result<bool, String> {
    let parts: Vec<&str> = stored.split('$').collect();
    if parts.len() != 5 {
        return Err("invalid pbkdf2 format".into());
    }

    let hash_algo = parts[0];
    if hash_algo != "sha256" {
        return Err(format!("unsupported hash algorithm: {}", hash_algo));
    }

    let _key_len: usize = parts[1].parse().map_err(|_| "invalid key length")?;
    let iterations: u32 = parts[2].parse().map_err(|_| "invalid iterations")?;

    let salt = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, parts[3])
        .map_err(|e| format!("invalid salt base64: {}", e))?;

    let stored_hash = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, parts[4])
        .map_err(|e| format!("invalid hash base64: {}", e))?;

    let mut computed_hash = vec![0u8; stored_hash.len()];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut computed_hash);

    Ok(computed_hash == stored_hash)
}

pub fn generate_random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut result = String::with_capacity(length);
    let mut rng = rand::rngs::OsRng;
    for _ in 0..length {
        let idx = (rng.next_u32() as usize) % CHARSET.len();
        result.push(CHARSET[idx] as char);
    }
    result
}
