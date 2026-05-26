use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub code: i32,
    pub msg: String,
    pub data: T,
}

pub fn success_response<T: Serialize>(data: T) -> Response {
    Json(ApiResponse {
        code: 0,
        msg: "success".into(),
        data,
    })
    .into_response()
}

pub fn error_response(code: i32, msg: impl Into<String>) -> Response {
    Json(ApiResponse {
        code,
        msg: msg.into(),
        data: serde_json::Value::Null,
    })
    .into_response()
}
