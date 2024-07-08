use std::{net::SocketAddr, sync::Arc};

use axum::{response::IntoResponse, routing::post, Json, Router};
use serde_json::json;

use super::disco::Discovery;

struct BullyState {
  membership: Arc<dyn Discovery>,
}

fn router(membership: Arc<dyn Discovery>) -> Router {
  let state = Arc::new(BullyState { membership });
  Router::new()
    .route("/election", post(election_handler))
    .route("/alive", post(alive_handler))
    .route("/coordinator", post(coordinator_handler))
    .route("/ping", post(ping_handler))
    .with_state(state)
}

async fn election_handler() -> impl IntoResponse {
  Json(json!({ "status": "ok" }))
}

async fn alive_handler() -> impl IntoResponse {
  Json(json!({ "status": "ok" }))
}

async fn ping_handler() -> impl IntoResponse {
  Json(json!({ "status": "ok" }))
}

async fn coordinator_handler() -> impl IntoResponse {
  Json(json!({ "status": "ok" }))
}
