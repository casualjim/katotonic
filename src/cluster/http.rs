use std::{net::SocketAddr, sync::Arc};

use axum::{response::IntoResponse, routing::post, Json, Router};
use serde_json::json;

use super::{disco::Discovery, leader::LeaderElection};

pub struct ServerCtx {
  id: u64,
  addr: SocketAddr,
  election: Arc<dyn LeaderElection>,
  membership: Arc<dyn Discovery>,
}

fn router() -> Router {
  Router::new().route("/", post(handler))
}

async fn get_leader(ctx: Arc<ServerCtx>) -> impl IntoResponse {
  Json(json!({ "leader": ctx.election.leader() }))
}

async fn handler() -> impl IntoResponse {
  Json(json!({ "status": "ok" }))
}
