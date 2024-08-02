pub mod handler;
#[cfg(feature = "smol")] mod smol;
mod tokio;

use std::sync::Arc;

use async_trait::async_trait;
use chitchat::{spawn_chitchat, transport::UdpTransport, ChitchatHandle};
use serde::{Deserialize, Serialize};
#[cfg(feature = "smol")] pub use smol::run_server as run_smol_server;
pub use tokio::run_server;
use tracing::info;

use crate::{disco::ChitchatDiscovery, Result};

pub async fn spawn_discovery(
  conf: crate::ServerConfig,
) -> anyhow::Result<(ChitchatHandle, Arc<ChitchatDiscovery>)> {
  let chitchat_config: chitchat::ChitchatConfig = (&conf).into();
  info!(
    "chitchat config cluster_id={} chitchat_id={:?} seed_nodes={:?}",
    chitchat_config.cluster_id, chitchat_config.chitchat_id, chitchat_config.seed_nodes
  );

  let api_server_name = conf.server_name();
  let cluster_server_name = conf
    .cluster_server_name()
    .unwrap_or(api_server_name.to_string());

  let tags: Vec<(String, String)> = vec![
    ("role".to_string(), "server".to_string()),
    (
      "cluster_addr".to_string(),
      conf
        .cluster_addr
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or("".to_string()),
    ),
    ("api_addr".to_string(), conf.addr.clone()),
    ("api_server_name".to_string(), api_server_name.to_string()),
    ("cluster_server_name".to_string(), cluster_server_name),
  ];

  let handle = spawn_chitchat(chitchat_config, tags, &UdpTransport).await?;
  let cc = handle.chitchat();

  Ok((handle, Arc::new(ChitchatDiscovery::new(cc, conf))))
}

#[async_trait]
pub trait Handler: Send + Sync {
  type Stream: Send + Sync;

  async fn handle(&self, request: &mut Self::Stream) -> Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RedirectInfo {
  pub leader: String,
  pub followers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Error {
  pub code: u16,
  pub message: String,
}
