use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chitchat::{spawn_chitchat, transport::UdpTransport};
use clap::Parser;
use futures::StreamExt;
use tokio::time::sleep;
use tracing::{info, instrument};
use ulidd::{
  bully,
  disco::ChitchatDiscovery,
  protocol::{Request, Response},
  server::{self, Handler},
};

struct MonotonicHandler(Arc<tokio::sync::Mutex<ulid::Generator>>);

impl std::fmt::Debug for MonotonicHandler {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("MonotonicHandler").finish()
  }
}

impl MonotonicHandler {
  fn new(generator: ulid::Generator) -> Self {
    Self(Arc::new(tokio::sync::Mutex::new(generator)))
  }

  async fn generate(&self) -> Result<ulid::Ulid, ulid::MonotonicError> {
    self.0.lock().await.generate()
  }
}

impl Default for MonotonicHandler {
  fn default() -> Self {
    Self::new(ulid::Generator::new())
  }
}

#[async_trait]
impl Handler for MonotonicHandler {
  #[instrument]
  async fn handle(&self, request: Request) -> ulidd::Result<Response> {
    match request {
      Request::NewId(id) => {
        for _ in 0..3 {
          match self.generate().await {
            Ok(ulid) => {
              return Ok(Response::Id(id, ulid.to_bytes()));
            }
            Err(ulid::MonotonicError::Overflow) => {
              sleep(Duration::from_millis(1)).await;
            }
          }
        }
        return Ok(Response::Error(id, "unable to generate id".to_string()));
      }
      Request::Heartbeat => Ok(Response::HeartbeatAck),
    }
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::init();

  let conf = ulidd::ServerConfig::parse();

  let chitchat_config: chitchat::ChitchatConfig = (&conf).into();
  info!(
    "chitchat config cluster_id={} chitchat_id={:?} seed_nodes={:?}",
    chitchat_config.cluster_id, chitchat_config.chitchat_id, chitchat_config.seed_nodes
  );

  let tags: Vec<(String, String)> = vec![
    ("role".to_string(), "server".to_string()),
    (
      "cluster_addr".to_string(),
      conf
        .cluster_addr
        .map(|v| v.to_string())
        .unwrap_or("".to_string()),
    ),
    ("api_addr".to_string(), conf.addr.clone()),
    ("server_name".to_string(), "localhost".to_string()),
  ];

  let handle = spawn_chitchat(chitchat_config, tags, &UdpTransport).await?;
  let cc = handle.chitchat();

  let mut watcher = cc.lock().await.live_nodes_watcher();
  tokio::spawn(async move {
    while let Some(members) = watcher.next().await {
      for member in members {
        info!("live node: {:#?}", member);
      }
    }
  });

  let discovery = ChitchatDiscovery::new(cc.clone());

  let mut leader_tracker = bully::track_leader(conf.clone(), Arc::new(discovery)).await?;
  tokio::spawn(async move {
    loop {
      if leader_tracker.changed().await.is_err() {
        break;
      }
      info!("leader changed: {:?}", leader_tracker.borrow());
    }
  });

  server::run(conf, MonotonicHandler::default()).await?;
  handle.shutdown().await?;
  Ok(())
}
