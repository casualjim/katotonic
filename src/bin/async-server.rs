use std::sync::Arc;

use clap::Parser;
use katotonic::{
  bully,
  server::{run_server, spawn_discovery},
};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let console_layer = console_subscriber::spawn();

  let env_filter = tracing_subscriber::EnvFilter::from_default_env();
  let subscriber = tracing_subscriber::fmt::layer()
    .pretty()
    .with_filter(env_filter);

  tracing_subscriber::registry()
    .with(subscriber)
    .with(console_layer)
    .init();

  let conf = katotonic::ServerConfig::parse();
  let quorum_state = katotonic::quorum::QuorumState::load(&conf.state_path)?;
  info!(
    "loaded quorum state term={} voted_term={} high_watermark={} config_version={}",
    quorum_state.current_term,
    quorum_state.voted_term,
    quorum_state.high_watermark,
    quorum_state.config_version
  );

  let (membership, discovery) = spawn_discovery(conf.clone()).await?;
  let (quorum_leader, quorum_service) =
    katotonic::quorum::track_leader(conf.clone(), Arc::clone(&discovery)).await?;
  let bully_leader = bully::track_leader(
    conf.clone(),
    Arc::clone(&discovery),
    None,
    Some(quorum_service.clone()),
  )
  .await?;

  run_server(conf, discovery, quorum_leader, quorum_service, bully_leader).await?;

  membership.shutdown().await?;
  Ok(())
}
