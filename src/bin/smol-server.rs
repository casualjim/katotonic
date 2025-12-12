use std::sync::Arc;

use async_compat::CompatExt;
use clap::Parser;
use katotonic::{
  bully,
  server::{run_smol_server, spawn_discovery},
};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
  let env_filter = tracing_subscriber::EnvFilter::from_default_env();
  let subscriber = tracing_subscriber::fmt::layer()
    .pretty()
    .with_filter(env_filter);

  tracing_subscriber::registry().with(subscriber).init();

  let conf = katotonic::ServerConfig::parse();
  let quorum_state = katotonic::quorum::QuorumState::load(&conf.state_path)?;
  info!(
    "loaded quorum state term={} voted_term={} high_watermark={} config_version={}",
    quorum_state.current_term,
    quorum_state.voted_term,
    quorum_state.high_watermark,
    quorum_state.config_version
  );
  smolscale::block_on(async move {
    let (membership, discovery) = spawn_discovery(conf.clone()).compat().await?;
    let leader_tracker = bully::track_leader(conf.clone(), Arc::clone(&discovery), None)
      .compat()
      .await?;

    run_smol_server(conf, discovery, leader_tracker).await?;
    membership.shutdown().compat().await?;
    Ok(())
  })
}
