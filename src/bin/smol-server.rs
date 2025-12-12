use std::sync::Arc;

use async_compat::CompatExt;
use clap::Parser;
use katotonic::server::{run_smol_server, spawn_discovery};
use mimalloc::MiMalloc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
    let (quorum_leader, quorum_service) =
      katotonic::quorum::track_leader(conf.clone(), Arc::clone(&discovery))
        .compat()
        .await?;
    run_smol_server(conf, discovery, quorum_leader, quorum_service).await?;
    membership.shutdown().compat().await?;
    Ok(())
  })
}
