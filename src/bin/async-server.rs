use std::sync::Arc;

use clap::Parser;
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer as _};
use ulidd::{
  bully,
  server::{run_server, spawn_discovery},
};

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

  let conf = ulidd::ServerConfig::parse();

  let (membership, discovery) = spawn_discovery(conf.clone()).await?;
  let leader_tracker = bully::track_leader(conf.clone(), Arc::clone(&discovery), None).await?;

  run_server(conf, discovery, leader_tracker).await?;

  membership.shutdown().await?;
  Ok(())
}
