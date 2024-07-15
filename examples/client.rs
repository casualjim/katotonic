use clap::Parser;
use futures::{stream::FuturesOrdered, StreamExt};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing::info;
use ulidd::{
  client::{AsyncClient as _, Client},
  ClientConfig, Result,
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
  tracing_subscriber::fmt::init();
  let client_config = ClientConfig::parse();
  let client = Client::new(client_config, 10).await?;

  info!("Sending newid request");
  let mut tasks = FuturesOrdered::new();
  for _ in 0..5 {
    let v = client.next_id().await;
    tasks.push_back(futures::future::ready(v));
    // let response: ulid::Ulid = client.next_id().await?;
    // info!(%response, "Received response");
    // let period = Duration::from_millis(rng.gen_range(50..500));
    // sleep(period).await;
  }

  let mut v = vec![];
  while let Some(response) = tasks.next().await {
    let response = response?;
    info!(%response, "Received response");
    v.push(response.to_string());
  }

  let original = v.clone();
  v.sort();
  // original.sort();
  assert_eq!(v, original);

  Ok(())
}
