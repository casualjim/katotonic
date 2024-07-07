// use std::time::Duration;

use clap::Parser;
// use rand::Rng;
// use tokio::time::sleep;
use tracing::info;
use ulidd::{client::Client, ClientConfig, IdGenerator, Result};

#[tokio::main]
async fn main() -> Result<()> {
  tracing_subscriber::fmt::init();
  let client_config = ClientConfig::parse();
  let client = Client::new(client_config).await?;

  info!("Sending newid request");
  // let mut rng = rand::thread_rng();
  for _ in 0..100_000 {
    let response: ulid::Ulid = client.next_id().await?;
    info!(%response, "Received response");
    // let period = Duration::from_millis(rng.gen_range(50..500));
    // sleep(period).await;
  }

  Ok(())
}
