use std::time::Duration;

use rand::Rng;
use tokio::time::sleep;
use tracing::info;
use ulidd::{client::Client, IdGenerator};
use ulidd::{ClientConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
  tracing_subscriber::fmt::init();
  let client = Client::new(ClientConfig::new()?).await?;

  info!("Sending newid request");
  let mut rng = rand::thread_rng();
  for _ in 0..10 {
    let response: ulid::Ulid = client.next_id().await?;
    info!(%response, "Received response");
    let period = Duration::from_millis(rng.gen_range(500..3000));
    sleep(period).await;
  }

  Ok(())
}
