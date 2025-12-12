use std::{collections::HashSet, time::Instant};

use clap::Parser as _;
use futures::stream::FuturesOrdered;
use futures_lite::StreamExt;
use katotonic::client::SmolClient;
use katotonic::KatotonicId;
use smol::channel::bounded;
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const CONCURRENCY: usize = 5;
const NUM_IDS: usize = 1_000_000;

fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::init();
  let config = katotonic::ClientConfig::parse();

  let client = smolscale::block_on(SmolClient::new(config, CONCURRENCY))?;

  let (tx, rx) = bounded(CONCURRENCY * NUM_IDS);

  let start = Instant::now();
  let mut processed = FuturesOrdered::new();
  for _ in 0..CONCURRENCY {
    let client = client.clone();
    let tx = tx.clone();
    processed.push_back(smolscale::spawn(async move {
      let mut prev = KatotonicId::nil();
      let mut local_ids = Vec::new();

      // Generate IDs in each task
      for _ in 0..NUM_IDS {
        match client.next_id().await {
          Ok(new) => {
            if new.as_bytes() < prev.as_bytes() {
              panic!("received non-monotonic ID");
            }
            local_ids.push(new);
            prev = new;
          }
          Err(e) => {
            eprintln!("Failed to get next ID: {}", e);
          }
        }
      }
      tx.send(local_ids).await.expect("Failed to send ID");
    }));
  }

  drop(tx); // Close the sending side of the channel

  // Run the executor until all tasks are complete
  smolscale::block_on(async move {
    while let Some(_) = processed.next().await {}
    println!("All tasks completed in {:?}", start.elapsed());

    let mut all_ids = Vec::new();
    while let Ok(received) = rx.recv().await {
      all_ids.extend(received);
    }

    // Ensure all IDs across all tasks are unique
    let mut id_set = HashSet::new();
    for id in &all_ids {
      id_set.insert(*id);
    }
    assert_eq!(id_set.len(), all_ids.len());

    println!("All IDs are unique and the ordering is valid");
  });
  Ok(())
}
