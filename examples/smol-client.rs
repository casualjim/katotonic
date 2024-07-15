use std::{collections::HashSet, time::Instant};

use clap::Parser as _;
use futures::stream::FuturesOrdered;
use futures_lite::StreamExt;
use smol::channel::bounded;
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use ulid::Ulid;
use ulidd::client::SmolClient;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const CONCURRENCY: usize = 50;
const NUM_IDS: usize = 1_000_000;

fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::init();
  let config = ulidd::ClientConfig::parse();

  let client = smolscale::block_on(SmolClient::new(config, CONCURRENCY))?;

  let (tx, rx) = bounded(CONCURRENCY * NUM_IDS);

  let start = Instant::now();
  let mut processed = FuturesOrdered::new();
  for _ in 0..CONCURRENCY {
    let client = client.clone();
    let tx = tx.clone();
    processed.push_back(smolscale::spawn(async move {
      let mut prev = Ulid::nil();
      let mut local_ulids = Vec::new();

      // Generate ULIDs in each thread
      for _ in 0..NUM_IDS {
        match client.next_id().await {
          Ok(new) => {
            if new < prev {
              panic!("received non-monotonic ID: {} < {}", new, prev);
            }
            local_ulids.push(new);
            prev = new;
          }
          Err(e) => {
            eprintln!("Failed to get next ID: {}", e);
          }
        }
      }
      tx.send(local_ulids).await.expect("Failed to send ID");
    }));
  }

  drop(tx); // Close the sending side of the channel

  // Run the executor until all tasks are complete
  smolscale::block_on(async move {
    while let Some(_) = processed.next().await {}
    println!("All tasks completed in {:?}", start.elapsed());

    let mut all_ulids = Vec::new();
    while let Ok(received) = rx.recv().await {
      all_ulids.extend(received);
    }

    // Ensure all ULIDs across all threads are unique
    let mut ulid_set = HashSet::new();
    for ulid in &all_ulids {
      ulid_set.insert(ulid.clone());
    }
    assert_eq!(ulid_set.len(), all_ulids.len());

    println!("All ULIDs are unique and the ordering is valid");
  });
  Ok(())
}
