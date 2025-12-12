use std::{collections::HashSet, time::Instant};

use clap::Parser as _;
use futures::{stream::FuturesOrdered, StreamExt};
use katotonic::client::{AsyncClient as _, Client};
use katotonic::KatotonicId;
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tokio::sync::mpsc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const CONCURRENCY: usize = 5;
const NUM_IDS: usize = 1_000_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::init();
  let config = katotonic::ClientConfig::parse();

  let client = Client::new(config, CONCURRENCY).await?;

  let (tx, mut rx) = mpsc::channel(CONCURRENCY * NUM_IDS);
  let mut tasks = FuturesOrdered::new();

  let start = Instant::now();
  for _ in 0..CONCURRENCY {
    let client = client.clone();
    let tx = tx.clone();
    tasks.push_back(tokio::spawn(async move {
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

  // Wait for all tasks to complete
  while tasks.next().await.is_some() {}
  println!("All tasks completed in {:?}", start.elapsed());

  let mut all_ids = Vec::new();
  while let Some(received) = rx.recv().await {
    all_ids.extend(received);
  }

  // Ensure all IDs across all tasks are unique
  let mut id_set = HashSet::new();
  for id in &all_ids {
    id_set.insert(*id);
  }
  assert_eq!(id_set.len(), all_ids.len());

  println!("All IDs are unique and the ordering is valid");
  // Wait for Ctrl-C signal
  // tokio::signal::ctrl_c().await?;
  // println!("Received Ctrl-C, shutting down");
  Ok(())
}
