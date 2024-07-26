use std::{collections::HashSet, time::Instant};

use clap::Parser as _;
use futures::{stream::FuturesOrdered, StreamExt};
use katotonic::client::{AsyncClient as _, Client};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tokio::sync::mpsc;
use ulid::Ulid;

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

  // Wait for all tasks to complete
  while let Some(_) = tasks.next().await {}
  println!("All tasks completed in {:?}", start.elapsed());

  let mut all_ulids = Vec::new();
  while let Some(received) = rx.recv().await {
    all_ulids.extend(received);
  }

  // Ensure all ULIDs across all threads are unique
  let mut ulid_set = HashSet::new();
  for ulid in &all_ulids {
    ulid_set.insert(ulid.clone());
  }
  assert_eq!(ulid_set.len(), all_ulids.len());

  println!("All ULIDs are unique and the ordering is valid");
  // Wait for Ctrl-C signal
  // tokio::signal::ctrl_c().await?;
  // println!("Received Ctrl-C, shutting down");
  Ok(())
}
