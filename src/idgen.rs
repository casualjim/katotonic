use std::sync::atomic::{self};

use ulid::Ulid;

static PREVIOUS_ULID: portable_atomic::AtomicU128 = portable_atomic::AtomicU128::new(0);

pub fn generate_ulid() -> Ulid {
  let mut new_ulid: u128 = 0;
  loop {
    let newulid = Ulid::new();
    let ordering = atomic::Ordering::SeqCst;

    let res = PREVIOUS_ULID.fetch_update(ordering, ordering, |prev| {
      let previous = Ulid(prev);
      if previous.timestamp_ms() == newulid.timestamp_ms() {
        if let Some(new_val) = previous.increment() {
          new_ulid = new_val.0;
          Some(new_val.0)
        } else {
          None
        }
      } else if previous < newulid {
        new_ulid = newulid.0;
        Some(newulid.0)
      } else {
        None
      }
    });
    if res.is_ok() {
      break;
    }
    // sleep(Duration::from_millis(1));
  }
  Ulid(new_ulid)
}

#[cfg(test)]
mod tests {
  use std::{collections::HashSet, thread, time::UNIX_EPOCH};

  use crossbeam::channel;

  use super::*;

  #[tokio::test]
  async fn test_generate_ulid() {
    let ulid = generate_ulid();
    assert_eq!(
      ulid
        .datetime()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis(),
      ulid.timestamp_ms() as u128
    );
  }

  #[test]
  fn test_generate_ulid_monotonicity_and_uniqueness() {
    let mut ulids = Vec::new();
    let mut ulid_set = HashSet::new();

    // Generate a large number of ULIDs
    for _ in 0..1_000_000 {
      let ulid = generate_ulid();
      ulid_set.insert(ulid);
      ulids.push(ulid);
    }

    // Ensure all ULIDs are unique
    assert_eq!(ulid_set.len(), ulids.len());

    // Ensure all ULIDs are in monotonic order
    let mut sorted_ulids = ulids.clone();
    sorted_ulids.sort();
    assert_eq!(ulids, sorted_ulids);
  }

  #[test]
  fn test_generate_ulid_monotonicity_and_uniqueness_concurrently() {
    let num_threads = 10;
    let ulid_count = 1_000_000;

    let (tx, rx) = channel::bounded(num_threads * ulid_count);

    thread::scope(|s| {
      for _ in 0..num_threads {
        let tx = tx.clone();

        s.spawn(move || {
          let mut local_ulids = Vec::new();
          let mut local_ulid_set = HashSet::new();

          // Generate ULIDs in each thread
          for _ in 0..ulid_count {
            let ulid = generate_ulid();
            local_ulid_set.insert(ulid.clone());
            local_ulids.push(ulid);
          }

          // Ensure all ULIDs in this thread are unique
          assert_eq!(local_ulid_set.len(), local_ulids.len());

          // Ensure all ULIDs in this thread are in monotonic order
          let mut sorted_ulids = local_ulids.clone();
          sorted_ulids.sort();
          assert_eq!(local_ulids, sorted_ulids);

          // Send the results back to the main thread
          tx.send(local_ulids).unwrap();
        });
      }
    });

    drop(tx); // Close the sending side of the channel

    let mut all_ulids = Vec::new();
    for received in rx {
      all_ulids.extend(received);
    }

    // Ensure all ULIDs across all threads are unique
    let mut ulid_set = HashSet::new();
    for ulid in &all_ulids {
      ulid_set.insert(ulid.clone());
    }
    assert_eq!(ulid_set.len(), all_ulids.len());
  }
}
