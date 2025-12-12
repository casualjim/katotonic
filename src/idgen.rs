use std::sync::atomic::Ordering;

use portable_atomic::AtomicU64;

use crate::id::{encode_v8, KatotonicId};

static PREVIOUS_SEQ: AtomicU64 = AtomicU64::new(0);

/// Temporary local monotonic generator used until quorum reservation is implemented.
pub fn generate_monotonic_id(term: u64) -> KatotonicId {
  let seq = PREVIOUS_SEQ.fetch_add(1, Ordering::SeqCst) as u128;
  encode_v8(term, seq)
}

#[cfg(test)]
mod tests {
  use std::{collections::HashSet, thread};

  use crossbeam::channel;

  use super::*;

  #[test]
  fn test_generate_monotonic_id_monotonicity_and_uniqueness() {
    let mut ids = Vec::new();
    let mut id_set = HashSet::new();

    // Generate a large number of IDs
    for _ in 0..1_000_000 {
      let id = generate_monotonic_id(0);
      id_set.insert(id);
      ids.push(id);
    }

    // Ensure all IDs are unique
    assert_eq!(id_set.len(), ids.len());

    // Ensure all IDs are in monotonic order by raw bytes
    let mut sorted_ids = ids.clone();
    sorted_ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(ids, sorted_ids);
  }

  #[test]
  fn test_generate_monotonic_id_monotonicity_and_uniqueness_concurrently() {
    let num_threads = 10;
    let id_count = 1_000_000;

    let (tx, rx) = channel::bounded(num_threads * id_count);

    thread::scope(|s| {
      for _ in 0..num_threads {
        let tx = tx.clone();

        s.spawn(move || {
          let mut local_ids = Vec::new();
          let mut local_id_set = HashSet::new();

          // Generate IDs in each thread
          for _ in 0..id_count {
            let id = generate_monotonic_id(0);
            local_id_set.insert(id);
            local_ids.push(id);
          }

          // Ensure all IDs in this thread are unique
          assert_eq!(local_id_set.len(), local_ids.len());

          // Ensure all IDs in this thread are in monotonic order
          let mut sorted_ids = local_ids.clone();
          sorted_ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
          assert_eq!(local_ids, sorted_ids);

          // Send the results back to the main thread
          tx.send(local_ids).unwrap();
        });
      }
    });

    drop(tx); // Close the sending side of the channel

    let mut all_ids = Vec::new();
    for received in rx {
      all_ids.extend(received);
    }

    // Ensure all IDs across all threads are unique
    let mut id_set = HashSet::new();
    for id in &all_ids {
      id_set.insert(*id);
    }
    assert_eq!(id_set.len(), all_ids.len());
  }
}
