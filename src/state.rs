pub use sync::WatchableValue;

mod sync {
  use std::sync::Arc;

  use crossbeam::sync::ShardedLock;
  use parking_lot::{Condvar, Mutex};
  use tokio::sync::Notify;

  #[derive(Clone)]
  pub struct WatchableValue<T> {
    value: Arc<ShardedLock<T>>,
    notify: Arc<(Mutex<()>, Condvar)>,
    async_notify: Arc<Notify>,
  }

  impl<T> WatchableValue<T>
  where
    T: std::fmt::Debug + Clone + PartialEq,
  {
    pub fn new(initial: T) -> Self {
      Self {
        value: Arc::new(ShardedLock::new(initial)),
        notify: Arc::new((Mutex::new(()), Condvar::new())),
        async_notify: Arc::new(Notify::new()),
      }
    }

    pub fn read(&self) -> T {
      self.value.read().unwrap().clone()
    }

    pub fn write(&self, new_value: T) -> bool {
      let mut current_value = self.value.write().unwrap();
      if *current_value != new_value {
        *current_value = new_value;
        let (lock, cvar) = &*self.notify;
        let _guard = lock.lock();
        cvar.notify_all();
        self.async_notify.notify_waiters();
        return true;
      }
      false
    }

    pub fn modify<F>(&self, update: F) -> bool
    where
      F: FnOnce(&T) -> Option<T>,
    {
      let mut current_value = self.value.write().unwrap();
      if let Some(new_value) = update(&current_value) {
        if *current_value != new_value {
          *current_value = new_value;
          let (lock, cvar) = &*self.notify;
          let _guard = lock.lock();
          cvar.notify_all();
          self.async_notify.notify_waiters();
          return true;
        }
      }
      false
    }

    pub fn wait_for_change(&self) {
      let (lock, cvar) = &*self.notify;
      let mut guard = lock.lock();
      cvar.wait(&mut guard);
    }

    pub async fn wait_for_change_async(&self) {
      self.async_notify.notified().await;
    }
  }

  #[cfg(test)]
  mod tests {
    use std::thread;

    use super::WatchableValue;

    #[test]
    fn test_leader_state_sync() {
      let leader_state = WatchableValue::new(42);

      // Simulate a reader that waits for updates
      let leader_state_reader = leader_state.clone();
      let handle = thread::spawn(move || {
        leader_state_reader.wait_for_change();
        let new_value = leader_state_reader.read();
        assert_eq!(new_value, 43);
      });

      // Simulate a writer
      thread::sleep(std::time::Duration::from_secs(1));
      leader_state.modify(|_| Some(43));

      handle.join().unwrap();
    }

    #[test]
    fn test_multiple_writers_sync() {
      let leader_state = WatchableValue::new(0);
      let leader_state1 = leader_state.clone();
      let leader_state2 = leader_state.clone();

      let handle1 = thread::spawn(move || {
        for _ in 0..10 {
          leader_state1.modify(|current_value| Some(current_value + 1));
        }
      });

      let handle2 = thread::spawn(move || {
        for _ in 0..10 {
          leader_state2.modify(|current_value| Some(current_value + 1));
        }
      });

      handle1.join().unwrap();
      handle2.join().unwrap();

      assert_eq!(leader_state.read(), 20);
    }
  }
}
