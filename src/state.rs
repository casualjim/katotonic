pub use sync::WatchableValue;

mod sync {
  use std::sync::Arc;

  use parking_lot::{Condvar, Mutex, RwLock, RwLockWriteGuard};
  use tokio::sync::Notify;
  use tracing::debug;

  #[derive(Clone)]
  pub struct WatchableValue<T> {
    value: Arc<RwLock<T>>,
    notify: Arc<(Mutex<()>, Condvar)>,
    async_notify: Arc<Notify>,
  }

  impl<T> WatchableValue<T>
  where
    T: std::fmt::Debug + Clone + PartialEq,
  {
    pub fn new(initial: T) -> Self {
      Self {
        value: Arc::new(RwLock::new(initial)),
        notify: Arc::new((Mutex::new(()), Condvar::new())),
        async_notify: Arc::new(Notify::new()),
      }
    }

    pub fn read(&self) -> T {
      self.value.read().clone()
    }

    pub fn write(&self, new_value: T) -> bool {
      let mut current_value: RwLockWriteGuard<'_, T> = self.value.write();
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
      let mut current_value: RwLockWriteGuard<'_, T> = self.value.write();
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

#[cfg(feature = "tokio-runtime")]
mod async_mod {
  use std::sync::Arc;

  use tokio::sync::{Notify, RwLock};

  pub struct WatchableValue<T> {
    value: Arc<RwLock<T>>,
    notify: Notify,
  }

  impl<T> WatchableValue<T>
  where
    T: Clone + PartialEq,
  {
    pub fn new(initial: T) -> Arc<Self> {
      Arc::new(Self {
        value: Arc::new(RwLock::new(initial)),
        notify: Notify::new(),
      })
    }

    pub async fn read(&self) -> T {
      self.value.read().await.clone()
    }

    pub async fn write(&self, new_value: T) -> bool {
      let mut current_value = self.value.write().await;
      if *current_value != new_value {
        *current_value = new_value;
        self.notify.notify_waiters();
        return true;
      }
      false
    }

    pub async fn modify<F>(&self, update: F) -> bool
    where
      F: FnOnce(&T) -> Option<T>,
    {
      let mut current_value = self.value.write().await;
      if let Some(new_value) = update(&current_value) {
        if *current_value != new_value {
          *current_value = new_value;
          self.notify.notify_waiters();
          return true;
        }
      }
      false
    }

    pub async fn wait_for_change(&self) {
      self.notify.notified().await
    }
  }

  #[cfg(test)]
  mod tests {
    use std::sync::Arc;

    use tokio::time::{sleep, Duration};

    use super::WatchableValue;

    #[tokio::test]
    async fn test_leader_state_async_tokio() {
      let leader_state = WatchableValue::new(42);

      // Simulate a reader that waits for updates
      let leader_state_reader = Arc::clone(&leader_state);
      let handle = tokio::spawn(async move {
        leader_state_reader.wait_for_change().await;
        let new_value = leader_state_reader.read().await;
        assert_eq!(new_value, 43);
      });

      // Simulate a writer
      sleep(Duration::from_secs(1)).await;
      leader_state.write(43).await;

      handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_writers_tokio() {
      let leader_state = WatchableValue::new(0);
      let leader_state1 = Arc::clone(&leader_state);
      let leader_state2 = Arc::clone(&leader_state);

      let handle1 = tokio::spawn(async move {
        for _ in 0..10 {
          leader_state1.modify(|old| Some(old + 1)).await;
        }
      });

      let handle2 = tokio::spawn(async move {
        for _ in 0..10 {
          leader_state2.modify(|old| Some(old + 1)).await;
        }
      });

      handle1.await.unwrap();
      handle2.await.unwrap();

      assert_eq!(leader_state.read().await, 20);
    }
  }
}

#[cfg(feature = "smol-runtime")]
mod smol_mod {
  use std::sync::Arc;

  use async_notify::Notify;
  use smol::lock::RwLock;

  pub struct WatchableValue<T> {
    value: Arc<RwLock<T>>,
    notify: Notify,
  }

  impl<T> WatchableValue<T>
  where
    T: Clone + PartialEq + Eq,
  {
    pub fn new(initial: T) -> Arc<Self> {
      Arc::new(Self {
        value: Arc::new(RwLock::new(initial)),
        notify: Notify::new(),
      })
    }

    pub async fn read(&self) -> T {
      self.value.read().await.clone()
    }

    pub async fn write(&self, new_value: T) -> bool {
      let mut current_value = self.value.write().await;
      if *current_value != new_value {
        *current_value = new_value;
        self.notify.notify();
        return true;
      }
      false
    }

    pub async fn modify<F>(&self, update: F) -> bool
    where
      F: FnOnce(&T) -> Option<T>,
    {
      let mut current_value = self.value.write().await;
      if let Some(new_value) = update(&current_value) {
        if *current_value != new_value {
          *current_value = new_value;
          self.notify.notify();
          return true;
        }
      }
      false
    }

    pub async fn wait_for_change(&self) {
      self.notify.notified().await;
    }
  }

  #[cfg(test)]
  mod tests {
    use std::{sync::Arc, time::Duration};

    use smol::{block_on, Timer};

    use super::WatchableValue;

    #[test]
    fn test_leader_state_async_smol() {
      block_on(async {
        let leader_state = WatchableValue::new(42);

        // Simulate a reader that waits for updates
        let leader_state_reader = Arc::clone(&leader_state);
        let handle = smol::spawn(async move {
          leader_state_reader.wait_for_change().await;
          let new_value = leader_state_reader.read().await;
          assert_eq!(new_value, 43);
        });

        // Simulate a writer
        Timer::after(Duration::from_secs(1)).await;
        leader_state.write(43).await;

        handle.await;
      });
    }

    #[test]
    fn test_multiple_writers_smol() {
      block_on(async {
        let leader_state = WatchableValue::new(0);
        let leader_state1 = Arc::clone(&leader_state);
        let leader_state2 = Arc::clone(&leader_state);

        let handle1 = smol::spawn(async move {
          for _ in 0..10 {
            leader_state1.modify(|old| Some(old + 1)).await;
          }
        });

        let handle2 = smol::spawn(async move {
          for _ in 0..10 {
            leader_state2.modify(|old| Some(old + 1)).await;
          }
        });

        handle1.await;
        handle2.await;

        assert_eq!(leader_state.read().await, 20);
      });
    }
  }
}
