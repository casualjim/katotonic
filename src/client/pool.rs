#![allow(async_fn_in_trait)]

// This is the fast_pool from rbatis but with kanal instead of flume.
// https://github.com/rbatis/fast_pool/blob/main/src/lib.rs
//
// The original code is licensed under the Apache 2.0 license.
// https://github.com/rbatis/fast_pool/blob/main/LICENSE

use std::{
  fmt::{Debug, Display, Formatter},
  ops::{Deref, DerefMut},
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Duration,
};

use kanal::{Receiver, Sender};
use thiserror::Error;

/// Pool have manager, get/get_timeout Connection from Pool
pub struct Pool<M: Manager> {
  manager: Arc<M>,
  idle_send: Arc<Sender<M::Connection>>,
  idle_recv: Arc<Receiver<M::Connection>>,
  max_open: Arc<AtomicU64>,
  in_use: Arc<AtomicU64>,
  waits: Arc<AtomicU64>,
}

impl<M: Manager> Debug for Pool<M> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Pool")
      // .field("manager", &self.manager)
      .field("max_open", &self.max_open)
      .field("in_use", &self.in_use)
      .finish()
  }
}

impl<M: Manager> Clone for Pool<M> {
  fn clone(&self) -> Self {
    Self {
      manager: self.manager.clone(),
      idle_send: self.idle_send.clone(),
      idle_recv: self.idle_recv.clone(),
      max_open: self.max_open.clone(),
      in_use: self.in_use.clone(),
      waits: self.waits.clone(),
    }
  }
}

/// Manager create Connection and check Connection
pub trait Manager {
  type Connection;

  type Error: From<PoolError>;

  ///switch Connection addr
  async fn switch_addr(&self, addr: &str) -> Result<(), Self::Error>;

  ///create Connection and check Connection
  async fn connect(&self) -> Result<Self::Connection, Self::Error>;
  ///check Connection is alive? if not return Error(Connection will be drop)
  async fn check(&self, conn: &mut Self::Connection) -> Result<(), Self::Error>;
}

impl<M: Manager> Pool<M> {
  pub fn new(m: M) -> Self
  where
    <M as Manager>::Connection: Unpin,
  {
    let default_max = num_cpus::get() as u64;

    let (s, r) = kanal::unbounded();
    Self {
      manager: Arc::new(m),
      idle_send: Arc::new(s),
      idle_recv: Arc::new(r),
      max_open: Arc::new(AtomicU64::new(default_max)),
      in_use: Arc::new(AtomicU64::new(0)),
      waits: Arc::new(AtomicU64::new(0)),
    }
  }

  pub async fn switch_addr(&self, addr: &str) -> Result<(), M::Error> {
    self.manager.switch_addr(addr).await
  }

  pub async fn get(&self) -> Result<ConnectionBox<M>, M::Error> {
    self.get_timeout(None).await
  }

  pub async fn get_timeout(&self, d: Option<Duration>) -> Result<ConnectionBox<M>, M::Error> {
    self.waits.fetch_add(1, Ordering::SeqCst);
    defer!(|| {
      self.waits.fetch_sub(1, Ordering::SeqCst);
    });
    //pop connection from channel
    let f = async {
      loop {
        let idle = self.idle_send.len() as u64;
        let connections = self.in_use.load(Ordering::SeqCst) + idle;
        if connections < self.max_open.load(Ordering::SeqCst) {
          //create connection,this can limit max idle,current now max idle = max_open
          let conn = self.manager.connect().await?;
          self
            .idle_send
            .as_async()
            .send(conn)
            .await
            .map_err(|e| M::Error::from(PoolError::KanalSend(e)))?;
        }
        let mut conn = self
          .idle_recv
          .as_async()
          .recv()
          .await
          .map_err(|e| M::Error::from(PoolError::KanalRecv(e)))?;
        //check connection
        match self.manager.check(&mut conn).await {
          Ok(_) => {
            break Ok(conn);
          }
          Err(_e) => {
            //TODO some thing need return e?
            if false {
              return Err(_e);
            }
            continue;
          }
        }
      }
    };
    let conn = {
      if d.is_none() {
        f.await?
      } else {
        tokio::time::timeout(d.unwrap(), f)
          .await
          .map_err(|_e| M::Error::from(PoolError::GetTimeout))??
      }
    };
    self.in_use.fetch_add(1, Ordering::SeqCst);
    Ok(ConnectionBox {
      inner: Some(conn),
      sender: self.idle_send.clone(),
      in_use: self.in_use.clone(),
      max_open: self.max_open.clone(),
    })
  }

  pub fn state(&self) -> State {
    State {
      max_open: self.max_open.load(Ordering::Relaxed),
      connections: self.in_use.load(Ordering::Relaxed) + self.idle_send.len() as u64,
      in_use: self.in_use.load(Ordering::Relaxed),
      idle: self.idle_send.len() as u64,
      waits: self.waits.load(Ordering::Relaxed),
    }
  }

  pub fn set_max_open(&self, n: u64) {
    if n == 0 {
      return;
    }
    self.max_open.store(n, Ordering::SeqCst);
    loop {
      if self.idle_send.len() > n as usize {
        _ = self.idle_recv.try_recv();
      } else {
        break;
      }
    }
  }
}

pub struct ConnectionBox<M: Manager> {
  pub inner: Option<M::Connection>,
  sender: Arc<Sender<M::Connection>>,
  in_use: Arc<AtomicU64>,
  max_open: Arc<AtomicU64>,
}

impl<M: Manager> Debug for ConnectionBox<M> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ConnectionBox")
      .field("sender", &self.sender)
      // .field("inner", &self.inner)
      .field("in_use", &self.in_use)
      .field("max_open", &self.max_open)
      .finish()
  }
}

impl<M: Manager> Deref for ConnectionBox<M> {
  type Target = M::Connection;

  fn deref(&self) -> &Self::Target {
    self.inner.as_ref().unwrap()
  }
}

impl<M: Manager> DerefMut for ConnectionBox<M> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.inner.as_mut().unwrap()
  }
}

impl<M: Manager> Drop for ConnectionBox<M> {
  fn drop(&mut self) {
    self.in_use.fetch_sub(1, Ordering::SeqCst);
    if let Some(v) = self.inner.take() {
      let max_open = self.max_open.load(Ordering::SeqCst);
      if self.sender.len() as u64 + self.in_use.load(Ordering::SeqCst) < max_open {
        _ = self.sender.send(v);
      }
    }
  }
}

#[derive(Debug, Eq, PartialEq)]
pub struct State {
  /// max open limit
  pub max_open: u64,
  ///connections = in_use number + idle number
  pub connections: u64,
  /// user use connection number
  pub in_use: u64,
  /// idle connection
  pub idle: u64,
  /// wait get connections number
  pub waits: u64,
}

impl Display for State {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{{ max_open: {}, connections: {}, in_use: {}, idle: {}, waits: {} }}",
      self.max_open, self.connections, self.in_use, self.idle, self.waits
    )
  }
}

#[derive(Debug, Error)]
pub enum PoolError {
  #[error("get connection timeout")]
  GetTimeout,
  #[error("Kanal receive error: {0}")]
  KanalRecv(#[from] kanal::ReceiveError),
  #[error("Kanal send error: {0}")]
  KanalSend(#[from] kanal::SendError),
}

#[cfg(test)]
mod tests {

  use std::{ops::Deref, time::Duration};

  use super::{Manager, Pool, PoolError};

  #[derive(Debug)]
  pub struct TestManager {}

  impl Manager for TestManager {
    type Connection = String;
    type Error = PoolError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
      println!("new Connection");
      Ok(String::new())
    }

    async fn check(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
      if conn != "" {
        return Err(kanal::SendError::Closed.into());
      }
      Ok(())
    }

    async fn switch_addr(&self, _addr: &str) -> Result<(), Self::Error> {
      Ok(())
    }
  }

  #[tokio::test]
  async fn test_debug() {
    let p = Pool::new(TestManager {});
    println!("{:?}", p);
  }

  #[tokio::test]
  async fn test_clone() {
    let p = Pool::new(TestManager {});
    let p2 = p.clone();
    assert_eq!(p.state(), p2.state());
  }

  // --nocapture
  #[tokio::test]
  async fn test_pool_get() {
    let p = Pool::new(TestManager {});
    p.set_max_open(10);
    let mut arr = vec![];
    for i in 0..10 {
      let v = p.get().await.unwrap();
      println!("{},{}", i, v.deref());
      arr.push(v);
    }
  }

  #[tokio::test]
  async fn test_pool_get2() {
    let p = Pool::new(TestManager {});
    p.set_max_open(10);
    for i in 0..3 {
      let v = p.get().await.unwrap();
      println!("{},{}", i, v.deref());
    }
    assert_eq!(p.state().idle, 3);
  }

  #[tokio::test]
  async fn test_pool_get_timeout() {
    let p = Pool::new(TestManager {});
    p.set_max_open(10);
    let mut arr = vec![];
    for i in 0..10 {
      let v = p.get().await.unwrap();
      println!("{},{}", i, v.deref());
      arr.push(v);
    }
    assert_eq!(
      p.get_timeout(Some(Duration::from_secs(0))).await.is_err(),
      true
    );
  }

  #[tokio::test]
  async fn test_pool_check() {
    let p = Pool::new(TestManager {});
    p.set_max_open(10);
    let mut v = p.get().await.unwrap();
    *v.inner.as_mut().unwrap() = "error".to_string();
    for _i in 0..10 {
      let v = p.get().await.unwrap();
      assert_eq!(v.deref() == "error", false);
    }
  }

  #[tokio::test]
  async fn test_pool_resize() {
    let p = Pool::new(TestManager {});
    p.set_max_open(10);
    let mut arr = vec![];
    for i in 0..10 {
      let v = p.get().await.unwrap();
      println!("{},{}", i, v.deref());
      arr.push(v);
    }
    assert_eq!(
      p.get_timeout(Some(Duration::from_secs(0))).await.is_err(),
      true
    );
    p.set_max_open(11);
    assert_eq!(
      p.get_timeout(Some(Duration::from_secs(0))).await.is_err(),
      false
    );
    arr.push(p.get().await.unwrap());
    assert_eq!(
      p.get_timeout(Some(Duration::from_secs(0))).await.is_err(),
      true
    );
  }

  #[tokio::test]
  async fn test_pool_resize2() {
    let p = Pool::new(TestManager {});
    p.set_max_open(2);
    let mut arr = vec![];
    for _i in 0..2 {
      let v = p.get().await.unwrap();
      arr.push(v);
    }
    p.set_max_open(1);
    drop(arr);
    println!("{:?}", p.state());
    assert_eq!(
      p.get_timeout(Some(Duration::from_secs(0))).await.is_err(),
      false
    );
  }

  #[tokio::test]
  async fn test_concurrent_access() {
    let p = Pool::new(TestManager {});
    p.set_max_open(10);
    let mut handles = vec![];
    for _ in 0..10 {
      let pool = p.clone();
      let handle = tokio::spawn(async move {
        let _ = pool.get().await.unwrap();
      });
      handles.push(handle);
    }
    for handle in handles {
      handle.await.unwrap();
    }
    assert_eq!(p.state().connections, 10);
  }

  #[tokio::test]
  async fn test_invalid_connection() {
    let p = Pool::new(TestManager {});
    p.set_max_open(1);

    let mut conn = p.get().await.unwrap();
    //conn timeout
    *conn.inner.as_mut().unwrap() = "error".to_string();
    drop(conn);

    // Attempt to get a new connection, should not be the invalid one
    let new_conn = p.get().await.unwrap();
    assert_ne!(new_conn.deref(), &"error".to_string());
  }

  #[tokio::test]
  async fn test_connection_lifetime() {
    let p = Pool::new(TestManager {});
    p.set_max_open(10);

    let conn = p.get().await.unwrap();
    // Perform operations using the connection
    // ...

    drop(conn); // Drop the connection

    // Ensure dropped connection is not in use
    assert_eq!(p.state().in_use, 0);

    // Acquire a new connection
    let new_conn = p.get().await.unwrap();
    assert_ne!(new_conn.deref(), &"error".to_string());
  }

  #[tokio::test]
  async fn test_boundary_conditions() {
    let p = Pool::new(TestManager {});
    p.set_max_open(2);

    // Acquire connections until pool is full
    let conn_1 = p.get().await.unwrap();
    let _conn_2 = p.get().await.unwrap();
    assert_eq!(p.state().in_use, 2);

    // Attempt to acquire another connection (pool is full)
    assert!(p.get_timeout(Some(Duration::from_secs(0))).await.is_err());

    // Release one connection, pool is no longer full
    drop(conn_1);
    assert_eq!(p.state().in_use, 1);

    // Acquire another connection (pool has space)
    let _conn_3 = p.get().await.unwrap();
    assert_eq!(p.state().in_use, 2);

    // Increase pool size
    p.set_max_open(3);
    // Acquire another connection after increasing pool size
    let _conn_4 = p.get().await.unwrap();
    assert_eq!(p.state().in_use, 3);
  }

  #[tokio::test]
  async fn test_pool_wait() {
    let p = Pool::new(TestManager {});
    p.set_max_open(1);
    let v = p.get().await.unwrap();
    let p1 = p.clone();
    tokio::spawn(async move {
      p1.get().await.unwrap();
      drop(p1);
    });
    let p1 = p.clone();
    tokio::spawn(async move {
      p1.get().await.unwrap();
      drop(p1);
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("{:?}", p.state());
    assert_eq!(p.state().waits, 2);
    drop(v);
  }
}
