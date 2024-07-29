use std::{
  collections::{HashSet, VecDeque},
  io::{self, Read, Write},
  net::{SocketAddr, TcpStream},
  sync::Arc,
  thread::{self, sleep},
  time::{Duration, Instant},
};

use clap::Parser as _;
use kanal::{Receiver, Sender};
use katotonic::{Error, Result};
use parking_lot::{Condvar, Mutex, RwLock};
use rustls::{pki_types::ServerName, ClientConfig, ClientConnection, StreamOwned};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing::{error, info};
use ulid::Ulid;

const CONCURRENCY: usize = 50; // This client doesn't do well with high concurrency rates
const NUM_IDS: usize = 1_000_000;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();
  let config = katotonic::ClientConfig::parse();
  let root_store = config.root_store()?;
  let server_name = config.server_name();
  let addr = config.addr();

  info!("connecting to server={addr} server_name={server_name}");
  let builder = ClientConfig::builder().with_root_certificates(root_store);

  let config = match config.keypair()? {
    Some((certs, private_key)) => builder
      .with_client_auth_cert(certs, private_key)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
    None => builder.with_no_client_auth(),
  };

  // Wrap the config in an Arc for use with ClientConnection
  let config = Arc::new(config);
  let pool = ConnectionPool::new(config, addr, server_name, CONCURRENCY);
  // let id = next_id(&pool)?;
  // println!("Received ID: {}", id);

  let (tx, rx) = kanal::bounded(CONCURRENCY * NUM_IDS);
  let start = Instant::now();
  thread::scope(|s| {
    for _ in 0..CONCURRENCY {
      s.spawn(|| {
        let mut prev = Ulid::nil();
        let mut local_ulids = Vec::new();
        let mut local_ulid_set = HashSet::new();

        // Generate ULIDs in each thread
        for _ in 0..NUM_IDS {
          let new = next_id(&pool).expect("Failed to get next ID");
          if new < prev {
            panic!("received non-monotonic ID: {} < {}", new, prev);
          }
          local_ulid_set.insert(new.clone());
          local_ulids.push(new);

          prev = new;
        }
        tx.send(local_ulids).expect("Failed to send ID");
      });
    }
  });

  drop(tx); // Close the sending side of the channel
  println!("All tasks completed in {:?}", start.elapsed());

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
  println!(
    "All {} ULIDs are unique and the ordering is valid",
    all_ulids.len()
  );
  Ok(())
}

fn next_id(pool: &ConnectionPool) -> katotonic::Result<Ulid> {
  let mut tries = 0;
  loop {
    tries += 1;
    if tries > 3 {
      return Err(io::Error::new(io::ErrorKind::Other, "too many tries").into());
    }
    let mut tls_stream = pool.get_connection()?;

    let msg = [1u8];
    if let Err(e) = tls_stream.write_all(&msg) {
      tls_stream.mark_invalid();
      return Err(e.into());
    }

    let mut response_type = [0u8];
    if let Err(e) = tls_stream.read_exact(&mut response_type) {
      tls_stream.mark_invalid();
      return Err(e.into());
    }

    match response_type[0] {
      1 => {
        let mut buffer = [0u8; 16];
        match tls_stream.read_exact(&mut buffer) {
          Ok(_) => {
            return Ok(Ulid::from_bytes(buffer));
          }
          Err(e) => {
            tls_stream.mark_invalid();
            return Err(e.into());
          }
        }
      }
      2 => {
        let mut response_len = [0u8; 4];

        if let Err(e) = tls_stream.read_exact(&mut response_len) {
          error!("Failed to read response length: {}", e);
          tls_stream.mark_invalid();
          return Err(e.into());
        }
        let len = u32::from_be_bytes(response_len) as usize;

        let mut buffer = vec![0u8; len];
        if let Err(e) = tls_stream.read_exact(&mut buffer) {
          error!("Failed to read response: {}", e);
          tls_stream.mark_invalid();
          return Err(e.into());
        }

        let redirect_info: katotonic::server::RedirectInfo = match serde_cbor::from_slice(&buffer) {
          Ok(info) => info,
          Err(e) => {
            error!("Failed to deserialize response: {:?}", e);
            tls_stream.mark_invalid();
            return Err(e.into());
          }
        };
        drop(tls_stream);
        pool.switch_addr(redirect_info.leader)?;
      }
      _ => {
        error!("Unknown response type: {}", response_type[0]);
      }
    }
  }
}

struct ConnectionPool {
  config: Arc<ClientConfig>,
  size: usize,
  server_name: ServerName<'static>,
  addr: Arc<RwLock<SocketAddr>>,
  sender: Sender<StreamOwned<ClientConnection, TcpStream>>,
  receiver: Receiver<StreamOwned<ClientConnection, TcpStream>>,
  semaphore: Arc<Semaphore>,
}

impl ConnectionPool {
  fn new(config: Arc<ClientConfig>, addr: &str, server_name: &str, size: usize) -> Self {
    let (sender, receiver) = kanal::bounded(size);

    // Initialize the pool with the specified number of connections
    let server_name = ServerName::try_from(server_name.to_string()).unwrap();
    for _ in 0..size {
      let tcp_stream = TcpStream::connect(addr).expect("Failed to connect");

      let client = ClientConnection::new(config.clone(), server_name.clone())
        .expect("Failed to create client connection");
      let tls_stream = StreamOwned::new(client, tcp_stream);
      sender
        .send(tls_stream)
        .expect("Failed to send connection to pool");
    }

    let semaphore = Arc::new(Semaphore::new(size));

    Self {
      config,
      size,
      server_name,
      addr: Arc::new(RwLock::new(addr.parse().expect("Failed to parse address"))),
      sender,
      receiver,
      semaphore,
    }
  }

  pub fn get_connection(&self) -> Result<ConnectionHandle> {
    let _permit = self.semaphore.acquire();

    let tls_stream = self
      .receiver
      .recv()
      .map_err(|_| Error::PoolAcquireConnectionFailed)?;

    Ok(ConnectionHandle {
      tls_stream: Some(tls_stream),
      pool: self,
      valid: true,
      _permit,
    })
  }

  fn return_connection(&self, conn: StreamOwned<ClientConnection, TcpStream>) {
    if let Err(e) = self.sender.send(conn) {
      error!("Failed to return connection to pool: {:?}", e);
    }
  }

  fn handle_broken_connection(&self, mut conn: StreamOwned<ClientConnection, TcpStream>) {
    conn.conn.send_close_notify();
    conn.sock.shutdown(std::net::Shutdown::Both).ok();
    self.reconnect()
  }

  fn reconnect(&self) {
    loop {
      let tcp_stream = match TcpStream::connect(&*self.addr.read()) {
        Ok(tcp_stream) => tcp_stream,
        Err(e) => {
          error!("Failed to connect: {:?}", e);
          sleep(Duration::from_secs(1));
          continue;
        }
      };

      let server_name = self.server_name.clone();
      let config = Arc::clone(&self.config);

      let client = match ClientConnection::new(config, server_name) {
        Ok(client) => client,
        Err(e) => {
          error!("Failed to create client connection: {:?}", e);
          sleep(Duration::from_secs(1));
          continue;
        }
      };

      if let Err(e) = self.sender.send(StreamOwned::new(client, tcp_stream)) {
        error!("Failed to return connection to pool: {:?}", e);
      }
      break;
    }
  }

  pub fn switch_addr(&self, new_addr: String) -> Result<()> {
    let new_addr = new_addr.parse()?;
    let mut addr = self.addr.write();
    if *addr == new_addr {
      return Ok(());
    }

    info!("switching to new address from {} to {}", *addr, new_addr);

    let _permit = self.semaphore.acquire_many(self.size);

    let mut closed_connections = 0;
    while closed_connections < self.size {
      if let Ok(conn) = self.receiver.recv() {
        let (mut client_conn, strm) = conn.into_parts();
        client_conn.send_close_notify();
        strm.shutdown(std::net::Shutdown::Both).ok();
        closed_connections += 1;
      }
    }

    for _ in 0..self.size {
      let tcp_stream = TcpStream::connect(new_addr)?;
      let client = ClientConnection::new(self.config.clone(), self.server_name.clone())?;
      let tls_stream = StreamOwned::new(client, tcp_stream);
      self.sender.send(tls_stream)?;
    }

    *addr = new_addr;
    Ok(())
  }
}

struct ConnectionHandle<'a> {
  tls_stream: Option<StreamOwned<ClientConnection, TcpStream>>,
  pool: &'a ConnectionPool,
  valid: bool,
  _permit: Permit,
}

impl<'a> ConnectionHandle<'a> {
  fn mark_invalid(&mut self) {
    self.valid = false;
  }
}

impl<'a> Read for ConnectionHandle<'a> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    if let Some(ref mut tls_stream) = self.tls_stream {
      tls_stream.read(buf)
    } else {
      Err(io::Error::new(io::ErrorKind::Other, "connection is broken"))
    }
  }
}

impl<'a> Write for ConnectionHandle<'a> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    if let Some(ref mut tls_stream) = self.tls_stream {
      tls_stream.write(buf)
    } else {
      Err(io::Error::new(io::ErrorKind::Other, "connection is broken"))
    }
  }

  fn flush(&mut self) -> io::Result<()> {
    if let Some(ref mut tls_stream) = self.tls_stream {
      tls_stream.flush()
    } else {
      Err(io::Error::new(io::ErrorKind::Other, "connection is broken").into())
    }
  }
}

impl<'a> Drop for ConnectionHandle<'a> {
  fn drop(&mut self) {
    if let Some(tls_stream) = self.tls_stream.take() {
      if self.valid {
        self.pool.return_connection(tls_stream);
      } else {
        self.pool.handle_broken_connection(tls_stream);
      }
    }
  }
}

pub struct Permit {
  semaphore: Arc<SemaphoreInner>,
  count: usize,
}

impl Drop for Permit {
  fn drop(&mut self) {
    self.semaphore.release_many(self.count);
  }
}

struct Semaphore {
  inner: Arc<SemaphoreInner>,
}

impl Semaphore {
  pub fn new(capacity: usize) -> Self {
    Self {
      inner: Arc::new(SemaphoreInner::new(capacity)),
    }
  }

  pub fn acquire(&self) -> Permit {
    self.inner.acquire();
    Permit {
      semaphore: Arc::clone(&self.inner),
      count: 1,
    }
  }

  pub fn acquire_many(&self, count: usize) -> Permit {
    self.inner.acquire_many(count);
    Permit {
      semaphore: Arc::clone(&self.inner),
      count,
    }
  }
}

struct SemaphoreInner {
  capacity: Mutex<usize>,
  cond: Condvar,
  waiters: Mutex<VecDeque<(usize, Arc<Condvar>)>>,
}

impl SemaphoreInner {
  pub fn new(capacity: usize) -> Self {
    Self {
      capacity: Mutex::new(capacity),
      cond: Condvar::new(),
      waiters: Mutex::new(VecDeque::new()),
    }
  }

  pub fn acquire(&self) {
    self.acquire_many(1);
  }

  pub fn acquire_many(&self, permits: usize) {
    let mut count = self.capacity.lock();
    let waiter = Arc::new(Condvar::new());
    {
      let mut waiters = self.waiters.lock();
      waiters.push_back((permits, Arc::clone(&waiter)));
    }

    while *count < permits
      || self
        .waiters
        .lock()
        .front()
        .map_or(false, |w| w.0 != permits)
    {
      waiter.wait(&mut count);
    }

    *count -= permits;
    {
      let mut waiters = self.waiters.lock();
      waiters.pop_front();
      if let Some(next) = waiters.front() {
        next.1.notify_one();
      }
    }
  }

  pub fn release_many(&self, permits: usize) {
    let mut count = self.capacity.lock();
    *count += permits;
    self.cond.notify_all();

    let waiters = self.waiters.lock();
    if let Some(next) = waiters.front() {
      next.1.notify_one();
    }
  }
}
