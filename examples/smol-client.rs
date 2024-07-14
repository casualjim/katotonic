use std::{collections::HashSet, io, sync::Arc, time::Instant};

use clap::Parser as _;
use futures::stream::FuturesOrdered;
use futures_lite::StreamExt;
use futures_rustls::{client::TlsStream, TlsConnector};
use rustls::{pki_types::ServerName, ClientConfig};
use smol::{
  channel::{bounded, Receiver, Sender},
  io::{AsyncReadExt, AsyncWriteExt},
  lock::{Mutex, Semaphore},
  net::TcpStream,
};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing::info;
use ulid::Ulid;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::init();
  let config = ulidd::ClientConfig::parse();
  let root_store = config.root_store()?;
  let server_name = config.server_name().to_string();
  let addr = config.addr().to_string();

  info!("connecting to server={addr} server_name={server_name}");
  let builder = ClientConfig::builder().with_root_certificates(root_store);

  let config = match config.keypair()? {
    Some((certs, private_key)) => builder
      .with_client_auth_cert(certs, private_key)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
    None => builder.with_no_client_auth(),
  };

  const CONCURRENCY: usize = 50;
  const NUM_IDS: usize = 1_000_000;

  // Wrap the config in an Arc for use with ClientConnection
  let config = Arc::new(config);
  let pool = smolscale::block_on(ConnectionPool::new(config, addr, server_name, CONCURRENCY));

  let (tx, rx) = bounded(CONCURRENCY * NUM_IDS);

  let start = Instant::now();
  let mut processed = FuturesOrdered::new();
  for _ in 0..CONCURRENCY {
    let pool = pool.clone();
    let tx = tx.clone();
    processed.push_back(smolscale::spawn(async move {
      let mut prev = Ulid::nil();
      let mut local_ulids = Vec::new();

      // Generate ULIDs in each thread
      for _ in 0..NUM_IDS {
        match next_id(&pool).await {
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

async fn next_id(pool: &ConnectionPool) -> ulidd::Result<Ulid> {
  let mut tls_stream = pool.get_connection().await?;
  let msg = [1u8];
  tls_stream.write_all(&msg).await?;

  // Buffer to hold the 16 bytes read from the stream
  let mut buffer = [0u8; 16];

  // Read 16 bytes from the TLS stream
  tls_stream.read_exact(&mut buffer).await?;
  pool.return_connection(tls_stream).await;

  Ok(Ulid::from_bytes(buffer))
}

#[derive(Clone)]
struct ConnectionPool {
  sender: Sender<TlsStream<TcpStream>>,
  receiver: Arc<Mutex<Receiver<TlsStream<TcpStream>>>>,
  semaphore: Arc<Semaphore>,
}

impl ConnectionPool {
  async fn new(config: Arc<ClientConfig>, addr: String, server_name: String, size: usize) -> Self {
    let (sender, receiver) = bounded(size);

    let connector = TlsConnector::from(config.clone());

    // Initialize the pool with the specified number of connections
    for _ in 0..size {
      let tcp_stream = TcpStream::connect(&addr).await.expect("Failed to connect");
      let dns_name = ServerName::try_from(server_name.clone()).unwrap();

      let tls_stream = connector
        .connect(dns_name, tcp_stream)
        .await
        .expect("Failed to create client connection");
      sender
        .send(tls_stream)
        .await
        .expect("Failed to send connection to pool");
    }

    Self {
      sender,
      receiver: Arc::new(Mutex::new(receiver)),
      semaphore: Arc::new(Semaphore::new(size)),
    }
  }

  async fn get_connection(&self) -> io::Result<TlsStream<TcpStream>> {
    let _permit = self.semaphore.acquire().await;
    Ok(
      self
        .receiver
        .lock()
        .await
        .recv()
        .await
        .expect("Failed to receive connection from pool"),
    )
  }

  async fn return_connection(&self, conn: TlsStream<TcpStream>) {
    self
      .sender
      .send(conn)
      .await
      .expect("Failed to return connection to pool");
  }
}
