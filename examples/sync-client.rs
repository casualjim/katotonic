use std::{
  collections::HashSet,
  io::{self, Read, Write},
  net::TcpStream,
  sync::Arc,
  thread,
  time::Instant,
};

use clap::Parser as _;
use crossbeam::channel::{self, bounded, Receiver, Sender};
use rustls::{pki_types::ServerName, ClientConfig, ClientConnection, StreamOwned};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing::info;
use ulid::Ulid;

const CONCURRENCY: usize = 10; // This client doesn't do well with high concurrency rates
const NUM_IDS: usize = 1_000_000;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  tracing_subscriber::fmt::init();
  let config = ulidd::ClientConfig::parse();
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

  let (tx, rx) = channel::bounded(CONCURRENCY * NUM_IDS);
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
  Ok(())
}

fn next_id(pool: &ConnectionPool) -> ulidd::Result<Ulid> {
  let mut tls_stream = pool.get_connection()?;
  let msg = 1u8.to_be_bytes();
  tls_stream.write_all(&msg)?;

  // Buffer to hold the 16 bytes read from the stream
  let mut buffer = [0u8; 16];

  // Read 16 bytes from the TLS stream
  tls_stream.read_exact(&mut buffer)?;
  pool.return_connection(tls_stream);
  Ok(Ulid::from_bytes(buffer))
}

struct ConnectionPool {
  sender: Sender<StreamOwned<ClientConnection, TcpStream>>,
  receiver: Receiver<StreamOwned<ClientConnection, TcpStream>>,
}

impl ConnectionPool {
  fn new(config: Arc<ClientConfig>, addr: &str, server_name: &str, size: usize) -> Self {
    let (sender, receiver) = bounded(size);

    // Initialize the pool with the specified number of connections
    for _ in 0..size {
      let tcp_stream = TcpStream::connect(addr).expect("Failed to connect");
      let server_name = ServerName::try_from(server_name.to_string()).unwrap();
      let client = ClientConnection::new(config.clone(), server_name)
        .expect("Failed to create client connection");
      let tls_stream = StreamOwned::new(client, tcp_stream);
      sender
        .send(tls_stream)
        .expect("Failed to send connection to pool");
    }

    Self { sender, receiver }
  }

  fn get_connection(&self) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
    Ok(
      self
        .receiver
        .recv()
        .expect("Failed to receive connection from pool"),
    )
  }

  fn return_connection(&self, conn: StreamOwned<ClientConnection, TcpStream>) {
    self
      .sender
      .send(conn)
      .expect("Failed to return connection to pool");
  }
}
