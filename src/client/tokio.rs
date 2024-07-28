use std::{io, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use rustls::{pki_types::ServerName, ClientConfig};
use tokio::{
  io::{AsyncReadExt as _, AsyncWriteExt as _},
  net::TcpStream,
  sync::{RwLock, Semaphore},
};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tracing::{debug, error, info, instrument};
use ulid::Ulid;

use super::{pool::Manager, AsyncClient};
use crate::{server::RedirectInfo, Error, Result};

#[derive(Clone)]
pub struct Client {
  pool: ConnectionPool,
  // pool: Pool<ConnectionManager>,
}

impl Client {
  pub async fn new(config: crate::ClientConfig, concurrency: usize) -> Result<Self> {
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
    let pool = ConnectionPool::new(Arc::new(config), addr, server_name, concurrency).await?;
    // let manager = ConnectionManager::new(addr, Arc::new(config), server_name)?;
    // let pool = Pool::new(manager);
    // pool.set_max_open(concurrency as u64);
    Ok(Self { pool })
  }
}

#[async_trait]
impl AsyncClient for Client {
  #[instrument(skip(self))]
  async fn next_id(&self) -> Result<Ulid> {
    let mut tries = 0;
    loop {
      tries += 1;
      if tries > 3 {
        return Err(io::Error::new(io::ErrorKind::Other, "too many tries").into());
      }
      let mut tls_stream = self.pool.get_connection().await?;

      let msg = [1u8];
      if let Err(e) = tls_stream.write_all(&msg).await {
        error!("Failed to send message: {}", e);
        self.pool.return_broken_connection(tls_stream).await;
        continue;
      }

      let mut response_type = [0u8];
      if let Err(e) = tls_stream.read_exact(&mut response_type).await {
        error!("Failed to read response type: {}", e);
        self.pool.return_broken_connection(tls_stream).await;
        continue;
      }
      match response_type[0] {
        1 => {
          let mut buffer = [0u8; 16];
          if let Err(e) = tls_stream.read_exact(&mut buffer).await {
            error!("Failed to read response: {}", e);
            self.pool.return_broken_connection(tls_stream).await;
            continue;
          }
          self.pool.return_connection(tls_stream).await;
          return Ok(Ulid::from_bytes(buffer));
        }
        2 => {
          let mut response_len = [0u8; 4];

          if let Err(e) = tls_stream.read_exact(&mut response_len).await {
            error!("Failed to read response length: {}", e);
            self.pool.return_broken_connection(tls_stream).await;
            continue;
          }
          let len = u32::from_be_bytes(response_len) as usize;

          let mut buffer = vec![0u8; len];
          if let Err(e) = tls_stream.read_exact(&mut buffer).await {
            error!("Failed to read response: {}", e);
            self.pool.return_broken_connection(tls_stream).await;
            continue;
          }

          let redirect_info: RedirectInfo = match serde_cbor::from_slice(&buffer) {
            Ok(info) => info,
            Err(e) => {
              error!("Failed to deserialize response: {:?}", e);
              self.pool.return_broken_connection(tls_stream).await;
              continue;
            }
          };
          self.pool.return_connection(tls_stream).await;
          self.pool.switch_addr(redirect_info.leader).await?;
          continue;
        }
        _ => {
          self.pool.return_connection(tls_stream).await;
          continue;
        }
      }
    }
  }
}

#[derive(Clone)]
struct ConnectionPool {
  connector: TlsConnector,
  size: usize,
  server_name: ServerName<'static>,
  addr: Arc<RwLock<SocketAddr>>,
  sender: AsyncSender<TlsStream<TcpStream>>,
  receiver: AsyncReceiver<TlsStream<TcpStream>>,
  semaphore: Arc<Semaphore>,
}

impl ConnectionPool {
  async fn new(
    config: Arc<ClientConfig>,
    addr: &str,
    server_name: &str,
    size: usize,
  ) -> Result<Self> {
    let (sender, receiver) = kanal::bounded_async(size);

    let dns_name = ServerName::try_from(server_name.to_string()).unwrap();
    let addr: SocketAddr = addr.parse()?;

    let connector = TlsConnector::from(config);

    let pool = Self {
      connector,
      server_name: dns_name,
      addr: Arc::new(RwLock::new(addr)),
      sender,
      receiver,
      size,
      semaphore: Arc::new(Semaphore::new(size)),
    };
    pool.initialize_connections(size).await?;

    Ok(pool)
  }

  async fn initialize_connections(&self, size: usize) -> Result<()> {
    let addr = *self.addr.read().await;
    for _ in 0..size {
      let tcp_stream = TcpStream::connect(&addr).await?;
      let server_name = self.server_name.clone();

      let tls_stream = self.connector.connect(server_name, tcp_stream).await?;
      if let Err(e) = self.sender.send(tls_stream).await {
        error!("Failed to send connection to pool: {}", e);
      }
    }
    Ok(())
  }

  #[instrument(skip(self))]
  async fn get_connection(&self) -> Result<TlsStream<TcpStream>> {
    let _permit = self.semaphore.acquire().await;
    Ok(
      self
        .receiver
        .recv()
        .await
        .map_err(|_| Error::PoolAcquireConnectionFailed)?,
    )
  }

  #[instrument(skip(self, conn))]
  async fn return_connection(&self, conn: TlsStream<TcpStream>) {
    if let Err(e) = self.sender.send(conn).await {
      error!("Failed to return connection to pool: {}", e);
    }
  }

  async fn return_broken_connection(&self, mut conn: TlsStream<TcpStream>) {
    let _ = conn.shutdown().await;

    let tcp_stream = match TcpStream::connect(&*self.addr.read().await).await {
      Ok(tcp_stream) => tcp_stream,
      Err(e) => {
        error!("Failed to reconnect to server: {}", e);
        return;
      }
    };
    let server_name = self.server_name.clone();
    let tls_stream = match self.connector.connect(server_name, tcp_stream).await {
      Ok(tls_stream) => tls_stream,
      Err(e) => {
        error!("Failed to reconnect to server: {}", e);
        return;
      }
    };
    if let Err(e) = self.sender.send(tls_stream).await {
      error!("Failed to return reconnected connection to pool: {}", e);
    }
  }

  #[instrument(skip(self))]
  async fn switch_addr(&self, new_addr: String) -> Result<()> {
    let new_addr: SocketAddr = new_addr.parse()?;
    let mut addr_guard = self.addr.write().await;
    if *addr_guard == new_addr {
      return Ok(());
    }

    info!("Switching address from {} to {}", *addr_guard, new_addr);

    let _permit = self.semaphore.acquire_many(self.size as u32).await.unwrap();
    // Close existing connections
    let mut closed_connections = 0;
    while closed_connections < self.size {
      if let Ok(mut conn) = self.receiver.recv().await {
        let _ = conn.shutdown().await;
        closed_connections += 1;
      }
    }

    // Create new connections
    let connector = self.connector.clone();
    for _ in 0..self.size {
      let tcp_stream = TcpStream::connect(&new_addr).await?;
      let dns_name = self.server_name.clone();

      let tls_stream = connector.connect(dns_name, tcp_stream).await?;
      if let Err(e) = self.sender.send(tls_stream).await {
        error!("Failed to send new connection to pool: {}", e);
      }
    }

    *addr_guard = new_addr;
    Ok(())
  }
}

struct ConnectionManager {
  addr: Arc<RwLock<SocketAddr>>,
  connector: TlsConnector,
  server_name: ServerName<'static>,
}

impl ConnectionManager {
  fn new(addr: &str, config: Arc<ClientConfig>, server_name: &str) -> Result<Self> {
    let dns_name = ServerName::try_from(server_name.to_string())?;
    let addr: SocketAddr = addr.parse()?;
    let connector = TlsConnector::from(config);
    Ok(Self {
      addr: Arc::new(RwLock::new(addr)),
      connector,
      server_name: dns_name,
    })
  }
}

impl Manager for ConnectionManager {
  type Connection = TlsStream<TcpStream>;

  type Error = Error;

  async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
    let addr = *self.addr.read().await;
    let tcp_stream = TcpStream::connect(&addr).await?;
    let dns_name = self.server_name.clone();
    Ok(self.connector.connect(dns_name, tcp_stream).await?)
  }

  async fn check(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
    let peer_addr = conn.get_ref().0.peer_addr()?;
    let addr = *self.addr.read().await;
    if peer_addr != addr {
      debug!("Connection to {} is invalid, reconnecting", peer_addr);
      *conn = self.connect().await?;
    }
    conn.write_all(&[u8::MAX; 1]).await?;
    debug!("Sent ping to {}", peer_addr);
    let mut pong = [0u8; 1];
    conn.read_exact(&mut pong).await?;
    // match timeout(Duration::from_millis(100), conn.read_exact(&mut pong)).await {
    //   Ok(Ok(_)) => {}
    //   Ok(Err(e)) => {
    //     panic!("Error reading pong response: {}", e);
    //     return Err(e.into());
    //   }
    //   Err(_) => {
    //     panic!("Timeout reading pong response");
    //     return Err(Error::PoolGetTimeout);
    //   }
    // }
    if pong[0] != u8::MAX {
      return Err(Error::InvalidPongResponse);
    }
    Ok(())
  }

  async fn switch_addr(&self, addr: &str) -> std::result::Result<(), Self::Error> {
    let new_addr = addr.parse()?;
    let mut addr_guard = self.addr.write().await;
    *addr_guard = new_addr;
    Ok(())
  }
}
