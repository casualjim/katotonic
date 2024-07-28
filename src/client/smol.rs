use std::{io, net::SocketAddr, sync::Arc};

use futures_rustls::{client::TlsStream, TlsConnector};
use kanal::{AsyncReceiver, AsyncSender};
use rustls::{pki_types::ServerName, ClientConfig};
use smol::{
  io::{AsyncReadExt, AsyncWriteExt},
  lock::{RwLock, Semaphore},
  net::TcpStream,
};
use tracing::{error, info, instrument};
use ulid::Ulid;

use crate::{server::RedirectInfo, Result};

#[derive(Clone)]
pub struct Client {
  pool: ConnectionPool,
}

impl Client {
  pub async fn new(config: crate::ClientConfig, concurrency: usize) -> Result<Self> {
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
    let pool = ConnectionPool::new(Arc::new(config), addr, server_name, concurrency).await?;
    Ok(Self { pool })
  }

  #[instrument(skip(self))]
  pub async fn next_id(&self) -> Result<Ulid> {
    let mut tries = 0;
    loop {
      tries += 1;
      if tries > 3 {
        return Err(io::Error::new(io::ErrorKind::Other, "too many tries").into());
      }
      let mut tls_stream = self.pool.get_connection().await?;
      let msg = [1u8];
      tls_stream.write_all(&msg).await?;

      let mut response_type = [0u8];
      tls_stream.read_exact(&mut response_type).await?;
      match response_type[0] {
        1 => {
          let mut buffer = [0u8; 16];
          tls_stream.read_exact(&mut buffer).await?;
          self.pool.return_connection(tls_stream).await;
          return Ok(Ulid::from_bytes(buffer));
        }
        2 => {
          let mut response_len = [0u8; 4];

          tls_stream.read_exact(&mut response_len).await?;
          let len = u32::from_be_bytes(response_len) as usize;

          let mut buffer = vec![0u8; len];
          tls_stream.read_exact(&mut buffer).await?;

          let redirect_info: RedirectInfo = serde_cbor::from_slice(&buffer)?;

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
  config: Arc<ClientConfig>,
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
    addr: String,
    server_name: String,
    size: usize,
  ) -> Result<Self> {
    let (sender, receiver) = kanal::bounded_async(size);

    let dns_name = ServerName::try_from(server_name.clone()).unwrap();
    let addr: SocketAddr = addr.parse()?;

    let pool = Self {
      config,
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
    let connector = TlsConnector::from(self.config.clone());
    for _ in 0..size {
      let tcp_stream = TcpStream::connect(&addr).await?;
      let dns_name = self.server_name.clone();

      let tls_stream = connector.connect(dns_name, tcp_stream).await?;
      if let Err(e) = self.sender.send(tls_stream).await {
        error!("Failed to send connection to pool: {}", e);
      }
    }
    Ok(())
  }

  #[instrument(skip(self))]
  async fn get_connection(&self) -> Result<TlsStream<TcpStream>> {
    let _permit = self.semaphore.acquire().await;
    Ok(self.receiver.recv().await?)
  }

  #[instrument(skip(self, conn))]
  async fn return_connection(&self, conn: TlsStream<TcpStream>) {
    if let Err(e) = self.sender.send(conn).await {
      error!("Failed to return connection to pool: {}", e);
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

    let mut permits = vec![];
    for _ in 0..self.size {
      permits.push(self.semaphore.acquire());
    }

    // Close existing connections
    let mut closed_connections = 0;
    while closed_connections < self.size {
      if let Ok(conn) = self.receiver.recv().await {
        conn.get_ref().0.shutdown(smol::net::Shutdown::Both)?;
        closed_connections += 1;
      }
    }

    // Create new connections
    let connector = TlsConnector::from(self.config.clone());
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
