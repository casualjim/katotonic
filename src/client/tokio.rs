use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use rustls::{pki_types::ServerName, ClientConfig};
use tokio::{
  io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf},
  net::TcpStream,
  sync::{RwLock, Semaphore, SemaphorePermit},
};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tracing::{error, info, instrument};
use ulid::Ulid;

use super::AsyncClient;
use crate::{server::RedirectInfo, Error, Result};

#[derive(Clone)]
pub struct Client {
  pool: ConnectionPool,
}

impl Client {
  pub async fn new(config: crate::ClientConfig, concurrency: usize) -> Result<Self> {
    let root_store = config.root_store()?;
    let server_name = config.server_name();
    let addr = config.addr()?;

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
        tls_stream.mark_invalid();
        return Err(e.into());
      }

      let mut response_type = [0u8];
      if let Err(e) = tls_stream.read_exact(&mut response_type).await {
        tls_stream.mark_invalid();
        return Err(e.into());
      }
      match response_type[0] {
        1 => {
          let mut buffer = [0u8; 16];
          match tls_stream.read_exact(&mut buffer).await {
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

          if let Err(e) = tls_stream.read_exact(&mut response_len).await {
            error!("Failed to read response length: {}", e);
            tls_stream.mark_invalid();
            return Err(e.into());
          }
          let len = u32::from_be_bytes(response_len) as usize;

          let mut buffer = vec![0u8; len];
          if let Err(e) = tls_stream.read_exact(&mut buffer).await {
            error!("Failed to read response: {}", e);
            tls_stream.mark_invalid();
            return Err(e.into());
          }

          let redirect_info: RedirectInfo = match serde_cbor::from_slice(&buffer) {
            Ok(info) => info,
            Err(e) => {
              error!("Failed to deserialize response: {:?}", e);
              tls_stream.mark_invalid();
              return Err(e.into());
            }
          };
          drop(tls_stream);
          self.pool.switch_addr(redirect_info.leader).await?;
        }
        _ => {
          error!("Unknown response type: {}", response_type[0]);
        }
      }
    }
  }
}

struct ConnectionHandle<'a> {
  tls_stream: Option<TlsStream<TcpStream>>,
  pool: &'a ConnectionPool,
  valid: bool,
  _permit: SemaphorePermit<'a>,
}

impl<'a> ConnectionHandle<'a> {
  fn mark_invalid(&mut self) {
    self.valid = false;
  }
}

impl<'a> AsyncRead for ConnectionHandle<'a> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<io::Result<()>> {
    if let Some(ref mut tls_stream) = self.tls_stream {
      Pin::new(tls_stream).poll_read(cx, buf)
    } else {
      Poll::Ready(Err(io::Error::new(
        io::ErrorKind::Other,
        "Connection is invalid",
      )))
    }
  }
}

impl<'a> AsyncWrite for ConnectionHandle<'a> {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<Result<usize, io::Error>> {
    if let Some(ref mut tls_stream) = self.tls_stream {
      Pin::new(tls_stream).poll_write(cx, buf)
    } else {
      Poll::Ready(Err(io::Error::new(
        io::ErrorKind::Other,
        "Connection is invalid",
      )))
    }
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
    if let Some(ref mut tls_stream) = self.tls_stream {
      Pin::new(tls_stream).poll_flush(cx)
    } else {
      Poll::Ready(Err(io::Error::new(
        io::ErrorKind::Other,
        "Connection is invalid",
      )))
    }
  }

  fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
    if let Some(ref mut tls_stream) = self.tls_stream {
      Pin::new(tls_stream).poll_shutdown(cx)
    } else {
      Poll::Ready(Err(io::Error::new(
        io::ErrorKind::Other,
        "Connection is invalid",
      )))
    }
  }
}

impl<'a> Drop for ConnectionHandle<'a> {
  fn drop(&mut self) {
    if let Some(tls_stream) = self.tls_stream.take() {
      let pool = self.pool.clone();
      if self.valid {
        tokio::spawn(async move {
          pool.return_connection(tls_stream).await;
        });
      } else {
        tokio::spawn(async move {
          pool.handle_broken_connection(tls_stream).await;
        });
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
    addr: SocketAddr,
    server_name: &str,
    size: usize,
  ) -> Result<Self> {
    let (sender, receiver) = kanal::bounded_async(size);

    let dns_name = ServerName::try_from(server_name.to_string()).unwrap();

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
    let futures = (0..size).map(|_| {
      let addr = addr.clone();
      let connector = self.connector.clone();
      let server_name = self.server_name.clone();
      async move {
        let tcp_stream = TcpStream::connect(&addr).await?;
        let tls_stream = connector.connect(server_name, tcp_stream).await?;
        self.sender.send(tls_stream).await?;
        Ok::<(), Error>(())
      }
    });
    futures::future::try_join_all(futures).await?;
    Ok(())
  }

  #[instrument(skip(self))]
  async fn get_connection(&self) -> Result<ConnectionHandle> {
    let _permit = self
      .semaphore
      .acquire()
      .await
      .map_err(|_| Error::PoolAcquireConnectionFailed)?;
    let tls_stream = self
      .receiver
      .recv()
      .await
      .map_err(|_| Error::PoolAcquireConnectionFailed)?;
    Ok(ConnectionHandle {
      tls_stream: Some(tls_stream),
      pool: self,
      valid: true,
      _permit,
    })
  }

  #[instrument(skip(self, conn))]
  async fn return_connection(&self, conn: TlsStream<TcpStream>) {
    if let Err(e) = self.sender.send(conn).await {
      error!("Failed to return connection to pool: {}", e);
    }
  }

  async fn handle_broken_connection(&self, mut conn: TlsStream<TcpStream>) {
    let (strm, client_conn) = conn.get_mut();
    let _ = client_conn.send_close_notify();
    let _ = strm.shutdown().await;
    self.reconnect().await;
  }

  async fn reconnect(&self) {
    loop {
      let tcp_stream = match TcpStream::connect(&*self.addr.read().await).await {
        Ok(tcp_stream) => tcp_stream,
        Err(e) => {
          error!("Failed to reconnect to server: {}", e);
          tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
          continue;
        }
      };
      let server_name = self.server_name.clone();
      let tls_stream = match self.connector.connect(server_name, tcp_stream).await {
        Ok(tls_stream) => tls_stream,
        Err(e) => {
          error!("Failed to reconnect to server: {}", e);
          tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
          continue;
        }
      };
      if let Err(e) = self.sender.send(tls_stream).await {
        error!("Failed to return reconnected connection to pool: {}", e);
      }
      break;
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
      if let Ok(conn) = self.receiver.recv().await {
        let (mut strm, mut client_conn) = conn.into_inner();
        let _ = client_conn.send_close_notify();
        let _ = strm.shutdown().await;
        closed_connections += 1;
      }
    }

    // Create new connections
    let reconnect_futures = (0..self.size).map(|_| {
      let connector = self.connector.clone();
      let new_addr = new_addr.clone();
      let server_name = self.server_name.clone();
      async move {
        let tcp_stream = TcpStream::connect(&new_addr).await?;
        let tls_stream = connector.connect(server_name, tcp_stream).await?;
        self.sender.send(tls_stream).await?;
        Ok::<(), Error>(())
      }
    });
    futures::future::try_join_all(reconnect_futures).await?;

    *addr_guard = new_addr;
    Ok(())
  }
}
