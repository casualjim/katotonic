use std::{io, sync::Arc};

use async_trait::async_trait;
use futures::lock::Mutex;
use rustls::{pki_types::ServerName, ClientConfig};
use tokio::{
  io::{AsyncReadExt as _, AsyncWriteExt as _},
  net::TcpStream,
  sync::{
    mpsc::{self, Receiver, Sender},
    Semaphore,
  },
};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tracing::{error, info, instrument};
use ulid::Ulid;

use super::AsyncClient;
use crate::{Error, Result};

#[derive(Clone)]
pub struct Client {
  pool: ConnectionPool,
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
    Ok(Self { pool })
  }
}

#[async_trait]
impl AsyncClient for Client {
  #[instrument(skip(self))]
  async fn next_id(&self) -> Result<Ulid> {
    let mut tls_stream = self.pool.get_connection().await?;

    let msg_type = [1u8];
    tls_stream.write_all(&msg_type).await?;

    let mut buffer = [0u8; 16];
    tls_stream.read_exact(&mut buffer).await?;
    self.pool.return_connection(tls_stream).await;

    Ok(Ulid::from_bytes(buffer))
  }
}

#[derive(Clone)]
struct ConnectionPool {
  sender: Sender<TlsStream<TcpStream>>,
  receiver: Arc<Mutex<Receiver<TlsStream<TcpStream>>>>,
  semaphore: Arc<Semaphore>,
}

impl ConnectionPool {
  async fn new(
    config: Arc<ClientConfig>,
    addr: &str,
    server_name: &str,
    size: usize,
  ) -> Result<Self> {
    let (sender, receiver) = mpsc::channel(size);

    let connector = TlsConnector::from(config.clone());
    // Initialize the pool with the specified number of connections
    for _ in 0..size {
      let tcp_stream = TcpStream::connect(addr).await?;
      let dns_name = ServerName::try_from(server_name.to_string()).unwrap();

      let tls_stream = connector.connect(dns_name, tcp_stream).await?;
      sender
        .send(tls_stream)
        .await
        .map_err(|_| Error::PoolInitializationFailed)?;
    }

    Ok(Self {
      sender,
      receiver: Arc::new(Mutex::new(receiver)),
      semaphore: Arc::new(Semaphore::new(size)),
    })
  }

  #[instrument(skip(self))]
  async fn get_connection(&self) -> Result<TlsStream<TcpStream>> {
    let _permit = self.semaphore.acquire().await;
    Ok(
      self
        .receiver
        .lock()
        .await
        .recv()
        .await
        .ok_or(Error::PoolAcquireConnectionFailed)?,
    )
  }

  #[instrument(skip(self, conn))]
  async fn return_connection(&self, conn: TlsStream<TcpStream>) {
    if let Err(e) = self.sender.send(conn).await {
      error!("Failed to return connection to pool: {}", e);
    };
  }
}
