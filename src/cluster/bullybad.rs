use std::{fmt::format, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use axum::http::request;
use dashmap::DashMap;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use rand::Rng;
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use tokio::{
  net::TcpStream,
  sync::{mpsc, Mutex, RwLock},
  time::timeout,
};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_serde_cbor::Codec;
use tokio_util::codec::{Decoder, Framed};
use tracing::error;

use super::disco::Discovery;
use crate::{client_tls_config, server_tls_config, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BullyRequest {
  Election(String, String),
  Coordinator(String, String),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BullyResponse {
  ElectionAck,
  Done(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Details {
  pub node_name: String,
  pub node_id: u64,
  pub url: String,
  pub election_in_progress: bool,
}

pub async fn generate_node_id(disco: Option<Arc<dyn Discovery>>) -> Result<String> {
  // To avoid always picking the latest node we assign a random id
  let mut rng = rand::thread_rng();
  if disco.is_none() {
    return Ok(format!("node-{}", rng.gen_range(1_000..65_000)));
  }

  loop {
    let maybe_id = format!("node-{}", rng.gen_range(1_000..65_000));
    if !disco
      .as_ref()
      .unwrap()
      .members()
      .await?
      .iter()
      .any(|member| member.name == maybe_id)
    {
      break Ok(maybe_id);
    }
  }
}

pub struct BullyConfig {
  pub id: &'static str,
  pub addr: SocketAddr,
  pub disco: Arc<dyn Discovery>,
  pub ca: &'static str,
  pub cert: &'static str,
  pub key: &'static str,
  pub server_name: &'static str,
  pub client_key: &'static str,
  pub client_cert: &'static str,
  pub client_ca: Option<&'static str>,
}

#[derive(Debug)]
pub struct Bully {
  id: &'static str,
  addr: SocketAddr,
  leader: Arc<RwLock<Option<String>>>,
  disco: Arc<dyn Discovery>,
  request_rx: mpsc::Receiver<BullyRequest>,
  request_tx: mpsc::Sender<BullyRequest>,
  response_tx: mpsc::Sender<BullyResponse>,
  response_rx: mpsc::Receiver<BullyResponse>,
  codec: tokio_serde_cbor::Codec<BullyResponse, BullyRequest>,
  server_config: Arc<rustls::ServerConfig>,
  client_config: Arc<rustls::ClientConfig>,
  server_name: ServerName<'static>,
  peers: Arc<DashMap<String, TlsStream<TcpStream>>>,
}

impl Bully {
  pub fn new(config: BullyConfig) -> Result<Self> {
    let (response_tx, response_rx) = mpsc::channel(32);
    let (request_tx, request_rx) = mpsc::channel(32);

    let codec = tokio_serde_cbor::Codec::new();

    let server_config = Arc::new(server_tls_config(config.cert, config.key, Some(config.ca))?);
    let client_config = Arc::new(client_tls_config(
      Some(config.client_cert),
      Some(config.client_key),
      config.client_ca.unwrap_or(config.ca),
    )?);

    let server_name = ServerName::try_from(config.server_name)?;

    Ok(Self {
      id: config.id,
      addr: config.addr,
      leader: Arc::new(RwLock::new(None)),
      disco: config.disco,
      request_rx,
      request_tx,
      response_tx,
      response_rx,
      codec,
      server_config,
      client_config,
      peers: Arc::new(DashMap::new()),
      server_name,
    })
  }

  pub async fn start(&self) -> Result<()> {
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::clone(&self.server_config));
    let listener = tokio::net::TcpListener::bind(self.addr).await?;

    let codec = self.codec.clone();
    let peers = self.peers.clone();
    let leader = self.leader.clone();
    let id = self.id;

    tokio::spawn(async move {
      loop {
        let maybe_socket = listener.accept().await;
        if let Err(e) = maybe_socket {
          error!("failed to accept socket: {}", e);
          continue;
        }
        let (socket, _) = maybe_socket.unwrap();
        let acceptor = acceptor.clone();

        let codec = codec.clone();
        let peers = peers.clone();
        let leader = leader.clone();
        tokio::spawn(async move {
          let stream = acceptor.accept(socket).await.unwrap();
          let (sender, mut receiver) = codec.framed(stream).split();

          while let Some(Ok(message)) = receiver.next().await {
            match message {
              BullyResponse::Done(peer_id) => {
                peers.remove(&peer_id);
                leader.write().await.replace(id.to_string());
              }
              BullyResponse::ElectionAck => {}
            }
          }
        });
      }
    });

    for member in self.disco.members().await? {
      if member.name == self.id {
        continue;
      }

      let addr = member.addr;
      let stream = TcpStream::connect(addr).await?;
      let stream = tokio_rustls::TlsConnector::from(Arc::clone(&self.client_config))
        .connect(self.server_name.clone(), stream)
        .await?;
      self.peers.insert(member.name, stream);
    }
    Ok(())
  }

  async fn connect(&self, id: String) -> Result<()> {
    for member in self.disco.members().await? {
      if member.name == self.id {
        continue;
      }

      let addr = member.addr;
      let stream = TcpStream::connect(addr).await?;
      let stream = tokio_rustls::TlsConnector::from(Arc::clone(&self.client_config))
        .connect(self.server_name.clone(), stream)
        .await?;
      self.peers.insert(member.name, stream);
    }
    Ok(())
  }

  pub async fn send(&self, to: &str, addr: SocketAddr, request: BullyRequest) -> Result<()> {
    // let mut peers = self.peers.iter();
    // while let Some((id, stream)) = peers.next() {
    //   let mut sender = self.codec.clone().framed(Arc::clone(stream));
    //   sender.send(request.clone()).await?;
    // }
    Ok(())
  }

  pub async fn coordinator(&self) -> Option<String> {
    (*self.leader.read().await).clone()
  }

  async fn set_coordinator(&self, id: String) {
    let mut guard = self.leader.write().await;
    if guard.as_ref().map_or(true, |v| v < &id || v == self.id) {
      *guard = Some(id);
    }
  }
}

struct Client {
  id: String,
  addr: SocketAddr,
  stream: mpsc::Sender<BullyRequest>,
}

impl Client {
  pub async fn connect(
    id: String,
    addr: SocketAddr,
    server_name: ServerName<'static>,
    client_config: Arc<rustls::ClientConfig>,
    receive_chan: mpsc::Sender<BullyResponse>,
  ) -> Result<Self> {
    let connector = TlsConnector::from(client_config);
    let codec: Codec<BullyResponse, BullyRequest> = Codec::new();

    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(server_name.clone(), stream).await?;
    let (mut sender, mut receiver) = codec.framed(stream).split();
    let (request_tx, mut request_rx) = mpsc::channel(32);

    tokio::spawn(async move {
      while let Some(message) = request_rx.recv().await {
        if let Err(e) = sender.send(message).await {
          error!("failed to send message: {}", e);
          break;
        }
      }
    });

    tokio::spawn(async move {
      while let Some(Ok(message)) = receiver.next().await {
        if receive_chan.send(message).await.is_err() {
          break;
        }
      }
    });

    Ok(Self {
      id,
      addr,
      stream: request_tx,
    })
  }

  pub async fn send(&mut self, request: BullyRequest) -> Result<()> {
    if let Err(e) = self.stream.send(request).await {
      error!("failed to send message: {}", e);
    }
    Ok(())
  }
}
