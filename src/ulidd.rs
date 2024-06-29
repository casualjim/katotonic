// Copyright (c) 2024 ivan
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::Result;
use ulid::Ulid;

#[async_trait::async_trait]
pub trait IdGenerator {
  async fn next_id(&self) -> Result<Ulid>;
}

pub mod protocol {
  use serde::{Deserialize, Serialize};

  #[derive(Serialize, Deserialize, Debug)]
  pub enum Request {
    NewId(u64),
    Heartbeat,
  }

  #[derive(Serialize, Deserialize, Debug)]
  pub enum Response {
    Id(u64, [u8; 16]),
    HeartbeatAck,
  }

  impl Response {
    pub(crate) fn get_request_id(&self) -> u64 {
      match self {
        Response::Id(id, _) => *id,
        Response::HeartbeatAck => 0,
      }
    }
  }
}

pub mod client {
  use std::{
    io::{self},
    sync::{
      atomic::{AtomicU64, Ordering},
      Arc,
    },
    time::Duration,
  };

  use crate::Result;
  use dashmap::DashMap;
  use futures::{SinkExt as _, StreamExt as _};
  use rustls::{pki_types::ServerName, ClientConfig};
  use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::mpsc,
    time,
  };
  use tokio_rustls::TlsConnector;
  use tokio_serde_cbor::Codec;
  use tokio_util::codec::Decoder;
  use tracing::debug;
  use ulid::Ulid;

  use super::{
    protocol::{Request, Response},
    IdGenerator,
  };

  async fn run<P: ToSocketAddrs>(
    connector: TlsConnector,
    addr: P,
    server_name: ServerName<'static>,
    in_flight: Arc<DashMap<u64, tokio::sync::oneshot::Sender<[u8; 16]>>>,
    mut receiver: mpsc::Receiver<Request>,
  ) {
    debug!("connecting to server");
    let stream = TcpStream::connect(addr).await.unwrap();
    let stream = connector
      .connect(server_name.clone(), stream)
      .await
      .unwrap();
    let codec: Codec<Response, Request> = Codec::new();

    let (mut server_sender, mut server_receiver) = codec.framed(stream).split();
    let mut interval = time::interval(Duration::from_secs(1));

    tokio::spawn(async move {
      loop {
        tokio::select! {
            _ = interval.tick() => {
                server_sender.send(Request::Heartbeat).await.unwrap();
            },
            Some(request) = receiver.recv() => {
                server_sender.send(request).await.unwrap();
            }
        }
      }
    });

    while let Some(Ok(response)) = server_receiver.next().await {
      if let Some((_, sender)) = in_flight.remove(&response.get_request_id()) {
        match response {
          Response::Id(_, data) => {
            sender.send(data).unwrap();
          }
          Response::HeartbeatAck => (),
        }
      }
    }
  }

  pub struct Client {
    in_flight_requests: Arc<DashMap<u64, tokio::sync::oneshot::Sender<[u8; 16]>>>,
    request_id_counter: AtomicU64,
    sender: mpsc::Sender<Request>,
  }

  impl Client {
    pub async fn new(config: crate::ClientConfig) -> Result<Self> {
      let root_store = config.root_store()?;
      let server_name = config.server_name();
      let addr = config.addr();

      let builder = ClientConfig::builder().with_root_certificates(root_store);

      let config = match config.keypair()? {
        Some((certs, private_key)) => builder
          .with_client_auth_cert(certs, private_key)
          .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None => builder.with_no_client_auth(),
      };

      let connector = TlsConnector::from(Arc::new(config));
      let server_name = ServerName::try_from(server_name.to_string()).unwrap();

      let (sender, receiver) = mpsc::channel(100);

      let client = Self {
        in_flight_requests: Arc::new(DashMap::new()),
        request_id_counter: AtomicU64::new(1),
        sender,
      };

      tokio::spawn(run(
        connector,
        addr.to_string(),
        server_name,
        client.in_flight_requests.clone(),
        receiver,
      ));

      Ok(client)
    }
  }

  #[async_trait::async_trait]
  impl IdGenerator for Client {
    async fn next_id(&self) -> Result<Ulid> {
      let request_id = self.request_id_counter.fetch_add(1, Ordering::SeqCst);
      let request = Request::NewId(request_id);

      let (sender, receiver) = tokio::sync::oneshot::channel();
      self.in_flight_requests.insert(request_id, sender);

      self.sender.send(request).await.unwrap();

      let response = receiver.await.unwrap();
      Ok(Ulid::from_bytes(response))
    }
  }
}
