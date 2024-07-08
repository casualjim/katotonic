use std::{borrow::Cow, cmp::Reverse, collections::HashSet, io, sync::Arc, time::Duration};

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt as _};
use serde::{Deserialize, Serialize};
use serde_json::error;
use tokio::{
  net::{TcpListener, TcpStream},
  sync::{
    mpsc::{Receiver, Sender},
    oneshot, watch, RwLock,
  },
  time::{sleep, timeout},
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tokio_serde_cbor::Codec;
use tokio_util::codec::Decoder;
use tracing::info;

use super::disco::Discovery;
use crate::{server_tls_config, Error, Result, ServerConfig};

#[async_trait]
pub trait Transport: Send + Sync {
  type Request: Send + Sync + Serialize + for<'de> Deserialize<'de>;
  type Response: Send + Sync + Serialize + for<'de> Deserialize<'de>;

  async fn send(&self, msg: Self::Request) -> Result<Self::Response>;
}

pub struct ChannelTransport<I, O> {
  sender: tokio::sync::mpsc::Sender<I>,
  receiver: Arc<RwLock<tokio::sync::mpsc::Receiver<O>>>,
}

impl<I, O> ChannelTransport<I, O>
where
  I: Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  pub fn new(sender: Sender<I>, receiver: Receiver<O>) -> Self {
    Self {
      sender,
      receiver: Arc::new(RwLock::new(receiver)),
    }
  }
}

#[async_trait]
impl<I, O> Transport for ChannelTransport<I, O>
where
  I: Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  type Request = I;
  type Response = O;

  async fn send(&self, msg: Self::Request) -> Result<Self::Response> {
    if let Err(_) = self.sender.send(msg).await {
      return Err(Error::Io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "unexpected EOF",
      )));
    }
    let mut receiver = self.receiver.write().await;
    if let Some(response) = receiver.recv().await {
      Ok(response)
    } else {
      Err(Error::Io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "unexpected EOF",
      )))
    }
  }
}

pub struct TcpTransport<'a, I, O> {
  addr: Cow<'a, str>,
  codec: Codec<O, I>,
}

impl<'a, I, O> TcpTransport<'a, I, O>
where
  I: Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  pub fn new(addr: Cow<'a, str>) -> Self {
    Self {
      addr,
      codec: Codec::new(),
    }
  }
}

#[async_trait]
impl<'a, I, O> Transport for TcpTransport<'a, I, O>
where
  I: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  type Request = I;
  type Response = O;

  async fn send(&self, req: Self::Request) -> Result<Self::Response> {
    let stream = tokio::net::TcpStream::connect(self.addr.as_ref()).await?;
    let codec = self.codec.clone();
    let (mut sender, mut receiver) = codec.framed(stream).split();
    sender.send(req).await?;
    if let Some(response) = receiver.next().await {
      Ok(response?)
    } else {
      Err(Error::Io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "unexpected EOF",
      )))
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Request {
  ElectMe(u64),
  Coordinator { node_name: String, node_id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Response {
  NotYou,
  ItsYou,
  Done(String),
}

pub struct Peer {
  node_name: String,
  node_id: u64,
  transport: Arc<dyn Transport<Request = Request, Response = Response>>,
}

impl Peer {
  pub fn new(node_name: String, node_id: u64, addr: String) -> Self {
    Self {
      node_name,
      node_id,
      transport: Arc::new(TcpTransport::new(addr.into())),
    }
  }

  pub fn with_transport(
    node_name: String,
    node_id: u64,
    transport: Arc<dyn Transport<Request = Request, Response = Response>>,
  ) -> Self {
    Self {
      node_name,
      node_id,
      transport,
    }
  }

  async fn send(&self, req: Request) -> Result<Response> {
    let make_request =
      (|| async { timeout(Duration::from_secs(1), self.transport.send(req.clone())).await }).retry(
        &ExponentialBuilder::default()
          .with_min_delay(Duration::from_millis(300))
          .with_jitter()
          .with_max_delay(Duration::from_secs(1)),
      );
    match make_request.await {
      Ok(Ok(response)) => Ok(response),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(Error::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "timed out",
      ))),
    }
  }
}

async fn manage_state(
  state: Arc<DashMap<String, Peer>>,
  disco: Arc<dyn Discovery>,
  leader: watch::Sender<Option<(String, u64)>>,
) {
  let mut watcher = disco.membership_changes().await;
  while let Some(members) = watcher.next().await {
    if let Ok(members) = members {
      // capture the old state keys
      let old_keys = state
        .iter()
        .map(|kv| kv.key().clone())
        .collect::<HashSet<String>>();

      // collect the new state keys
      let mut new_keys = HashSet::new();
      for member in members {
        if !state.contains_key(&member.name) {
          let peer = Peer::new(member.name.clone(), member.id, member.addr.to_string());
          new_keys.insert(member.name.clone());
          state.insert(member.name, peer);
        }
      }

      // remove the old keys that are not in the new state
      for key in old_keys.difference(&new_keys) {
        let lost_leader = leader
          .borrow()
          .as_ref()
          .is_some_and(|(name, _)| name == key);

        if lost_leader {
          let _ = leader.send(None);
        }
        state.remove(key);
      }
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ElectionResult {
  Lost,
  Won,
}

async fn run_election<'a>(
  this_node: &LocalPeer<'a>,
  state: Arc<DashMap<String, Peer>>,
) -> ElectionResult {
  let mut higher_ranked = state
    .iter()
    .filter(|kv| kv.value().node_id > this_node.id)
    .collect::<Vec<_>>();

  higher_ranked.sort_by_key(|kv| Reverse(kv.value().node_id));

  for kv in higher_ranked {
    let peer = kv.value();

    let maybe_response = peer.send(Request::ElectMe(this_node.id)).await;
    if let Ok(Response::NotYou) = maybe_response {
      if peer.node_id > this_node.id {
        return ElectionResult::Lost;
      }
    }
  }
  ElectionResult::Won
}

async fn broadcast_coordinator<'a>(this_node: &LocalPeer<'a>, state: Arc<DashMap<String, Peer>>) {
  for peer in state.iter() {
    let _ = peer
      .value()
      .send(Request::Coordinator {
        node_name: this_node.name.to_string(),
        node_id: this_node.id,
      })
      .await;
  }
}

async fn monitor_leader<'a>(
  this_node: &LocalPeer<'a>,
  state: Arc<DashMap<String, Peer>>,
  leader_holder: watch::Sender<Option<(String, u64)>>,
) {
  let mut leader_changes = leader_holder.subscribe();
  loop {
    if leader_changes.changed().await.is_err() {
      break;
    }

    match leader_changes.borrow().clone() {
      Some((name, id)) => {
        if id != this_node.id || name != this_node.name {
          continue;
        }

        broadcast_coordinator(this_node, state.clone()).await;
      }
      None => {
        if run_election(this_node, state.clone()).await == ElectionResult::Won {
          leader_holder.send_modify(|v| {
            if let Some((_, id)) = v {
              if *id > this_node.id {
                *v = Some((this_node.name.to_string(), this_node.id));
              }
            }
          });
        }
      }
    }
  }
}

struct ServerRequest(Request, oneshot::Sender<Response>);

async fn handle_requests<'a>(
  this_node: &LocalPeer<'a>,
  leader_holder: watch::Sender<Option<(String, u64)>>,
  stream: TlsStream<TcpStream>,
) {
  let codec: Codec<Request, Response> = tokio_serde_cbor::Codec::new();
  let (mut sender, mut receiver) = codec.framed(stream).split();
  while let Some(request) = receiver.next().await {
    match request {
      ServerRequest(Request::ElectMe(sender_id), reply_to) => {
        if sender_id < this_node.id {
          let _ = reply_to.send(Response::NotYou);
        } else {
          let _ = reply_to.send(Response::ItsYou);
        }
      }
      ServerRequest(Request::Coordinator { node_name, node_id }, reply_to) => {
        leader_holder.send_modify(|v| {
          if let Some((name, id)) = v {
            if *id != node_id || name != &node_name {
              *v = Some((node_name, node_id));
            }
          }
        });
        let _ = reply_to.send(Response::Done(this_node.name.to_string()));
      }
    }
  }
}

struct LocalPeer<'a> {
  id: u64,
  name: &'a str,
}

async fn run_server(conf: ServerConfig, discovery: Arc<dyn Discovery>) -> Result<()> {
  let config = Arc::new(server_tls_config(
    &conf.cluster_cert,
    &conf.cluster_key,
    Some(&conf.cluster_ca),
  )?);
  let acceptor = TlsAcceptor::from(config);
  let addr = conf.cluster_addr.unwrap();
  info!(%addr, "cluster listening");

  let id = discovery.myself().await?;
  let this_node = LocalPeer {
    id: id.id,
    name: &id.name,
  };
  let current_leader = watch::channel(None);

  let listener = TcpListener::bind(addr).await?;
  loop {
    let (stream, _) = listener.accept().await?;
    let acceptor = acceptor.clone();

    tokio::spawn(async move {
      let stream = acceptor.accept(stream).await.unwrap();
      if let Err(e) = handle_requests(&this_node, current_leader.clone()).await {
        tracing::error!("failed to handle request: {}", e);
      }
    });
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use tokio::sync::{mpsc, oneshot};

  use super::*;

  #[tokio::test]
  async fn test_channel_transport() {
    let (req_tx, req_rx) = mpsc::channel(1);
    let (resp_tx, mut resp_rx) = mpsc::channel(1);
    let transport = ChannelTransport::new(resp_tx, req_rx);
    let msg = Request::Coordinator {
      node_name: "test".to_string(),
      node_id: 1,
    };

    let (response_value_tx, response_value_rx) = oneshot::channel();
    let msg_to_send = msg.clone();
    tokio::spawn(async move {
      let response = transport.send(msg_to_send).await;
      response_value_tx.send(response).unwrap();
    });

    assert_eq!(msg, resp_rx.recv().await.unwrap());
    req_tx.send(Response::Done("1".to_string())).await.unwrap();
    assert_eq!(
      response_value_rx.await.unwrap().unwrap(),
      Response::Done("1".to_string())
    );
  }

  #[tokio::test]
  async fn test_tcp_transport() {
    let addr = "".to_string();
  }

  #[tokio::test]
  async fn test_peer() {
    let (req_tx, req_rx) = mpsc::channel(1);
    let (resp_tx, mut resp_rx) = mpsc::channel(1);
    let transport = ChannelTransport::new(resp_tx, req_rx);

    let peer = Peer::with_transport("test".to_string(), 1, Arc::new(transport));
    let msg = Request::Coordinator {
      node_name: "test".to_string(),
      node_id: 1,
    };
    let response = peer.send(msg).await.unwrap();
    assert_eq!(response, Response::Done("".to_string()));
  }
}
