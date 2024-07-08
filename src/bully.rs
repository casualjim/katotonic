use std::{borrow::Cow, cmp::Reverse, collections::HashSet, io, sync::Arc, time::Duration};

use backon::{ExponentialBuilder, Retryable};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt as _};
use serde::{Deserialize, Serialize};
use tokio::{
  io::{AsyncRead, AsyncWrite},
  net::TcpListener,
  sync::watch,
  time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_serde_cbor::Codec;
use tokio_util::codec::Decoder;
use tracing::{error, info};

use super::disco::Discovery;
use crate::{
  server_tls_config,
  transport::{TcpTransport, Transport},
  Error, Result, ServerConfig,
};

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
  node_id: u64,
  transport: Arc<dyn Transport<Request = Request, Response = Response>>,
}

impl Peer {
  pub fn new(node_id: u64, addr: String) -> Self {
    Self {
      node_id,
      transport: Arc::new(TcpTransport::new(addr.into())),
    }
  }

  pub fn with_transport(
    node_id: u64,
    transport: Arc<dyn Transport<Request = Request, Response = Response>>,
  ) -> Self {
    Self { node_id, transport }
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
          let peer = Peer::new(member.id, member.addr.to_string());
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
    let res = peer
      .value()
      .send(Request::Coordinator {
        node_name: this_node.name.to_string(),
        node_id: this_node.id,
      })
      .await;
    if let Ok(Response::Done(_name)) = res {}
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

    let leader = leader_changes.borrow().clone();
    match leader {
      Some((name, id)) => {
        if id != this_node.id || name != this_node.name {
          continue;
        }

        broadcast_coordinator(this_node, state.clone()).await;
      }
      None => {
        drop(leader);
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

async fn handle_requests<'a, S: AsyncRead + AsyncWrite>(
  this_node: &LocalPeer<'a>,
  leader_holder: watch::Sender<Option<(String, u64)>>,
  stream: S,
) {
  let codec: Codec<Request, Response> = tokio_serde_cbor::Codec::new();
  let (mut sender, mut receiver) = codec.framed(stream).split();
  while let Some(maybe_error) = receiver.next().await {
    if let Err(e) = maybe_error {
      error!("failed to decode request: {}", e);
      continue;
    }
    match maybe_error.unwrap() {
      Request::ElectMe(sender_id) => {
        if sender_id < this_node.id {
          let _ = sender.send(Response::NotYou).await;
        } else {
          let _ = sender.send(Response::ItsYou).await;
        }
      }
      Request::Coordinator { node_name, node_id } => {
        leader_holder.send_modify(|v| {
          if let Some((name, id)) = v {
            if *id != node_id || name != &node_name {
              *v = Some((node_name, node_id));
            }
          }
        });
        let _ = sender.send(Response::Done(this_node.name.to_string()));
      }
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LocalPeer<'a> {
  id: u64,
  name: Cow<'a, str>,
}

async fn run_server(
  conf: ServerConfig,
  discovery: Arc<dyn Discovery>,
  current_leader: watch::Sender<Option<(String, u64)>>,
) -> Result<()> {
  let config = Arc::new(server_tls_config(
    &conf.cluster_cert,
    &conf.cluster_key,
    Some(&conf.cluster_ca),
  )?);
  let acceptor = TlsAcceptor::from(config);
  let addr = conf.cluster_addr.unwrap();
  info!(%addr, "cluster listening");

  let id = discovery.myself().await?;

  let listener = TcpListener::bind(addr).await?;
  loop {
    let (stream, _) = listener.accept().await?;
    let acceptor = acceptor.clone();

    let this_node = LocalPeer {
      id: id.id,
      name: id.name.clone().into(),
    };

    let current_leader = current_leader.clone();
    tokio::spawn(async move {
      let stream = acceptor.accept(stream).await.unwrap();
      handle_requests(&this_node, current_leader.clone(), stream).await
    });
  }
}

pub async fn track_leader(
  conf: ServerConfig,
  discovery: Arc<dyn Discovery>,
) -> Result<watch::Receiver<Option<(String, u64)>>> {
  let (leader_holder, leader_receiver) = watch::channel(None as Option<(String, u64)>);

  let state = Arc::new(DashMap::new());

  tokio::spawn({
    let disco = Arc::clone(&discovery);
    let state = Arc::clone(&state);
    let leader_holder = leader_holder.clone();
    async move {
      manage_state(state, disco, leader_holder).await;
    }
  });

  let this_node = discovery.myself().await?;
  let state_clone = Arc::clone(&state);
  tokio::spawn({
    let leader_holder = leader_holder.clone();
    async move {
      monitor_leader(
        &LocalPeer {
          id: this_node.id,
          name: this_node.name.clone().into(),
        },
        state_clone,
        leader_holder.clone(),
      )
      .await;
    }
  });

  tokio::spawn(async move {
    run_server(conf, discovery, leader_holder).await.unwrap();
  });

  Ok(leader_receiver)
}

#[cfg(test)]
mod tests {

  use tokio::sync::mpsc;

  use super::*;
  use crate::transport::ChannelTransport;

  #[tokio::test]
  async fn test_peer() {
    let (req_tx, req_rx) = mpsc::channel(1);
    let (resp_tx, mut resp_rx) = mpsc::channel(1);
    let transport = ChannelTransport::new(resp_tx, req_rx);

    tokio::spawn(async move {
      while let Some(_) = resp_rx.recv().await {
        req_tx.send(Response::Done("".to_string())).await.unwrap();
      }
    });

    let peer = Peer::with_transport(1, Arc::new(transport));
    let msg = Request::Coordinator {
      node_name: "test".to_string(),
      node_id: 1,
    };
    let response = peer.send(msg).await.unwrap();
    assert_eq!(response, Response::Done("".to_string()));
  }
}
