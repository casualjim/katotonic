use std::{
  borrow::Cow, cmp::Reverse, collections::HashSet, io, net::SocketAddr, sync::Arc, time::Duration,
};

use dashmap::DashMap;
use futures::{future::try_join_all, SinkExt, StreamExt as _};
use rustls::{
  pki_types::{IpAddr, Ipv4Addr, ServerName},
  ClientConfig,
};
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
use tracing::{debug, error, info, warn};

use super::disco::Discovery;
use crate::{
  keypair, root_store, server_tls_config,
  transport::{TcpTransport, TlsTransport, Transport},
  Error, Result, ServerConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
  ElectMe(u64),
  Coordinator { node_name: String, node_id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
  NotYou { node_name: String, node_id: u64 },
  ItsYou { node_name: String, node_id: u64 },
  Done(String),
}

pub type PeerFactory = Arc<dyn Fn(u64, SocketAddr) -> Peer + Send + Sync>;

fn with_transport_factory(
  create_transport: impl Fn(u64, SocketAddr) -> Arc<dyn Transport<Request = Request, Response = Response>>
    + Send
    + Sync
    + 'static,
) -> PeerFactory {
  Arc::new(move |id, addr| Peer::with_transport(id, create_transport(id, addr)))
}

#[derive(Clone)]
pub struct Peer {
  id: u64,
  transport: Arc<dyn Transport<Request = Request, Response = Response>>,
}

impl Peer {
  pub fn new(id: u64, addr: String) -> Self {
    Self {
      id,
      transport: Arc::new(TcpTransport::with_codec(
        addr.into(),
        Codec::<Response, Request>::new(),
      )),
    }
  }

  pub fn with_transport(
    id: u64,
    transport: Arc<dyn Transport<Request = Request, Response = Response>>,
  ) -> Self {
    Self { id, transport }
  }

  async fn send(&self, req: Request) -> Result<Response> {
    match timeout(Duration::from_secs(1), self.transport.send(req.clone())).await {
      Ok(Ok(response)) => Ok(response),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(Error::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "request timed out",
      ))),
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PeerState {
  Down,
  Participant(String, u64),
  Leader(String, u64),
  Follower(String, u64),
}

async fn manage_state(
  state: Arc<DashMap<String, Peer>>,
  disco: Arc<dyn Discovery>,
  leader: watch::Sender<PeerState>,
  transport_factory: PeerFactory,
) {
  let mut watcher = disco.membership_changes().await;

  let this_node = disco.myself().await.unwrap();

  while let Some(members) = watcher.next().await {
    if let Ok(members) = members {
      // capture the old state keys
      let old_keys = state
        .clone()
        .iter()
        .map(|kv| kv.key().clone())
        .collect::<HashSet<String>>();

      // collect the new state keys
      let mut new_keys = HashSet::new();
      for member in members {
        new_keys.insert(member.name.clone());
        if !state.contains_key(&member.name) {
          let peer = transport_factory(member.id, member.addr);
          state.insert(member.name.clone(), peer.clone());
          let ldr = leader.borrow().clone();
          match ldr {
            PeerState::Leader(name, id) => {
              if member.id > id {
                leader.send_replace(PeerState::Down);
              } else if &this_node.name == &name && this_node.id == id {
                if let Err(e) = peer
                  .send(Request::Coordinator {
                    node_name: this_node.name.to_string(),
                    node_id: this_node.id,
                  })
                  .await
                {
                  error!(name=%member.name, id=%member.id, "failed to send coordinator message: {:?}", e);
                }
              }
            }
            PeerState::Follower(_, id) => {
              if member.id > id {
                leader.send_replace(PeerState::Down);
              }
            }
            _ => {}
          }
        }
      }

      // remove the old keys that are not in the new state
      old_keys.difference(&new_keys).for_each(|key| {
        state.remove(key);
      });
      let ldr = leader.borrow().clone();
      match ldr {
        PeerState::Follower(name, _) | PeerState::Leader(name, _) => {
          if !state.contains_key(&name) {
            leader.send_replace(PeerState::Down);
          }
        }
        _ => {}
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
  debug!(id = %this_node.id, name = %this_node.name, "running election");
  let mut higher_ranked = state
    .iter()
    .filter(|kv| kv.value().id > this_node.id)
    .collect::<Vec<_>>();

  higher_ranked.sort_by_key(|kv| Reverse(kv.value().id));

  let mut requests = Vec::new();
  for kv in higher_ranked {
    let peer = kv.value().clone();
    let node_id = this_node.id;

    requests.push(tokio::spawn(async move {
      match peer.send(Request::ElectMe(node_id)).await {
        Ok(Response::NotYou { .. }) => ElectionResult::Lost,
        Err(e) => {
          error!("failed to send election request: {}", e);
          ElectionResult::Won
        }
        _ => {
          // we are the leader
          ElectionResult::Won
        }
      }
    }));
  }
  if let Ok(result) = try_join_all(requests).await {
    for res in result {
      if res == ElectionResult::Lost {
        return ElectionResult::Lost;
      }
    }
  }

  ElectionResult::Won
}

async fn broadcast_coordinator<'a>(this_node: &LocalPeer<'a>, state: Arc<DashMap<String, Peer>>) {
  let broadcast_size = if state.is_empty() { 0 } else { state.len() - 1 };
  debug!(
    broadcast_size,
    "broadcasting coordinator to all peers ({})", broadcast_size
  );
  let mut broadcasted = 0;
  for peer in state.iter() {
    if peer.value().id == this_node.id {
      continue;
    }
    let res = peer
      .value()
      .send(Request::Coordinator {
        node_name: this_node.name.to_string(),
        node_id: this_node.id,
      })
      .await;
    broadcasted += 1;
    if let Ok(Response::Done(_name)) = res {}
  }
  debug!(broadcasted, "broadcasted coordinator to all peers");
  debug_assert_eq!(broadcasted, broadcast_size);
}

async fn monitor_leader<'a>(
  this_node: &LocalPeer<'a>,
  state: Arc<DashMap<String, Peer>>,
  leader_holder: watch::Sender<PeerState>,
) {
  let mut leader_changes = leader_holder.subscribe();
  loop {
    let leader = leader_changes.borrow_and_update().clone();
    match leader {
      PeerState::Leader(name, id) => {
        if id != this_node.id || name != this_node.name {
          continue;
        }

        broadcast_coordinator(this_node, state.clone()).await;
      }
      PeerState::Participant(_, _) => {}
      PeerState::Follower(_, _) => {}
      PeerState::Down => {
        let election_result = run_election(this_node, state.clone()).await;
        if election_result == ElectionResult::Won {
          leader_holder.send_replace(PeerState::Leader(this_node.name.to_string(), this_node.id));
        }
      }
    }
    if leader_changes.changed().await.is_err() {
      warn!("leader changes channel closed");
      break;
    }
    info!("leader changed: {:?}", leader_changes.borrow().clone());
  }
}

async fn handle_requests<'a, S: AsyncRead + AsyncWrite + Unpin>(
  this_node: &LocalPeer<'a>,
  codec: Codec<Request, Response>,
  leader_holder: watch::Sender<PeerState>,
  stream: S,
) {
  let (mut sender, mut receiver) = codec.framed(stream).split();
  while let Some(maybe_error) = receiver.next().await {
    if let Err(e) = maybe_error {
      match e {
        tokio_serde_cbor::Error::Io(ref e) => {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            error!("failed to decode request: io error: {}", e);
          }
        }
        e => error!("failed to decode request: {:?}", e),
      }

      continue;
    }
    match maybe_error.unwrap() {
      Request::ElectMe(sender_id) => {
        debug!(sender_id, "got election message");
        if sender_id < this_node.id {
          let _ = sender
            .send(Response::NotYou {
              node_id: this_node.id,
              node_name: this_node.name.to_string(),
            })
            .await;
        } else {
          let _ = sender
            .send(Response::ItsYou {
              node_id: this_node.id,
              node_name: this_node.name.to_string(),
            })
            .await;
        }
      }
      Request::Coordinator { node_name, node_id } => {
        let _ = sender.send(Response::Done(this_node.name.to_string()));
        debug!(node_id, node_name, "got coordinator message");
        leader_holder.send_modify(|v| {
          let maybe_follower = PeerState::Follower(node_name, node_id);
          if v != &maybe_follower {
            *v = maybe_follower
          }
        });
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
  current_leader: watch::Sender<PeerState>,
) -> Result<()> {
  let codec = Codec::new();
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
    let codec = codec.clone();
    tokio::spawn(async move {
      let stream = acceptor.accept(stream).await.unwrap();
      handle_requests(&this_node, codec, current_leader.clone(), stream).await
    });
  }
}

fn tls_transport_factory(conf: &ServerConfig) -> Result<PeerFactory> {
  let client_config =
    ClientConfig::builder().with_root_certificates(root_store(conf.cluster_ca.clone())?);

  let client_config = match conf.cluster_client_cert.as_ref().and_then(|client_cert| {
    conf
      .cluster_client_key
      .as_ref()
      .and_then(|client_key| keypair(client_cert, client_key).ok())
  }) {
    Some((client_cert, client_key)) => {
      client_config.with_client_auth_cert(client_cert, client_key)?
    }
    None => client_config.with_no_client_auth(),
  };

  Ok(with_transport_factory(move |_, peer_addr| {
    Arc::new(TlsTransport::new(
      Cow::Owned(peer_addr.to_string()),
      client_config.clone(),
      ServerName::IpAddress(IpAddr::V4(Ipv4Addr::try_from("127.0.0.1").unwrap())),
    ))
  }))
}

pub async fn track_leader(
  conf: ServerConfig,
  discovery: Arc<dyn Discovery>,
  peer_factory: Option<PeerFactory>,
) -> Result<watch::Receiver<PeerState>> {
  let (leader_holder, leader_receiver) = watch::channel(PeerState::Down);

  let state = Arc::new(DashMap::new());

  let this_node = discovery.myself().await?;

  let factory = if let Some(factory) = peer_factory {
    factory
  } else {
    tls_transport_factory(&conf)?
  };

  tokio::spawn({
    let discovery = Arc::clone(&discovery);
    let leader_holder = leader_holder.clone();
    async move {
      run_server(conf, discovery, leader_holder).await.unwrap();
    }
  });

  tokio::spawn({
    let disco = Arc::clone(&discovery);
    let state = Arc::clone(&state);
    let leader_holder = leader_holder.clone();
    async move {
      manage_state(state, disco, leader_holder, factory).await;
    }
  });

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

  Ok(leader_receiver)
}

#[cfg(test)]
mod tests {

  use std::{
    pin::Pin,
    task::{Context, Poll},
  };

  use futures::{ready, stream::BoxStream, Stream};
  use tokio::sync::{mpsc, Mutex};
  use tokio_util::sync::ReusableBoxFuture;
  use tracing::debug;
  use watch::{error::RecvError, Receiver};

  use super::*;
  use crate::{
    disco::Member,
    transport::{channel_server_client, ChannelTransport},
  };

  pub struct WatchStream<T> {
    inner: ReusableBoxFuture<'static, (Result<(), RecvError>, Receiver<T>)>,
  }

  async fn make_future<T: Clone + Send + Sync>(
    mut rx: Receiver<T>,
  ) -> (Result<(), RecvError>, Receiver<T>) {
    let result = rx.changed().await;
    (result, rx)
  }

  impl<T: 'static + Clone + Send + Sync> WatchStream<T> {
    /// Create a new `WatchStream`.
    pub fn new(rx: Receiver<T>) -> Self {
      Self {
        inner: ReusableBoxFuture::new(async move { (Ok(()), rx) }),
      }
    }

    /// Create a new `WatchStream` that waits for the value to be changed.
    pub fn from_changes(rx: Receiver<T>) -> Self {
      Self {
        inner: ReusableBoxFuture::new(make_future(rx)),
      }
    }
  }

  impl<T: Clone + 'static + Send + Sync> Stream for WatchStream<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      let (result, mut rx) = ready!(self.inner.poll(cx));
      match result {
        Ok(_) => {
          let received = (*rx.borrow_and_update()).clone();
          self.inner.set(make_future(rx));
          Poll::Ready(Some(Ok(received)))
        }
        Err(_) => {
          self.inner.set(make_future(rx));
          Poll::Ready(None)
        }
      }
    }
  }

  impl<T> Unpin for WatchStream<T> {}

  impl<T> std::fmt::Debug for WatchStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("WatchStream").finish()
    }
  }

  impl<T: 'static + Clone + Send + Sync> From<Receiver<T>> for WatchStream<T> {
    fn from(recv: Receiver<T>) -> Self {
      Self::new(recv)
    }
  }

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

  #[tokio::test]
  async fn runs_election_only_for_higher_ranked_peers() {
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count1 = Arc::clone(&count);
    let count2 = Arc::clone(&count);
    let transport = channel_server_client(move |_: Request| {
      let count = Arc::clone(&count1);
      async move {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Response::NotYou {
          node_id: 10,
          node_name: "peer1".to_string(),
        })
      }
    });

    let transport2 = channel_server_client(move |_: Request| {
      let count = Arc::clone(&count2);
      count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
      async move {
        Ok(Response::NotYou {
          node_id: 1,
          node_name: "peer2".to_string(),
        })
      }
    });

    let state = Arc::new(DashMap::new());
    state.insert(
      "peer1".to_string(),
      Peer::with_transport(10, Arc::new(transport)),
    );
    state.insert(
      "peer2".to_string(),
      Peer::with_transport(1, Arc::new(transport2)),
    );

    let this_node = LocalPeer {
      id: 5,
      name: "test".into(),
    };

    let result = run_election(&this_node, state.clone()).await;
    assert_eq!(result, ElectionResult::Lost);
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);

    let this_node = LocalPeer {
      id: 15,
      name: "test-highest".into(),
    };

    let result = run_election(&this_node, state).await;
    assert_eq!(result, ElectionResult::Won);
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
  }

  #[tokio::test]
  async fn manages_state() {
    let state = Arc::new(DashMap::new());
    let this_node = Member {
      name: "test".to_string(),
      id: 1,
      addr: "127.0.0.1:9001".parse().unwrap(),
      server_name: None,
    };

    let this_node_transport =
      channel_server_client(|_: Request| async move { Ok(Response::Done("".to_string())) });

    state.insert(
      this_node.name.clone(),
      Peer::with_transport(1, Arc::new(this_node_transport)),
    );
    let (sd, tx) = StaticDiscovery::new(this_node.clone());
    let disco = Arc::new(sd);
    let (leader_tx, mut leader_rx) = watch::channel(PeerState::Down);

    tokio::spawn(manage_state(
      state.clone(),
      disco.clone(),
      leader_tx.clone(),
      with_transport_factory(move |_, addr| Arc::new(TcpTransport::new(addr.to_string().into()))),
    ));

    // Initial state: should contain the `this_node`
    assert!(state.contains_key(&this_node.name));

    // Test member addition
    let new_member = Member {
      name: "new_member".to_string(),
      id: 2,
      addr: "127.0.0.1:9002".parse().unwrap(),
      server_name: None,
    };
    let new_member3 = Member {
      name: "new_member3".to_string(),
      id: 3,
      addr: "127.0.0.1:9003".parse().unwrap(),
      server_name: None,
    };
    tx.send(vec![
      this_node.clone(),
      new_member.clone(),
      new_member3.clone(),
    ])
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    assert!(state.contains_key(&this_node.name));
    assert!(state.contains_key(&new_member.name));
    assert!(state.contains_key(&new_member3.name));

    // Test member removal
    tx.send(vec![this_node.clone(), new_member.clone()])
      .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    assert!(!state.contains_key(&new_member3.name));
    let expected = PeerState::Leader(new_member.name.clone(), new_member.id);
    leader_tx.send(expected.clone()).unwrap();

    assert_eq!(leader_rx.borrow_and_update().clone(), expected);

    // Test leader loss
    tx.send(vec![this_node.clone()]).unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    assert_eq!(leader_rx.borrow_and_update().clone(), PeerState::Down);
  }

  fn counting_client_server(
    name: &'static str,
    count: Arc<std::sync::atomic::AtomicUsize>,
  ) -> Arc<dyn Transport<Request = Request, Response = Response>> {
    let transport = channel_server_client(move |req: Request| {
      let count = Arc::clone(&count);
      async move {
        match req {
          Request::ElectMe(_) => Ok(Response::NotYou {
            node_id: 1,
            node_name: name.to_string(),
          }),
          Request::Coordinator { .. } => {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Response::Done(name.to_string()))
          }
        }
      }
    });
    Arc::new(transport)
  }
  #[tokio::test(flavor = "multi_thread")]
  async fn broadcasts_coordinator() {
    let this_node = LocalPeer {
      id: 5,
      name: "test".into(),
    };

    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let this_transport = counting_client_server("test", Arc::clone(&count));
    let this_peer = Peer::with_transport(this_node.id, this_transport);

    let trans1 = counting_client_server("member1", Arc::clone(&count));
    let trans2 = counting_client_server("member2", Arc::clone(&count));
    let trans3 = counting_client_server("member3", Arc::clone(&count));

    let state = Arc::new(DashMap::new());
    state.insert(this_node.name.to_string(), this_peer);
    state.insert("member1".to_string(), Peer::with_transport(1, trans1));
    state.insert("member2".to_string(), Peer::with_transport(10, trans2));
    state.insert("member3".to_string(), Peer::with_transport(7, trans3));

    broadcast_coordinator(&this_node, state.clone()).await;
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 3);
  }

  fn counting_client_server_monitor(
    this_node: &LocalPeer,
    election_count: Arc<std::sync::atomic::AtomicUsize>,
    commit_count: Arc<std::sync::atomic::AtomicUsize>,
  ) -> Arc<dyn Transport<Request = Request, Response = Response>> {
    let name = this_node.name.to_string();
    let id = this_node.id;

    let transport = channel_server_client(move |req: Request| {
      let commit_count = Arc::clone(&commit_count);
      let election_count = Arc::clone(&election_count);

      let name = name.clone();
      async move {
        match req {
          Request::ElectMe(sender_id) => {
            debug!(id, sender_id, "got election message");
            election_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if sender_id < id {
              debug!(id, sender_id, "sender is not it");
              Ok(Response::NotYou {
                node_id: id,
                node_name: name.to_string(),
              })
            } else {
              debug!(id, sender_id, "sender is it");
              Ok(Response::ItsYou {
                node_id: id,
                node_name: name.to_string(),
              })
            }
          }
          Request::Coordinator { node_id, node_name } => {
            debug!(id, node_id, node_name, "got commit message");
            commit_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Response::Done(name.to_string()))
          }
        }
      }
    });
    Arc::new(transport)
  }

  #[tokio::test(flavor = "multi_thread")]
  async fn monitors_leader() {
    console_subscriber::init();
    let this_node = LocalPeer {
      id: 5,
      name: "test".into(),
    };

    let election_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let commit_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let this_transport = counting_client_server_monitor(
      &this_node,
      Arc::clone(&election_count),
      Arc::clone(&commit_count),
    );
    let this_peer = Peer::with_transport(this_node.id, this_transport);

    let lpeer1 = LocalPeer {
      id: 1,
      name: "member1".into(),
    };
    let trans1 = counting_client_server_monitor(
      &lpeer1,
      Arc::clone(&election_count),
      Arc::clone(&commit_count),
    );
    let lpeer2 = LocalPeer {
      id: 10,
      name: "member2".into(),
    };
    let trans2 = counting_client_server_monitor(
      &lpeer2,
      Arc::clone(&election_count),
      Arc::clone(&commit_count),
    );
    let lpeer3 = LocalPeer {
      id: 7,
      name: "member3".into(),
    };
    let trans3 = counting_client_server_monitor(
      &lpeer3,
      Arc::clone(&election_count),
      Arc::clone(&commit_count),
    );

    let state = Arc::new(DashMap::new());
    state.insert(this_node.name.to_string(), this_peer);
    state.insert(
      lpeer1.name.to_string(),
      Peer::with_transport(lpeer1.id, trans1),
    );

    let (leader_tx, mut leader_rx) = watch::channel(PeerState::Down);
    tokio::spawn({
      let this_node = this_node.clone();
      let state = state.clone();
      let leader_tx = leader_tx.clone();
      async move {
        monitor_leader(&this_node, state, leader_tx).await;
      }
    });
    assert!(leader_rx.changed().await.is_ok());
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    assert_eq!(election_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(commit_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
      leader_rx.borrow().clone(),
      PeerState::Leader(this_node.name.to_string(), 5)
    );

    state.insert(
      lpeer2.name.to_string(),
      Peer::with_transport(lpeer2.id, trans2),
    );

    state.insert(
      lpeer3.name.to_string(),
      Peer::with_transport(lpeer3.id, trans3),
    );

    leader_tx.send_replace(PeerState::Down);
    assert!(leader_rx.changed().await.is_ok());
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    assert_eq!(election_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(commit_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    assert_eq!(
      run_election(&lpeer3, state.clone()).await,
      ElectionResult::Lost
    );
    assert_eq!(election_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert_eq!(commit_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
      run_election(&lpeer2, state.clone()).await,
      ElectionResult::Won
    );
    assert_eq!(election_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert_eq!(commit_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    broadcast_coordinator(&lpeer2, state).await;
    assert_eq!(election_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert_eq!(commit_count.load(std::sync::atomic::Ordering::SeqCst), 4);
  }

  #[derive(Clone, Debug)]
  struct StaticDiscovery {
    watch: Arc<Mutex<watch::Receiver<Vec<Member>>>>,
    this_node: Member,
  }

  impl StaticDiscovery {
    pub fn new(this_node: Member) -> (Self, watch::Sender<Vec<Member>>) {
      let (tx, rx) = watch::channel(vec![this_node.clone()]);
      (
        Self {
          watch: Arc::new(Mutex::new(rx)),
          this_node,
        },
        tx,
      )
    }
  }

  #[async_trait::async_trait]
  impl Discovery for StaticDiscovery {
    async fn members(&self) -> Result<Vec<Member>> {
      Ok(self.watch.lock().await.borrow_and_update().clone())
    }

    async fn membership_changes(&self) -> BoxStream<'static, Result<Vec<Member>>> {
      Box::pin(WatchStream::from_changes(self.watch.lock().await.clone()))
    }

    async fn myself(&self) -> Result<Member> {
      Ok(self.this_node.clone())
    }
  }
}
