use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use dashmap::DashSet;
use futures::{SinkExt, StreamExt as _};
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::{
  net::{TcpListener, TcpStream},
  sync::watch,
};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tokio_serde_cbor::Codec;
use tokio_util::codec::Decoder;
use ulid::Ulid;

use crate::{Result, SeedNode};

pub async fn run(conf: crate::ServerConfig) -> Result<()> {
  let server_config = Arc::new(crate::server_tls_config(
    &conf.cluster_cert,
    &conf.cluster_key,
    Some(&conf.cluster_ca),
  )?);
  let client_config = Arc::new(crate::client_tls_config(
    conf.cluster_client_cert.as_ref(),
    conf.cluster_client_key.as_ref(),
    &conf.cluster_ca,
  )?);
  let bully = Bully::new(
    Ulid::new(),
    conf.cluster_addr()?.ok_or(io::Error::new(
      io::ErrorKind::InvalidInput,
      "Missing cluster address",
    ))?,
    SmolStr::new(conf.server_name()),
    server_config,
    client_config,
  );
  bully.start(conf.seed.iter(), conf.bootstrap).await;
  Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Message {
  Heartbeat,
  Election,
  Coordinator(Ulid),
  Ok,
  Join(Node),
  NodeList(Vec<Node>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Node {
  pub id: Ulid,
  pub addr: SocketAddr,
  pub hostname: SmolStr,
}

impl Node {
  pub fn server_name(&self) -> ServerName {
    ServerName::try_from(self.hostname.to_string()).unwrap()
  }
}

struct State {
  id: Node,
  nodes: DashSet<Node>,
  leader: watch::Sender<Option<Node>>,
}

async fn start_election(state: Arc<State>, client: Client) {
  let self_id = state.id.id;
  let higher_nodes: Vec<Node> = state
    .nodes
    .iter()
    .filter(|node| node.id > self_id)
    .map(|v| v.clone())
    .collect();
  if higher_nodes.is_empty() {
    declare_victory(state, client).await;
  } else {
    for node in higher_nodes {
      let _ = client.send_message(node, Message::Election).await;
    }
  }
}

async fn declare_victory(state: Arc<State>, client: Client) {
  let leader_node = state.id.clone();
  state.leader.send(Some(leader_node.clone())).unwrap();
  client
    .broadcast(
      state.nodes.iter().map(|v| v.clone()),
      Message::Coordinator(leader_node.id),
    )
    .await;
}

pub struct Bully {
  state: Arc<State>,
  // server_config: Arc<rustls::ServerConfig>,
  // client_config: Arc<rustls::ClientConfig>,
  client: Client,
}

async fn handle_connection(stream: TcpStream, state: Arc<State>, client: Client) {
  let codec: Codec<Message, Message> = Codec::new();
  let mut framed = codec.framed(stream);

  while let Some(Ok(message)) = framed.next().await {
    let state = state.clone();
    match message {
      Message::Join(new_node) => {
        state.nodes.insert(new_node);
        let nodes: Vec<Node> = state.nodes.iter().map(|v| v.clone()).collect();
        let response = Message::NodeList(nodes);
        framed.send(response).await.unwrap();
      }
      Message::NodeList(nodes) => {
        for node in nodes {
          state.nodes.insert(node);
        }
      }
      Message::Election => {
        start_election(state, client.clone()).await;
      }
      Message::Coordinator(leader_id) => {
        let leader = state
          .nodes
          .iter()
          .find(|n| n.id == leader_id)
          .map(|n| n.clone());
        if let Some(leader) = leader {
          state.leader.send(Some(leader)).unwrap();
        }
      }
      _ => {}
    }
  }
}

impl Bully {
  pub fn new(
    id: Ulid,
    addr: SocketAddr,
    server_name: SmolStr,
    server_config: Arc<rustls::ServerConfig>,
    client_config: Arc<rustls::ClientConfig>,
  ) -> Self {
    let (tx, _) = watch::channel(None);
    let this_node = Node {
      id,
      addr,
      hostname: server_name,
    };
    let client = Client::new(this_node.clone(), client_config.clone());
    Self {
      state: Arc::new(State {
        id: this_node,
        nodes: DashSet::new(),
        leader: tx,
      }),
      server_config,
      client_config,
      client,
    }
  }

  pub fn leader_changes(&self) -> watch::Receiver<Option<Node>> {
    self.state.leader.subscribe()
  }

  pub fn leader(&self) -> Option<Node> {
    self.state.leader.borrow().clone()
  }

  pub async fn start(&self, seeds: impl Iterator<Item = &SeedNode>, bootstrap: bool) {
    if bootstrap {
      let node = self.state.id.clone();
      self.state.nodes.insert(node.clone());
      self.state.leader.send(Some(node)).unwrap();
    } else {
      for seed in seeds {
        self.join_cluster(seed).await;
      }
    }

    // Clone the state and client to move into the async task
    let state = self.state.clone();
    let client = self.client.clone();

    // Start TCP server to listen for incoming connections
    let listener = TcpListener::bind(self.state.id.addr).await.unwrap();
    tokio::spawn(async move {
      while let Ok((stream, _)) = listener.accept().await {
        let state = state.clone();
        let client = client.clone();
        tokio::spawn(async move {
          handle_connection(stream, state, client).await;
        });
      }
    });

    // Start heartbeat and election timeout tasks
    self.start_heartbeat().await;
    self.start_election_timeout().await;
  }

  async fn join_cluster(&self, seed: &SeedNode) {
    let new_node = self.state.id.clone();
    let join_msg = Message::Join(new_node.clone());

    let seed_node = Node {
      id: Ulid::nil(),
      addr: seed.addr,
      hostname: SmolStr::new(seed.server_name.as_str()),
    };

    if let Ok(Message::NodeList(nodes)) = self.client.send_message(seed_node, join_msg).await {
      for node in nodes {
        self.state.nodes.insert(node);
      }
    }
  }

  async fn start_heartbeat(&self) {
    let interval = Duration::from_secs(1);
    let client = self.client.clone();
    let state = self.state.clone();
    let self_id = state.id.clone();
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(interval);
      let state = state.clone();
      let client = client.clone();
      loop {
        interval.tick().await;
        let client = client.clone();
        let leader = { state.leader.borrow().clone() };
        if let Some(leader) = leader {
          if leader.id != self_id.id {
            let message = Message::Heartbeat;
            if client.send_message(leader.clone(), message).await.is_err() {
              state.leader.send(None).unwrap();
              // Call start_election from a cloned context
              let state_clone = state.clone();
              tokio::spawn(async move {
                start_election(state_clone, client.clone()).await;
              });
            }
          }
        }
      }
    });
  }

  async fn start_election_timeout(&self) {
    let timeout = Duration::from_secs(5);
    let state = self.state.clone();
    let client = self.client.clone();
    tokio::spawn(async move {
      loop {
        tokio::time::sleep(timeout).await;
        let client = client.clone();
        if state.leader.borrow().is_none() {
          // Call start_election from a cloned context
          let state_clone = state.clone();
          tokio::spawn(async move {
            start_election(state_clone, client.clone()).await;
          });
        }
      }
    });
  }
}

#[derive(Clone)]
struct Client {
  id: Node,
  connector: TlsConnector,
}

impl Client {
  pub fn new(id: Node, client_config: Arc<rustls::ClientConfig>) -> Self {
    let connector = TlsConnector::from(client_config);
    Self { id, connector }
  }

  pub async fn send_message(&self, node: Node, message: Message) -> crate::Result<Message> {
    let stream = self.connect(node).await?;

    let codec: Codec<Message, Message> = tokio_serde_cbor::Codec::new();
    let (mut sender, mut receiver) = codec.framed(stream).split();
    sender.send(message).await?;

    let response = receiver
      .next()
      .await
      .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))??;
    Ok(response)
  }

  async fn connect(&self, node: Node) -> crate::Result<TlsStream<TcpStream>> {
    let stream = TcpStream::connect(node.addr).await?;
    let hn = node.hostname.to_string();
    let server_name = ServerName::try_from(hn)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let stream = self.connector.connect(server_name, stream).await?;
    Ok(stream)
  }

  pub async fn fire_forget(&self, node: Node, message: Message) -> crate::Result<()> {
    let stream = self.connect(node).await?;

    let codec: Codec<Message, Message> = tokio_serde_cbor::Codec::new();
    let (mut sender, _) = codec.framed(stream).split();
    sender.send(message).await?;
    Ok(())
  }

  pub async fn broadcast(&self, peers: impl Iterator<Item = Node>, message: Message) {
    for peer in peers {
      if self.id == peer {
        continue;
      }
      let _ = self.send_message(peer, message.clone()).await;
    }
  }
}
