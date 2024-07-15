use std::{
  fmt::Debug,
  net::{IpAddr, SocketAddr},
  sync::Arc,
};

use async_trait::async_trait;
use chitchat::Chitchat;
use futures::{stream::BoxStream, StreamExt as _};
use rustls::pki_types::ServerName;
use tokio::sync::{Mutex, MutexGuard, RwLock};
use trust_dns_resolver::{
  proto::rr::rdata::{A, AAAA},
  AsyncResolver,
};

use crate::{
  client::{self, AsyncClient},
  ClientConfig, Error, Result, ServerConfig,
};

pub async fn resolve_dns(name: &str) -> Result<Vec<IpAddr>> {
  let resolver = AsyncResolver::tokio_from_system_conf()?;
  // Resolve IPv4 addresses
  let ipv4_response = resolver.ipv4_lookup(name).await?;
  let ipv4_addresses: Vec<IpAddr> = ipv4_response
    .iter()
    .map(|&A(addr)| IpAddr::V4(addr))
    .collect();

  // Resolve IPv6 addresses
  let ipv6_response = resolver.ipv6_lookup(name).await?;
  let ipv6_addresses: Vec<IpAddr> = ipv6_response
    .iter()
    .map(|&AAAA(addr)| IpAddr::V6(addr))
    .collect();

  // Combine both IPv4 and IPv6 addresses
  let all_addresses: Vec<IpAddr> = [ipv4_addresses, ipv6_addresses].concat();

  Ok(all_addresses)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Member {
  pub name: String,
  pub id: u64,
  pub addr: SocketAddr,
  pub server_name: Option<ServerName<'static>>,
}

#[cfg_attr(test, mry::mry)]
#[async_trait]
pub trait Discovery: std::fmt::Debug + Send + Sync + 'static {
  async fn membership_changes(&self) -> BoxStream<'static, Result<Vec<Member>>>;
  async fn members(&self) -> Result<Vec<Member>>;
  async fn member_count(&self) -> Result<usize> {
    Ok(self.members().await?.len())
  }
  async fn myself(&self) -> Result<Member>;
}

#[async_trait]
pub trait ApiClientFactory: Send + Sync {
  type ClientFactory: AsyncClientFactory;
  async fn api_client(
    &self,
    id: &(String, u64),
    factory: Self::ClientFactory,
  ) -> Result<Arc<dyn AsyncClient + Send + Sync>>;
}

#[async_trait]
pub trait AsyncClientFactory: Send + Sync {
  async fn new_client(
    &self,
    config: ClientConfig,
    concurrency: usize,
  ) -> Result<Arc<dyn AsyncClient + Send + Sync>>;
}

pub struct TokioClientFactory;

#[async_trait]
impl AsyncClientFactory for TokioClientFactory {
  async fn new_client(
    &self,
    config: ClientConfig,
    concurrency: usize,
  ) -> Result<Arc<dyn AsyncClient + Send + Sync>> {
    let client = client::Client::new(config, concurrency).await?;
    Ok(Arc::new(client))
  }
}

#[derive(Clone)]
pub struct ChitchatDiscovery {
  chitchat: Arc<Mutex<Chitchat>>,
  conf: ServerConfig,
  current_client: Arc<RwLock<Option<(String, Arc<dyn AsyncClient + Send + Sync>)>>>,
}

#[async_trait]
impl ApiClientFactory for ChitchatDiscovery {
  type ClientFactory = TokioClientFactory;

  async fn api_client(
    &self,
    id: &(String, u64),
    cf: Self::ClientFactory,
  ) -> Result<Arc<dyn AsyncClient + Send + Sync>> {
    let guard = self.current_client.read().await;
    if let Some((ref current_id, ref client)) = *guard {
      if current_id == &id.0 {
        return Ok(Arc::clone(&client));
      }
    }
    drop(guard);

    let guard = self.chitchat.lock().await;

    let client_config = self
      .api_client_config_for(&guard, &id.0)
      .ok_or_else(|| Error::UnexpectedResponse("unable to create client config".to_string()))?;
    let client = cf.new_client(client_config, 5).await?;
    let result = client.clone();
    self
      .current_client
      .write()
      .await
      .replace((id.0.clone(), client));
    Ok(result)
  }
}

impl ChitchatDiscovery {
  fn api_client_config_for(&self, guard: &MutexGuard<Chitchat>, id: &str) -> Option<ClientConfig> {
    let cid = guard.live_nodes().find(|cid| cid.node_id == id)?;
    let state = guard.node_state(&cid)?;
    let addr = state.get("api_addr")?.to_string();
    let server_name = state.get("api_server_name")?.to_string();

    Some(ClientConfig {
      cert: self.conf.client_cert.clone(),
      key: self.conf.client_key.clone(),
      ca: self.conf.ca.clone(),
      addr,
      server_name: Some(server_name),
    })
  }
}

impl Debug for ChitchatDiscovery {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let rt = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap();
    let mut b = f.debug_struct("ChitchatDiscovery");

    let cc = self.chitchat.clone();
    let members = rt.block_on(async move {
      let guard = cc.lock().await;
      let nodes = guard.live_nodes();
      let mut members = vec![];
      for cid in nodes {
        if let Some(state) = guard.node_state(cid) {
          let server_name = state
            .get("server_name")
            .map(|v| v.to_string())
            .and_then(|sn| ServerName::try_from(sn).ok());
          members.push(Member {
            name: cid.node_id.clone(),
            id: cid.generation_id,
            addr: cid.gossip_advertise_addr,
            server_name,
          });
        }
      }
      members
    });
    b.field("members", &members);
    b.finish()
  }
}

impl ChitchatDiscovery {
  pub fn new(chitchat: Arc<Mutex<Chitchat>>, conf: ServerConfig) -> Self {
    Self {
      chitchat,
      conf,
      current_client: Arc::new(RwLock::new(None)),
    }
  }

  pub async fn find<S: AsRef<str>>(&self, id: S) -> Option<Member> {
    let guard = self.chitchat.lock().await;

    let cid = guard.live_nodes().find(|cid| cid.node_id == id.as_ref())?;
    let state = guard.node_state(&cid)?;
    let server_name = state.get("cluster_server_name")?.to_string();
    Some(Member {
      name: cid.node_id.clone(),
      id: cid.generation_id,
      addr: cid.gossip_advertise_addr,
      server_name: ServerName::try_from(server_name).ok(),
    })
  }
}

#[async_trait]
impl Discovery for ChitchatDiscovery {
  async fn members(&self) -> Result<Vec<Member>> {
    let guard = self.chitchat.lock().await;
    let online_peers = guard.live_nodes();

    let mut members = vec![];
    for peer in online_peers {
      if let Some(server_name) = guard
        .node_state(peer)
        .and_then(|v| v.get("server_name").map(|v| v.to_string()))
      {
        let server_name = ServerName::try_from(server_name).ok();
        let member = Member {
          name: peer.node_id.clone(),
          id: peer.generation_id,
          addr: peer.gossip_advertise_addr,
          server_name,
        };
        members.push(member);
      }
    }
    Ok(members)
  }

  async fn membership_changes(&self) -> BoxStream<'static, Result<Vec<Member>>> {
    let watcher = self.chitchat.lock().await.live_nodes_watcher();

    Box::pin(watcher.map(|peers| {
      Ok(
        peers
          .iter()
          .map(|(cid, state)| {
            let server_name = state
              .get("server_name")
              .map(|v| v.to_string())
              .unwrap_or_default();
            Member {
              name: cid.node_id.clone(),
              addr: cid.gossip_advertise_addr,
              id: cid.generation_id,
              server_name: ServerName::try_from(server_name).ok(),
            }
          })
          .collect(),
      )
    }))
  }

  async fn myself(&self) -> Result<Member> {
    let mut guard = self.chitchat.lock().await;
    let state = guard
      .self_node_state()
      .get("cluster_server_name")
      .map(|v| v.to_string());
    let cid = guard.self_chitchat_id();

    Ok(Member {
      name: cid.node_id.clone(),
      id: cid.generation_id,
      addr: cid.gossip_advertise_addr,
      server_name: state.and_then(|sn| ServerName::try_from(sn).ok()),
    })
  }
}
