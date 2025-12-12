use std::{
  fmt::Debug,
  net::{IpAddr, SocketAddr},
  sync::Arc,
};

use async_trait::async_trait;
use chitchat::Chitchat;
use futures::{stream::BoxStream, StreamExt as _};
use rand::seq::IteratorRandom as _;
use rustls::pki_types::ServerName;
use tokio::sync::{Mutex, MutexGuard, RwLock};
use tokio_stream::wrappers::WatchStream;
use trust_dns_resolver::{
  proto::rr::rdata::{A, AAAA},
  AsyncResolver,
};

use crate::{
  client::{self, AsyncClient},
  ClientConfig, Error, Result, ServerConfig,
};

pub async fn parse_socket_addr(addr: &str) -> Result<SocketAddr> {
  match addr.parse() {
    Ok(addr) => Ok(addr),
    Err(e) => match addr.rsplit_once(':') {
      Some((name, port)) => {
        let port = match port.parse() {
          Ok(port) => port,
          Err(_) => return Err(e.into()),
        };
        let addrs = resolve_dns(name).await?;
        let mut rng = rand::rng();
        let res = addrs
          .iter()
          .filter(|v| matches!(v, IpAddr::V6(_)))
          .choose(&mut rng)
          .map(|ip| SocketAddr::new(*ip, port))
          .or_else(|| {
            addrs
              .iter()
              .filter(|v| matches!(v, IpAddr::V4(_)))
              .choose(&mut rng)
              .map(|ip| SocketAddr::new(*ip, port))
          });
        match res {
          Some(addr) => Ok(addr),
          None => Err(e.into()),
        }
      }
      None => Err(e.into()),
    },
  }
}

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
  pub config_version: u64,
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
  current_client: CachedClientSlot,
}

type CachedClient = (String, Arc<dyn AsyncClient + Send + Sync>);
type CachedClientSlot = Arc<RwLock<Option<CachedClient>>>;

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
        return Ok(Arc::clone(client));
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
    let state = guard.node_state(cid)?;
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

  pub async fn api_addr_for(&self, id: &str, generation: u64) -> Option<String> {
    let guard = self.chitchat.lock().await;
    let cid = guard
      .live_nodes()
      .find(|cid| cid.node_id == id && cid.generation_id == generation)?;
    let state = guard.node_state(cid)?;
    state.get("api_addr").map(|v| v.to_string())
  }

  pub async fn api_addr_for_latest(&self, id: &str) -> Option<String> {
    let guard = self.chitchat.lock().await;
    let cid = guard
      .live_nodes()
      .filter(|cid| cid.node_id == id)
      .max_by_key(|cid| cid.generation_id)?;
    let state = guard.node_state(cid)?;
    state.get("api_addr").map(|v| v.to_string())
  }

  pub async fn set_local_config_version_tag(&self, version: u64) {
    let mut guard = self.chitchat.lock().await;
    guard
      .self_node_state()
      .set("config_version", version.to_string());
  }

  pub async fn app_addrs_without(&self, id: &str) -> Vec<String> {
    let guard = self.chitchat.lock().await;
    let mut addrs = vec![];
    for cid in guard.live_nodes() {
      if cid.node_id == id {
        continue;
      }
      let maybe_api_addr = guard.node_state(cid).and_then(|v| v.get("api_addr"));
      if let Some(api_addr) = maybe_api_addr {
        addrs.push(api_addr.to_string());
      }
    }
    addrs
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
	            .get("cluster_server_name")
	            .or_else(|| state.get("server_name"))
	            .map(|v| v.to_string())
	            .and_then(|sn| ServerName::try_from(sn).ok());
	          let config_version = state
	            .get("config_version")
	            .and_then(|v| v.parse().ok())
	            .unwrap_or(0);
	          members.push(Member {
	            name: cid.node_id.clone(),
	            id: cid.generation_id,
	            addr: cid.gossip_advertise_addr,
	            server_name,
	            config_version,
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
    let state = guard.node_state(cid)?;
    let server_name = state.get("cluster_server_name")?.to_string();
    let config_version = state
      .get("config_version")
      .and_then(|v| v.parse().ok())
      .unwrap_or(0);
    Some(Member {
      name: cid.node_id.clone(),
      id: cid.generation_id,
      addr: cid.gossip_advertise_addr,
      server_name: ServerName::try_from(server_name).ok(),
      config_version,
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
      if let Some(server_name) = guard.node_state(peer).and_then(|v| {
        v.get("cluster_server_name")
          .or_else(|| v.get("server_name"))
          .map(|v| v.to_string())
      }) {
        let config_version = guard
          .node_state(peer)
          .and_then(|v| v.get("config_version"))
          .and_then(|v| v.parse().ok())
          .unwrap_or(0);
        let server_name = ServerName::try_from(server_name).ok();
        let member = Member {
          name: peer.node_id.clone(),
          id: peer.generation_id,
          addr: peer.gossip_advertise_addr,
          server_name,
          config_version,
        };
        members.push(member);
      }
    }
    Ok(members)
  }

  async fn membership_changes(&self) -> BoxStream<'static, Result<Vec<Member>>> {
    let watcher = self.chitchat.lock().await.live_nodes_watcher();

    let stream = WatchStream::new(watcher);
    Box::pin(stream.map(|peers| {
          Ok(
            peers
              .iter()
              .map(|(cid, state)| {
                let server_name = state
                  .get("cluster_server_name")
                  .or_else(|| state.get("server_name"))
                  .map(|v| v.to_string())
                  .unwrap_or_default();
                let config_version = state
                  .get("config_version")
                  .and_then(|v| v.parse().ok())
                  .unwrap_or(0);
                Member {
                  name: cid.node_id.clone(),
                  addr: cid.gossip_advertise_addr,
                  id: cid.generation_id,
                  server_name: ServerName::try_from(server_name).ok(),
                  config_version,
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
      config_version: guard
        .self_node_state()
        .get("config_version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_parse_socket_addr() {
    let addr = "google.com:443";
    let resolved = resolve_dns("google.com").await.unwrap();
    let res = parse_socket_addr(addr).await;
    assert!(res.is_ok());
    let addr = res.unwrap();
    assert_eq!(addr.port(), 443);
    assert!(resolved.contains(&addr.ip()));
  }
}
