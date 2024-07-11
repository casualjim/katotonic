use std::{
  fmt::Debug,
  net::{IpAddr, SocketAddr},
  sync::Arc,
};

use async_trait::async_trait;
use chitchat::Chitchat;
use futures::{stream::BoxStream, StreamExt as _};
use rustls::pki_types::ServerName;
use tokio::sync::Mutex;
use trust_dns_resolver::{
  proto::rr::rdata::{A, AAAA},
  AsyncResolver,
};

use crate::Result;

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

#[derive(Clone)]
pub struct ChitchatDiscovery {
  chitchat: Arc<Mutex<Chitchat>>,
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
  pub fn new(chitchat: Arc<Mutex<Chitchat>>) -> Self {
    Self { chitchat }
  }

  pub async fn find<S: AsRef<str>>(&self, id: S) -> Option<Member> {
    let guard = self.chitchat.lock().await;

    let cid = guard.live_nodes().find(|cid| cid.node_id == id.as_ref())?;
    let state = guard.node_state(&cid)?;
    let server_name = state.get("server_name")?.to_string();
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
      .get("server_name")
      .map(|v| v.to_string());
    let cid = guard.self_chitchat_id();

    if let Some(sn) = state {
      let server_name = ServerName::try_from(sn.to_string()).ok();
      return Ok(Member {
        name: cid.node_id.clone(),
        id: cid.generation_id,
        addr: cid.gossip_advertise_addr,
        server_name,
      });
    }

    Ok(Member {
      name: cid.node_id.clone(),
      id: cid.generation_id,
      addr: cid.gossip_advertise_addr,
      server_name: None,
    })
  }
}
