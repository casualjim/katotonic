use std::{
  net::{IpAddr, SocketAddr},
  pin::Pin,
  sync::Arc,
};

use async_trait::async_trait;
use chitchat::Chitchat;
use futures::{stream::BoxStream, Stream, StreamExt as _};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Member {
  pub id: u64,
  pub addr: SocketAddr,
}

#[async_trait]
pub trait Discovery: Send + Sync {
  async fn membership_changes(&self) -> BoxStream<Result<Vec<Member>>>;
  async fn members(&self) -> Result<Vec<Member>>;
  async fn member_count(&self) -> Result<usize> {
    Ok(self.members().await?.len())
  }
  async fn myself(&self) -> Result<Member>;
}

pub struct ChitchatDiscovery {
  chitchat: Arc<Mutex<Chitchat>>,
}

impl ChitchatDiscovery {
  pub fn new(chitchat: Arc<Mutex<Chitchat>>) -> Self {
    Self { chitchat }
  }
}

#[async_trait]
impl Discovery for ChitchatDiscovery {
  async fn members(&self) -> Result<Vec<Member>> {
    let guard = self.chitchat.lock().await;
    let online_peers = guard.live_nodes();
    Ok(
      online_peers
        .map(|cid| Member {
          id: cid.generation_id,
          addr: cid.gossip_advertise_addr,
        })
        .collect(),
    )
  }

  async fn membership_changes(&self) -> BoxStream<Result<Vec<Member>>> {
    let watcher = self.chitchat.lock().await.live_nodes_watcher();
    Box::pin(watcher.map(|peers| {
      Ok(
        peers
          .iter()
          .map(|cid| Member {
            id: cid.0.generation_id,
            addr: cid.0.gossip_advertise_addr,
          })
          .collect(),
      )
    }))
  }

  async fn myself(&self) -> Result<Member> {
    let mut guard = self.chitchat.lock().await;
    let state = guard.self_node_state();

    let cid = guard.self_chitchat_id();

    Ok(Member {
      id: cid.generation_id,
      addr: cid.gossip_advertise_addr,
    })
  }
}
