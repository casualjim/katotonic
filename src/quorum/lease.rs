use std::{
  borrow::Cow,
  net::IpAddr,
  sync::Arc,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::future::join_all;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

use super::{election, rpc::QuorumPeer, LeaderState, QuorumRequest, QuorumResponse, QuorumService};
use crate::{
  disco::{Discovery, Member},
  Result, ServerConfig, WatchableValue,
};

const LEASE_RENEW_INTERVAL: Duration = Duration::from_millis(300);
const LEASE_DURATION: Duration = Duration::from_millis(1000);
const LEASE_RPC_TIMEOUT: Duration = Duration::from_millis(750);

#[derive(Debug, Default, Clone)]
pub(crate) struct FollowerLeaseState {
  acked_term: u64,
  acked_leader: Option<String>,
}

impl FollowerLeaseState {
  pub fn allows(&self, term: u64, leader_id: &str) -> bool {
    if term == self.acked_term {
      if let Some(ref existing) = self.acked_leader {
        return existing == leader_id;
      }
    }
    true
  }

  pub fn record(&mut self, term: u64, leader_id: String) {
    self.acked_term = term;
    self.acked_leader = Some(leader_id);
  }
}

pub async fn track_lease(
  conf: ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
  service: Arc<QuorumService>,
) -> Result<()> {
  loop {
    match leader_state.read() {
      LeaderState::Leader { term, .. } => {
        if let Err(e) = leader_lease_loop(
          &conf,
          Arc::clone(&discovery),
          leader_state.clone(),
          &service,
          term,
        )
        .await
        {
          warn!(term, "leader lease loop failed: {e}");
          leader_state.write(LeaderState::Down);
        }
      }
      _ => {
        leader_state.wait_for_change_async().await;
      }
    }
  }
}

async fn leader_lease_loop(
  conf: &ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
  service: &QuorumService,
  term: u64,
) -> Result<()> {
  let mut lease_valid = false;
  let mut lease_until_ms = 0u64;

  loop {
    if !matches!(
      leader_state.read(),
      LeaderState::Leader { term: t, .. } if t == term
    ) {
      return Ok(());
    }

    let now = now_ms();
    let proposed_until_ms = now.saturating_add(LEASE_DURATION.as_millis() as u64);

    let this_node = discovery.myself().await?;
    let members = discovery.members().await?;
    let peers = members
      .into_iter()
      .filter(|m| m.name != this_node.name)
      .collect::<Vec<_>>();

    let majority = (peers.len() + 2).div_ceil(2);
    let leader_id = this_node.name.clone();

    let client_config = election::cluster_client_config(conf)?;
    let mut tasks = Vec::new();
    for peer in peers {
      let addr = Cow::Owned(peer.addr.to_string());
      let server_name = election::server_name_for(&peer);
      let peer_client = QuorumPeer::new_tls(addr, client_config.clone(), server_name);
      let leader_id = leader_id.clone();
      tasks.push(tokio::spawn(async move {
        timeout(
          LEASE_RPC_TIMEOUT,
          peer_client.send(QuorumRequest::LeaseRenew {
            term,
            leader_id,
            lease_until_ms: proposed_until_ms,
          }),
        )
        .await
      }));
    }

    let mut yes = 1usize; // self-ack
    let mut highest_seen_term = term;
    let mut max_hw = service.get_state().await.high_watermark;

    for res in join_all(tasks).await {
      match res {
        Ok(Ok(Ok(response))) => match response {
          QuorumResponse::LeaseAck {
            term: ack_term,
            high_watermark,
          } => {
            highest_seen_term = highest_seen_term.max(ack_term);
            if ack_term == term {
              yes += 1;
              max_hw = max_hw.max(high_watermark);
            }
          }
          QuorumResponse::LeaseNack {
            term: nack_term,
            voted_term,
          } => {
            highest_seen_term = highest_seen_term.max(nack_term).max(voted_term);
          }
          QuorumResponse::VoteNo {
            term: other_term,
            voted_term,
            high_watermark,
            ..
          } => {
            highest_seen_term = highest_seen_term
              .max(other_term)
              .max(voted_term);
            max_hw = max_hw.max(high_watermark);
          }
          other => {
            debug!(?other, "unexpected quorum response during lease renew");
          }
        },
        Ok(Ok(Err(e))) => {
          debug!("lease renew request failed: {e}");
        }
        Ok(Err(_elapsed)) => {
          debug!("lease renew request timed out");
        }
        Err(join_err) => {
          debug!("lease renew task join error: {join_err}");
        }
      }
    }

    if highest_seen_term > term {
      service.observe_higher_voted_term(highest_seen_term).await?;
      warn!(
        term,
        highest_seen_term,
        "observed higher term during lease renew; stepping down"
      );
      leader_state.write(LeaderState::Down);
      return Ok(());
    }

    if yes >= majority {
      lease_valid = true;
      lease_until_ms = proposed_until_ms;
      if matches!(
        leader_state.read(),
        LeaderState::Leader { term: t, .. } if t == term
      ) {
        leader_state.write(LeaderState::Leader {
          term,
          lease_valid: true,
        });
      }
      info!(
        term,
        yes,
        majority,
        lease_until_ms,
        max_hw,
        "lease renewed"
      );
    } else if lease_valid {
      if now >= lease_until_ms {
        warn!(
          term,
          yes,
          majority,
          lease_until_ms,
          "lease expired without majority; stepping down"
        );
        leader_state.write(LeaderState::Down);
        return Ok(());
      }
      warn!(
        term,
        yes,
        majority,
        lease_until_ms,
        "lease renew failed; keeping previous lease"
      );
    } else {
      if lease_until_ms == 0 || now >= lease_until_ms {
        lease_until_ms = proposed_until_ms;
      }
      if matches!(
        leader_state.read(),
        LeaderState::Leader { term: t, .. } if t == term
      ) {
        leader_state.write(LeaderState::Leader {
          term,
          lease_valid: false,
        });
      }
      warn!(
        term,
        yes,
        majority,
        lease_until_ms,
        "failed to acquire majority lease"
      );
    }

    sleep(LEASE_RENEW_INTERVAL).await;
  }
}

fn now_ms() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}

#[allow(dead_code)]
fn server_name_for(member: &Member) -> rustls::pki_types::ServerName<'static> {
  member
    .server_name
    .clone()
    .unwrap_or_else(|| match member.addr.ip() {
      IpAddr::V4(ip) => rustls::pki_types::ServerName::IpAddress(ip.into()),
      IpAddr::V6(ip) => rustls::pki_types::ServerName::IpAddress(ip.into()),
    })
}
