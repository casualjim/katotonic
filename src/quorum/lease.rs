use std::{
  borrow::Cow,
  net::IpAddr,
  sync::Arc,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::future::join_all;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

use super::{
  config::JointQuorum, election, rpc::QuorumPeer, LeaderState, QuorumRequest, QuorumResponse,
  QuorumService,
};
use crate::{
  disco::{Discovery, Member},
  Result, ServerConfig, WatchableValue,
};

const LEASE_RPC_TIMEOUT: Duration = Duration::from_millis(750);

#[derive(Debug, Default, Clone)]
pub(crate) struct FollowerLeaseState {
  acked_term: u64,
  acked_leader: Option<String>,
}

#[derive(Debug, Clone)]
struct LeaseRoundDecision {
  highest_seen_term: u64,
  yes: usize,
  old_yes: usize,
  has_joint_majority: bool,
  has_old_majority: bool,
  lease_until_ms: u64,
  max_hw: u64,
  expired: bool,
}

fn evaluate_lease_round<I>(
  term: u64,
  quorum_cfg: &JointQuorum,
  this_is_old: bool,
  state_high_watermark: u64,
  responses: I,
  now_ms: u64,
  proposed_until_ms: u64,
  prev_lease_valid: bool,
  prev_lease_until_ms: u64,
) -> LeaseRoundDecision
where
  I: IntoIterator<Item = (bool, QuorumResponse)>,
{
  let mut yes = 1usize; // self-ack
  let mut old_yes = if this_is_old { 1usize } else { 0usize };
  let mut highest_seen_term = term;
  let mut max_hw = state_high_watermark;

  for (is_old, response) in responses {
    match response {
      QuorumResponse::LeaseAck {
        term: ack_term,
        high_watermark,
      } => {
        highest_seen_term = highest_seen_term.max(ack_term);
        if ack_term == term {
          yes += 1;
          if is_old {
            old_yes += 1;
          }
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
        highest_seen_term = highest_seen_term.max(other_term).max(voted_term);
        max_hw = max_hw.max(high_watermark);
      }
      other => {
        debug!(?other, "unexpected quorum response during lease renew");
      }
    }
  }

  if highest_seen_term > term {
    return LeaseRoundDecision {
      highest_seen_term,
      yes,
      old_yes,
      has_joint_majority: false,
      has_old_majority: false,
      lease_until_ms: prev_lease_until_ms,
      max_hw,
      expired: false,
    };
  }

  let joint_majority = quorum_cfg.joint_majority();
  let old_majority = quorum_cfg.old_majority();
  let has_joint_majority = yes >= joint_majority;
  let has_old_majority = !quorum_cfg.is_joint() || old_yes >= old_majority;

  let has_majority = has_joint_majority && has_old_majority;

  if has_majority {
    LeaseRoundDecision {
      highest_seen_term,
      yes,
      old_yes,
      has_joint_majority,
      has_old_majority,
      lease_until_ms: proposed_until_ms,
      max_hw,
      expired: false,
    }
  } else if prev_lease_valid {
    let expired = now_ms >= prev_lease_until_ms;
    LeaseRoundDecision {
      highest_seen_term,
      yes,
      old_yes,
      has_joint_majority,
      has_old_majority,
      lease_until_ms: prev_lease_until_ms,
      max_hw,
      expired,
    }
  } else {
    let lease_until_ms = if prev_lease_until_ms == 0 || now_ms >= prev_lease_until_ms {
      proposed_until_ms
    } else {
      prev_lease_until_ms
    };
    LeaseRoundDecision {
      highest_seen_term,
      yes,
      old_yes,
      has_joint_majority,
      has_old_majority,
      lease_until_ms,
      max_hw,
      expired: false,
    }
  }
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
  let lease_duration = Duration::from_millis(conf.lease_duration_ms.max(1));
  let renew_interval = Duration::from_millis(conf.renew_interval_ms.max(1));
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
    let proposed_until_ms = now.saturating_add(lease_duration.as_millis() as u64);

    let this_node = discovery.myself().await?;
    let members = discovery.members().await?;
    let state = service.get_state().await;
    let local_config_version = state.config_version;
    let quorum_cfg = JointQuorum::new(local_config_version, &members);
    let joint_majority = quorum_cfg.joint_majority();
    let old_majority = quorum_cfg.old_majority();

    let peers = members
      .into_iter()
      .filter(|m| m.name != this_node.name)
      .collect::<Vec<_>>();

    let leader_id = this_node.name.clone();

    let client_config = election::cluster_client_config(conf)?;
    let mut tasks = Vec::new();
    for peer in peers {
      let addr = Cow::Owned(peer.addr.to_string());
      let server_name = election::server_name_for(&peer);
      let peer_client = QuorumPeer::new_tls(addr, client_config.clone(), server_name);
      let leader_id = leader_id.clone();
      let is_old = quorum_cfg.is_old(&peer.name);
      tasks.push(tokio::spawn(async move {
        (
          is_old,
          timeout(
            LEASE_RPC_TIMEOUT,
            peer_client.send(QuorumRequest::LeaseRenew {
              term,
              leader_id,
              lease_until_ms: proposed_until_ms,
            }),
          )
          .await,
        )
      }));
    }

    let mut responses = Vec::new();
    for res in join_all(tasks).await {
      match res {
        Ok((is_old, Ok(Ok(response)))) => responses.push((is_old, response)),
        Ok((_is_old, Ok(Err(e)))) => {
          debug!("lease renew request failed: {e}");
        }
        Ok((_is_old, Err(_elapsed))) => {
          debug!("lease renew request timed out");
        }
        Err(join_err) => {
          debug!("lease renew task join error: {join_err}");
        }
      }
    }

    let decision = evaluate_lease_round(
      term,
      &quorum_cfg,
      quorum_cfg.is_old(&this_node.name),
      state.high_watermark,
      responses,
      now,
      proposed_until_ms,
      lease_valid,
      lease_until_ms,
    );

    if decision.highest_seen_term > term {
      service.observe_higher_voted_term(decision.highest_seen_term).await?;
      warn!(
        term,
        highest_seen_term = decision.highest_seen_term,
        "observed higher term during lease renew; stepping down"
      );
      leader_state.write(LeaderState::Down);
      return Ok(());
    }

    if decision.has_joint_majority && decision.has_old_majority {
      lease_valid = true;
      lease_until_ms = decision.lease_until_ms;
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
        yes = decision.yes,
        joint_majority,
        old_yes = decision.old_yes,
        old_majority,
        joint = quorum_cfg.is_joint(),
        lease_until_ms,
        max_hw = decision.max_hw,
        "lease renewed"
      );
    } else if lease_valid {
      if decision.expired {
        warn!(
          term,
          yes = decision.yes,
          joint_majority,
          old_yes = decision.old_yes,
          old_majority,
          joint = quorum_cfg.is_joint(),
          lease_until_ms,
          "lease expired without majority; stepping down"
        );
        leader_state.write(LeaderState::Down);
        return Ok(());
      }
      warn!(
        term,
        yes = decision.yes,
        joint_majority,
        old_yes = decision.old_yes,
        old_majority,
        joint = quorum_cfg.is_joint(),
        lease_until_ms,
        "lease renew failed; keeping previous lease"
      );
    } else {
      lease_valid = false;
      lease_until_ms = decision.lease_until_ms;
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
        yes = decision.yes,
        joint_majority,
        old_yes = decision.old_yes,
        old_majority,
        joint = quorum_cfg.is_joint(),
        lease_until_ms,
        "failed to acquire majority lease"
      );
    }

    sleep(renew_interval).await;
  }
}

fn now_ms() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::disco::Member;
  use std::net::{IpAddr, Ipv4Addr, SocketAddr};

  fn members_three() -> Vec<Member> {
    let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1111);
    let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2222);
    let addr3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3333);
    vec![
      Member {
        name: "n1".to_string(),
        id: 1,
        addr: addr1,
        server_name: None,
        config_version: 0,
      },
      Member {
        name: "n2".to_string(),
        id: 2,
        addr: addr2,
        server_name: None,
        config_version: 0,
      },
      Member {
        name: "n3".to_string(),
        id: 3,
        addr: addr3,
        server_name: None,
        config_version: 0,
      },
    ]
  }

  #[test]
  fn lease_round_acquires_majority_sets_valid_until() {
    let members = members_three();
    let quorum_cfg = JointQuorum::new(0, &members);
    let term = 1;
    let now = 1000;
    let proposed_until = 2000;
    let responses = vec![
      (
        true,
        QuorumResponse::LeaseAck {
          term,
          high_watermark: 10,
        },
      ),
      (
        true,
        QuorumResponse::LeaseAck {
          term,
          high_watermark: 8,
        },
      ),
    ];

    let decision = evaluate_lease_round(
      term,
      &quorum_cfg,
      true,
      0,
      responses,
      now,
      proposed_until,
      false,
      0,
    );

    assert_eq!(decision.lease_until_ms, proposed_until);
    assert!(!decision.expired);
    assert!(decision.has_joint_majority);
    assert!(decision.has_old_majority);
  }

  #[test]
  fn lease_round_expires_without_majority_steps_down() {
    let members = members_three();
    let quorum_cfg = JointQuorum::new(0, &members);
    let term = 2;
    let responses: Vec<(bool, QuorumResponse)> = Vec::new();

    let decision = evaluate_lease_round(
      term,
      &quorum_cfg,
      true,
      0,
      responses,
      3000,
      4000,
      true,
      2500,
    );

    assert!(decision.expired);
  }

  #[test]
  fn lease_round_observes_higher_term_triggers_stepdown() {
    let members = members_three();
    let quorum_cfg = JointQuorum::new(0, &members);
    let term = 5;
    let responses = vec![(
      true,
      QuorumResponse::LeaseNack {
        term,
        voted_term: 7,
      },
    )];

    let decision = evaluate_lease_round(
      term,
      &quorum_cfg,
      true,
      0,
      responses,
      1000,
      2000,
      true,
      1500,
    );

    assert!(decision.highest_seen_term > term);
    assert_eq!(decision.highest_seen_term, 7);
  }
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
