use std::{borrow::Cow, net::IpAddr, sync::Arc, time::Duration};

use futures::{future::join_all, StreamExt as _};
use rand::Rng as _;
use rustls::pki_types::ServerName;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use super::{rpc::QuorumPeer, LeaderState, QuorumRequest, QuorumResponse, QuorumService};
use crate::{
  disco::{Discovery, Member},
  keypair, root_store, Result, ServerConfig, WatchableValue,
};

pub async fn track_leader(
  conf: ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
) -> Result<(WatchableValue<LeaderState>, Arc<QuorumService>)> {
  let leader_state = WatchableValue::new(LeaderState::Down);
  let service = Arc::new(QuorumService::new(
    conf.state_path.clone(),
    leader_state.clone(),
  )?);

  let election_conf = conf.clone();
  let election_discovery = Arc::clone(&discovery);
  let election_leader_state = leader_state.clone();
  let election_service = Arc::clone(&service);
  tokio::spawn(async move {
    if let Err(e) = election_loop(
      election_conf,
      election_discovery,
      election_leader_state,
      election_service,
    )
    .await
    {
      error!("election loop exited: {e}");
    }
  });

  let monitor_discovery = Arc::clone(&discovery);
  let monitor_leader_state = leader_state.clone();
  tokio::spawn(async move {
    if let Err(e) = membership_monitor(monitor_discovery, monitor_leader_state).await {
      error!("membership monitor exited: {e}");
    }
  });

  let lease_conf = conf.clone();
  let lease_discovery = Arc::clone(&discovery);
  let lease_leader_state = leader_state.clone();
  let lease_service = Arc::clone(&service);
  tokio::spawn(async move {
    if let Err(e) =
      super::lease::track_lease(lease_conf, lease_discovery, lease_leader_state, lease_service)
        .await
    {
      error!("lease loop exited: {e}");
    }
  });

  Ok((leader_state, service))
}

async fn membership_monitor(
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
) -> Result<()> {
  let mut changes = discovery.membership_changes().await;
  while let Some(members) = changes.next().await {
    let members = members?;
    if let LeaderState::Follower { leader, term } = leader_state.read() {
      if !members.iter().any(|m| m.name == leader) {
        warn!(
          leader,
          term, "leader missing from membership; triggering election"
        );
        leader_state.write(LeaderState::Down);
      }
    }
  }
  Ok(())
}

async fn election_loop(
  conf: ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
  service: Arc<QuorumService>,
) -> Result<()> {
  loop {
    match leader_state.read() {
      LeaderState::Down => {
        let delay = random_election_delay();
        sleep(delay).await;
        if !matches!(leader_state.read(), LeaderState::Down) {
          continue;
        }
        let term = service.bump_term_self_vote().await?;
        leader_state.write(LeaderState::Candidate { term });
        if let Err(e) = run_election_attempt(
          &conf,
          Arc::clone(&discovery),
          leader_state.clone(),
          &service,
          term,
        )
        .await
        {
          warn!("election attempt failed: {e}");
          leader_state.write(LeaderState::Down);
        }
      }
      _ => {
        leader_state.wait_for_change_async().await;
      }
    }
  }
}

fn random_election_delay() -> Duration {
  let mut rng = rand::rng();
  let ms = rng.random_range(800..=1500);
  Duration::from_millis(ms)
}

async fn run_election_attempt(
  conf: &ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
  service: &QuorumService,
  proposed_term: u64,
) -> Result<()> {
  let this_node = discovery.myself().await?;
  let members = discovery.members().await?;
  let peers = members
    .into_iter()
    .filter(|m| m.name != this_node.name)
    .collect::<Vec<_>>();

  let majority = (peers.len() + 2).div_ceil(2);
  debug!(
    proposed_term,
    majority,
    peers = peers.len(),
    "starting quorum election"
  );

  let client_config = cluster_client_config(conf)?;
  let mut tasks = Vec::new();
  for peer in peers {
    let addr = Cow::Owned(peer.addr.to_string());
    let server_name = server_name_for(&peer);
    let peer_client = QuorumPeer::new_tls(addr, client_config.clone(), server_name);
    let candidate_id = this_node.name.clone();

    tasks.push(tokio::spawn(async move {
      timeout(
        Duration::from_secs(1),
        peer_client.send(QuorumRequest::VoteRequest {
          proposed_term,
          candidate_id,
        }),
      )
      .await
    }));
  }

  let mut yes = 1usize; // self vote
  let mut max_hw = service.get_state().await.high_watermark;
  let mut highest_seen_term = proposed_term;

  for res in join_all(tasks).await {
    match res {
      Ok(Ok(Ok(response))) => match response {
        QuorumResponse::VoteYes {
          term,
          high_watermark,
          ..
        } => {
          highest_seen_term = highest_seen_term.max(term);
          if term == proposed_term {
            yes += 1;
            max_hw = max_hw.max(high_watermark);
          }
        }
        QuorumResponse::VoteNo {
          term,
          voted_term,
          high_watermark,
          ..
        } => {
          highest_seen_term = highest_seen_term.max(term).max(voted_term);
          max_hw = max_hw.max(high_watermark);
        }
        other => {
          debug!(?other, "unexpected quorum response during election");
        }
      },
      Ok(Ok(Err(e))) => {
        debug!("vote request failed: {e}");
      }
      Ok(Err(_elapsed)) => {
        debug!("vote request timed out");
      }
      Err(join_err) => {
        debug!("vote task join error: {join_err}");
      }
    }
  }

  if highest_seen_term > proposed_term {
    service.observe_higher_voted_term(highest_seen_term).await?;
    leader_state.write(LeaderState::Down);
    return Ok(());
  }

  if yes >= majority {
    info!(proposed_term, yes, majority, "won quorum election");
    service.install_leader_term(proposed_term, max_hw).await?;
    leader_state.write(LeaderState::Leader {
      term: proposed_term,
      lease_valid: false,
    });
  } else {
    info!(proposed_term, yes, majority, "lost quorum election");
    leader_state.write(LeaderState::Down);
  }

  Ok(())
}

pub(crate) fn cluster_client_config(conf: &ServerConfig) -> Result<rustls::ClientConfig> {
  let client_config =
    rustls::ClientConfig::builder().with_root_certificates(root_store(conf.cluster_ca.clone())?);

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

  Ok(client_config)
}

pub(crate) fn server_name_for(member: &Member) -> ServerName<'static> {
  member
    .server_name
    .clone()
    .unwrap_or_else(|| match member.addr.ip() {
      IpAddr::V4(ip) => ServerName::IpAddress(ip.into()),
      IpAddr::V6(ip) => ServerName::IpAddress(ip.into()),
    })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_random_delay_in_range() {
    for _ in 0..50 {
      let d = random_election_delay();
      assert!(d >= Duration::from_millis(800));
      assert!(d <= Duration::from_millis(1500));
    }
  }
}
