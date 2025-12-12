use std::{borrow::Cow, net::IpAddr, sync::Arc, time::Duration};

use futures::{future::join_all, StreamExt as _};
use rand::Rng as _;
use rustls::pki_types::ServerName;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use super::{
  config::JointQuorum, rpc::QuorumPeer, LeaderState, QuorumRequest, QuorumResponse, QuorumService,
};
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

  let cluster_conf = conf.clone();
  let cluster_service = Arc::clone(&service);
  tokio::spawn(async move {
    if let Err(e) = super::cluster::run_cluster_server(cluster_conf, cluster_service).await {
      error!("quorum cluster server exited: {e}");
    }
  });

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
  let monitor_conf = conf.clone();
  let monitor_service = Arc::clone(&service);
  tokio::spawn(async move {
    if let Err(e) =
      membership_monitor(monitor_conf, monitor_discovery, monitor_leader_state, monitor_service)
        .await
    {
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
  conf: ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
  service: Arc<QuorumService>,
) -> Result<()> {
  let mut changes = discovery.membership_changes().await;
  let mut last_joint_triggered_version = service.get_state().await.config_version;
  while let Some(members) = changes.next().await {
    let members = members?;
    let local_config_version = service.get_state().await.config_version;
    let max_config_version = members
      .iter()
      .map(|m| m.config_version)
      .max()
      .unwrap_or(local_config_version);

    match leader_state.read() {
      LeaderState::Follower { leader, term } => {
        if !members.iter().any(|m| m.name == leader) {
          warn!(
            leader,
            term, "leader missing from membership; triggering election"
          );
          leader_state.write(LeaderState::Down);
          continue;
        }

        if max_config_version > local_config_version {
          if let Some(leader_member) = members.iter().find(|m| m.name == leader) {
            let client_config = cluster_client_config(&conf)?;
            let addr = Cow::Owned(leader_member.addr.to_string());
            let server_name = server_name_for(leader_member);
            let peer = QuorumPeer::new_tls(addr, client_config, server_name);
            match timeout(Duration::from_secs(1), peer.send(QuorumRequest::CatchupRequest)).await {
              Ok(Ok(QuorumResponse::CatchupResponse {
                term,
                high_watermark,
                config_version,
              })) => {
                let before_version = service.get_state().await.config_version;
                if let Err(e) = service
                  .install_catchup(term, high_watermark, config_version)
                  .await
                {
                  warn!("failed to install catchup state: {e}");
                } else {
                  let after_version = service.get_state().await.config_version;
                  if after_version > before_version {
                    discovery.set_local_config_version_tag(after_version).await;
                    last_joint_triggered_version =
                      last_joint_triggered_version.max(after_version);
                  }
                  info!(
                    term,
                    high_watermark,
                    config_version,
                    "installed catchup state from leader"
                  );
                }
              }
              Ok(Ok(other)) => {
                debug!(?other, "unexpected response to catchup request");
              }
              Ok(Err(e)) => {
                debug!("catchup request failed: {e}");
              }
              Err(_elapsed) => {
                debug!("catchup request timed out");
              }
            }
          }
        }
      }
      LeaderState::Leader {
        term,
        lease_valid,
      } => {
        if max_config_version > local_config_version {
          if max_config_version > last_joint_triggered_version {
            warn!(
              term,
              local_config_version,
              max_config_version,
              "config version change detected; triggering election for joint quorum"
            );
            last_joint_triggered_version = max_config_version;
            leader_state.write(LeaderState::Down);
          } else if lease_valid {
            match service.set_config_version_to(max_config_version).await {
              Ok(true) => {
                discovery
                  .set_local_config_version_tag(max_config_version)
                  .await;
                info!(
                  term,
                  config_version = max_config_version,
                  "committed config version and updated discovery tag"
                );
              }
              Ok(false) => {}
              Err(e) => {
                warn!(
                  term,
                  max_config_version,
                  "failed to commit config version: {e}"
                );
              }
            }
          }
        }
      }
      _ => {}
    };
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
        let delay = random_election_delay(&conf);
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

fn random_election_delay(conf: &ServerConfig) -> Duration {
  let mut rng = rand::rng();
  let min = conf.election_timeout_min_ms.max(1);
  let max = conf.election_timeout_max_ms.max(min);
  let ms = if max == min {
    min
  } else {
    rng.random_range(min..=max)
  };
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
  let state = service.get_state().await;
  let local_config_version = state.config_version;
  let max_config_version = members
    .iter()
    .map(|m| m.config_version)
    .max()
    .unwrap_or(local_config_version);

  let quorum_cfg = JointQuorum::new(local_config_version, &members);
  let joint_majority = quorum_cfg.joint_majority();
  let old_majority = quorum_cfg.old_majority();

  let peers = members
    .into_iter()
    .filter(|m| m.name != this_node.name)
    .collect::<Vec<_>>();
  debug!(
    proposed_term,
    joint_majority,
    old_majority,
    joint = quorum_cfg.is_joint(),
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

    let is_old = quorum_cfg.is_old(&peer.name);
    tasks.push(tokio::spawn(async move {
      (
        is_old,
        timeout(
          Duration::from_secs(1),
          peer_client.send(QuorumRequest::VoteRequest {
            proposed_term,
            candidate_id,
          }),
        )
        .await,
      )
    }));
  }

  let mut yes = 1usize; // self vote
  let mut old_yes = if quorum_cfg.is_old(&this_node.name) {
    1usize
  } else {
    0usize
  };
  let mut max_hw = state.high_watermark;
  let mut highest_seen_term = proposed_term;

  for res in join_all(tasks).await {
    match res {
      Ok((is_old, Ok(Ok(response)))) => match response {
        QuorumResponse::VoteYes {
          term,
          high_watermark,
          ..
        } => {
          highest_seen_term = highest_seen_term.max(term);
          if term == proposed_term {
            yes += 1;
            if is_old {
              old_yes += 1;
            }
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
      Ok((_is_old, Ok(Err(e)))) => {
        debug!("vote request failed: {e}");
      }
      Ok((_is_old, Err(_elapsed))) => {
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

  let has_joint_majority = yes >= joint_majority;
  let has_old_majority = !quorum_cfg.is_joint() || old_yes >= old_majority;

  if has_joint_majority && has_old_majority {
    info!(
      proposed_term,
      yes,
      joint_majority,
      old_yes,
      old_majority,
      joint = quorum_cfg.is_joint(),
      "won quorum election"
	    );
	    service.install_leader_term(proposed_term, max_hw).await?;
	    leader_state.write(LeaderState::Leader {
	      term: proposed_term,
	      lease_valid: false,
	    });

	    // If we just won a joint election and at least one new-node acked, commit the new
	    // config_version and advertise it via discovery. This avoids being stuck in joint mode.
	    let new_yes = yes.saturating_sub(old_yes);
	    if quorum_cfg.is_joint()
	      && max_config_version > local_config_version
	      && new_yes > 0
	    {
	      match service.set_config_version_to(max_config_version).await {
	        Ok(true) => {
	          discovery
	            .set_local_config_version_tag(max_config_version)
	            .await;
	          info!(
	            proposed_term,
	            config_version = max_config_version,
	            "committed config version after joint election"
	          );
	        }
	        Ok(false) => {}
	        Err(e) => {
	          warn!(
	            proposed_term,
	            max_config_version,
	            "failed to commit config version after joint election: {e}"
	          );
	        }
	      }
	    }
	  } else {
	    info!(
      proposed_term,
      yes,
      joint_majority,
      old_yes,
      old_majority,
      joint = quorum_cfg.is_joint(),
      "lost quorum election"
    );
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
    let conf = ServerConfig::new().unwrap();
    for _ in 0..50 {
      let d = random_election_delay(&conf);
      assert!(d >= Duration::from_millis(conf.election_timeout_min_ms));
      assert!(d <= Duration::from_millis(conf.election_timeout_max_ms));
    }
  }
}
