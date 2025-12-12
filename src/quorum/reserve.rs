use std::{borrow::Cow, sync::Arc, time::Duration};

use futures::future::join_all;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use super::{
  config::JointQuorum, election, rpc::QuorumPeer, QuorumRequest, QuorumResponse, QuorumService,
};
use crate::{disco::Discovery, Result, ServerConfig};

const RESERVE_RPC_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RESERVE_ATTEMPTS: usize = 3;

pub async fn reserve_next_block(
  conf: &ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  service: &QuorumService,
  term: u64,
  block_size: u64,
) -> Result<(u64, u64)> {
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

  let client_config = election::cluster_client_config(conf)?;

  let mut base_hw = state.high_watermark;
  for attempt in 1..=MAX_RESERVE_ATTEMPTS {
    let start_seq = base_hw.saturating_add(1);
    let end_seq = start_seq
      .checked_add(block_size.saturating_sub(1))
      .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "block end overflow"))?;

    debug!(
      term,
      attempt,
      start_seq,
      end_seq,
      joint_majority,
      old_majority,
      joint = quorum_cfg.is_joint(),
      peers = peers.len(),
      "reserving block"
    );

    if peers.is_empty() {
      service.advance_high_watermark_to(end_seq).await?;
      return Ok((start_seq, end_seq));
    }

    let mut tasks = Vec::new();
    for peer in peers.iter().cloned() {
      let addr = Cow::Owned(peer.addr.to_string());
      let server_name = election::server_name_for(&peer);
      let peer_client = QuorumPeer::new_tls(addr, client_config.clone(), server_name);

      let is_old = quorum_cfg.is_old(&peer.name);
      tasks.push(tokio::spawn(async move {
        (
          is_old,
          timeout(
            RESERVE_RPC_TIMEOUT,
            peer_client.send(QuorumRequest::ReserveBlock {
              term,
              start_seq,
              end_seq,
            }),
          )
          .await,
        )
      }));
    }

    let mut yes = 1usize; // self-ack
    let mut old_yes = if quorum_cfg.is_old(&this_node.name) {
      1usize
    } else {
      0usize
    };
    let mut max_hw_seen = base_hw;
    let mut highest_term_seen = term;

    for res in join_all(tasks).await {
      match res {
        Ok((is_old, Ok(Ok(response)))) => match response {
          QuorumResponse::ReserveAck {
            term: ack_term,
            high_watermark,
          } => {
            highest_term_seen = highest_term_seen.max(ack_term);
            if ack_term == term {
              yes += 1;
              if is_old {
                old_yes += 1;
              }
              max_hw_seen = max_hw_seen.max(high_watermark);
            }
          }
          QuorumResponse::ReserveNack {
            term: nack_term,
            high_watermark,
          } => {
            highest_term_seen = highest_term_seen.max(nack_term);
            max_hw_seen = max_hw_seen.max(high_watermark);
          }
          other => {
            debug!(?other, "unexpected quorum response during reserve");
          }
        },
        Ok((_is_old, Ok(Err(e)))) => {
          debug!("reserve request failed: {e}");
        }
        Ok((_is_old, Err(_elapsed))) => {
          debug!("reserve request timed out");
        }
        Err(join_err) => {
          debug!("reserve task join error: {join_err}");
        }
      }
    }

    if highest_term_seen > term {
      service.observe_higher_voted_term(highest_term_seen).await?;
      return Err(std::io::Error::other("higher term observed during reserve").into());
    }

    if max_hw_seen > end_seq {
      service.advance_high_watermark_to(max_hw_seen).await?;
      base_hw = max_hw_seen;
      continue;
    }

    let has_joint_majority = yes >= joint_majority;
    let has_old_majority = !quorum_cfg.is_joint() || old_yes >= old_majority;

    if has_joint_majority && has_old_majority {
      let new_hw = service.advance_high_watermark_to(end_seq).await?;
      info!(
        term,
        yes,
        joint_majority,
        old_yes,
        old_majority,
        joint = quorum_cfg.is_joint(),
        start_seq,
        end_seq,
        new_hw,
        "reserved block"
      );
      return Ok((start_seq, end_seq));
    }

    warn!(
      term,
      yes,
      joint_majority,
      old_yes,
      old_majority,
      joint = quorum_cfg.is_joint(),
      start_seq,
      end_seq,
      "reserve failed"
    );
    return Err(
      std::io::Error::new(std::io::ErrorKind::TimedOut, "failed to reserve block").into(),
    );
  }

  Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "reserve retries exhausted").into())
}
