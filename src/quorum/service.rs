use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::error;

use super::lease::FollowerLeaseState;
use super::{LeaderState, QuorumRequest, QuorumResponse, QuorumState};
use crate::{
  disco::ChitchatDiscovery,
  idgen::BlockIdGen,
  id::KatotonicId,
  Result, ServerConfig, WatchableValue,
};

pub struct QuorumService {
  state: Arc<Mutex<QuorumState>>,
  follower_lease: Arc<Mutex<FollowerLeaseState>>,
  idgen: Arc<BlockIdGen>,
  reserve_lock: Mutex<()>,
  leader_state: WatchableValue<LeaderState>,
  state_path: String,
}

impl QuorumService {
  pub fn new(state_path: String, leader_state: WatchableValue<LeaderState>) -> Result<Self> {
    let state = QuorumState::load(&state_path)?;
    Ok(Self {
      state: Arc::new(Mutex::new(state)),
      follower_lease: Arc::new(Mutex::new(FollowerLeaseState::default())),
      idgen: Arc::new(BlockIdGen::new()),
      reserve_lock: Mutex::new(()),
      leader_state,
      state_path,
    })
  }

  pub async fn next_id(
    &self,
    conf: &ServerConfig,
    discovery: Arc<ChitchatDiscovery>,
    term: u64,
  ) -> Result<KatotonicId> {
    loop {
      if let Some(id) = self.idgen.try_next_id() {
        return Ok(id);
      }

      let _guard = self.reserve_lock.lock().await;
      if let Some(id) = self.idgen.try_next_id() {
        return Ok(id);
      }

      let block_size = conf.block_size.max(1);
      let (start_seq, end_seq) = super::reserve::reserve_next_block(
        conf,
        Arc::clone(&discovery),
        self,
        term,
        block_size,
      )
      .await?;
      self.idgen.install_block(term, start_seq, end_seq);
    }
  }

  pub async fn handle(&self, request: QuorumRequest) -> QuorumResponse {
    match request {
      QuorumRequest::Ping => QuorumResponse::Pong,
      QuorumRequest::CatchupRequest => {
        let guard = self.state.lock().await;
        QuorumResponse::CatchupResponse {
          term: guard.current_term.max(guard.voted_term),
          high_watermark: guard.high_watermark,
          config_version: guard.config_version,
        }
      }
      QuorumRequest::VoteRequest {
        proposed_term,
        candidate_id,
      } => {
        let mut guard = self.state.lock().await;
        if proposed_term <= guard.voted_term {
          return QuorumResponse::VoteNo {
            term: proposed_term,
            voted_term: guard.voted_term,
            high_watermark: guard.high_watermark,
            config_version: guard.config_version,
          };
        }

        if let Err(e) = guard.set_voted_term(proposed_term) {
          error!("failed to set voted term: {e}");
          return QuorumResponse::Error {
            code: 400,
            message: e.to_string(),
          };
        }

        if let Err(e) = guard.persist(&self.state_path) {
          error!("failed to persist quorum state: {e}");
          return QuorumResponse::Error {
            code: 500,
            message: e.to_string(),
          };
        }

        self.leader_state.write(LeaderState::Follower {
          leader: candidate_id,
          term: proposed_term,
        });

        QuorumResponse::VoteYes {
          term: proposed_term,
          high_watermark: guard.high_watermark,
          config_version: guard.config_version,
        }
      }
      QuorumRequest::LeaseRenew {
        term,
        leader_id,
        lease_until_ms: _,
      } => {
        let mut guard = self.state.lock().await;
        if term < guard.voted_term {
          return QuorumResponse::LeaseNack {
            term,
            voted_term: guard.voted_term,
          };
        }

        let mut changed = false;
        if term > guard.current_term {
          if let Err(e) = guard.set_current_term(term) {
            error!("failed to set current term from lease renew: {e}");
            return QuorumResponse::Error {
              code: 400,
              message: e.to_string(),
            };
          }
          changed = true;
        }
        if term > guard.voted_term {
          if let Err(e) = guard.set_voted_term(term) {
            error!("failed to set voted term from lease renew: {e}");
            return QuorumResponse::Error {
              code: 400,
              message: e.to_string(),
            };
          }
          changed = true;
        }

        if changed {
          if let Err(e) = guard.persist(&self.state_path) {
            error!("failed to persist quorum state on lease renew: {e}");
            return QuorumResponse::Error {
              code: 500,
              message: e.to_string(),
            };
          }
        }

        let voted_term = guard.voted_term;
        let high_watermark = guard.high_watermark;
        drop(guard);

        let mut lease_guard = self.follower_lease.lock().await;
        if !lease_guard.allows(term, &leader_id) {
          return QuorumResponse::LeaseNack { term, voted_term };
        }
        lease_guard.record(term, leader_id.clone());
        drop(lease_guard);

        self.leader_state.write(LeaderState::Follower {
          leader: leader_id,
          term,
        });

        QuorumResponse::LeaseAck {
          term,
          high_watermark,
        }
      }
      QuorumRequest::ReserveBlock {
        term,
        start_seq: _,
        end_seq,
      } => {
        let mut guard = self.state.lock().await;
        if term < guard.voted_term {
          return QuorumResponse::ReserveNack {
            term: guard.voted_term,
            high_watermark: guard.high_watermark,
          };
        }

        let mut changed = false;
        if term > guard.current_term {
          if let Err(e) = guard.set_current_term(term) {
            error!("failed to set current term from reserve: {e}");
            return QuorumResponse::Error {
              code: 400,
              message: e.to_string(),
            };
          }
          changed = true;
        }
        if term > guard.voted_term {
          if let Err(e) = guard.set_voted_term(term) {
            error!("failed to set voted term from reserve: {e}");
            return QuorumResponse::Error {
              code: 400,
              message: e.to_string(),
            };
          }
          changed = true;
        }

        match guard.advance_high_watermark(end_seq) {
          Ok(true) => changed = true,
          Ok(false) => {}
          Err(e) => {
            error!("failed to advance high watermark from reserve: {e}");
            return QuorumResponse::Error {
              code: 400,
              message: e.to_string(),
            };
          }
        }

        if changed {
          if let Err(e) = guard.persist(&self.state_path) {
            error!("failed to persist quorum state on reserve: {e}");
            return QuorumResponse::Error {
              code: 500,
              message: e.to_string(),
            };
          }
        }

        QuorumResponse::ReserveAck {
          term,
          high_watermark: guard.high_watermark,
        }
      }
    }
  }

  pub async fn get_state(&self) -> QuorumState {
    self.state.lock().await.clone()
  }

  pub async fn advance_high_watermark_to(&self, new_hw: u64) -> Result<u64> {
    let mut guard = self.state.lock().await;
    let changed = guard.advance_high_watermark(new_hw)?;
    if changed {
      guard.persist(&self.state_path)?;
    }
    Ok(guard.high_watermark)
  }

  pub async fn bump_term_self_vote(&self) -> Result<u64> {
    let mut guard = self.state.lock().await;
    let base = guard.current_term.max(guard.voted_term);
    let proposed_term = base.saturating_add(1);
    guard.set_voted_term(proposed_term)?;
    guard.persist(&self.state_path)?;
    Ok(proposed_term)
  }

  pub async fn install_leader_term(&self, term: u64, high_watermark: u64) -> Result<()> {
    let mut guard = self.state.lock().await;
    guard.set_current_term(term)?;
    guard.advance_high_watermark(high_watermark)?;
    guard.persist(&self.state_path)?;
    Ok(())
  }

  pub async fn install_catchup(
    &self,
    term: u64,
    high_watermark: u64,
    config_version: u64,
  ) -> Result<()> {
    let mut guard = self.state.lock().await;
    let mut changed = false;

    if term > guard.current_term {
      guard.set_current_term(term)?;
      changed = true;
    }
    if term > guard.voted_term {
      guard.set_voted_term(term)?;
      changed = true;
    }
    if high_watermark > guard.high_watermark {
      guard.advance_high_watermark(high_watermark)?;
      changed = true;
    }
    if config_version > guard.config_version {
      guard.set_config_version(config_version)?;
      changed = true;
    }

    if changed {
      guard.persist(&self.state_path)?;
    }
    Ok(())
  }

  pub async fn set_config_version_to(&self, new_version: u64) -> Result<bool> {
    let mut guard = self.state.lock().await;
    let changed = guard.set_config_version(new_version)?;
    if changed {
      guard.persist(&self.state_path)?;
    }
    Ok(changed)
  }

  pub async fn observe_higher_voted_term(&self, term: u64) -> Result<()> {
    let mut guard = self.state.lock().await;
    if term > guard.voted_term {
      guard.set_voted_term(term)?;
      guard.persist(&self.state_path)?;
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use std::{
    env, fs,
    time::{SystemTime, UNIX_EPOCH},
  };

  use super::*;

  fn temp_state_path(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_nanos();
    let mut dir = env::temp_dir();
    dir.push(format!(
      "katotonic-service-test-{label}-{}-{nanos}",
      std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir.join("state.cbor")
  }

  #[tokio::test]
  async fn vote_request_updates_state_and_leader() {
    let path = temp_state_path("vote");
    let leader_state = WatchableValue::new(LeaderState::Down);
    let service =
      QuorumService::new(path.to_string_lossy().to_string(), leader_state.clone()).unwrap();

    let resp = service
      .handle(QuorumRequest::VoteRequest {
        proposed_term: 1,
        candidate_id: "c1".to_string(),
      })
      .await;

    assert_eq!(
      resp,
      QuorumResponse::VoteYes {
        term: 1,
        high_watermark: 0,
        config_version: 0,
      }
    );
    assert_eq!(
      leader_state.read(),
      LeaderState::Follower {
        leader: "c1".to_string(),
        term: 1
      }
    );

    let resp2 = service
      .handle(QuorumRequest::VoteRequest {
        proposed_term: 1,
        candidate_id: "c2".to_string(),
      })
      .await;
    match resp2 {
      QuorumResponse::VoteNo { voted_term, .. } => assert_eq!(voted_term, 1),
      other => panic!("unexpected response: {other:?}"),
    }

    let reloaded = QuorumState::load(&path).unwrap();
    assert_eq!(reloaded.voted_term, 1);
  }

  #[tokio::test]
  async fn lease_renew_advances_term_and_acks() {
    let path = temp_state_path("lease-ack");
    let leader_state = WatchableValue::new(LeaderState::Down);
    let service =
      QuorumService::new(path.to_string_lossy().to_string(), leader_state.clone()).unwrap();

    let resp = service
      .handle(QuorumRequest::LeaseRenew {
        term: 2,
        leader_id: "l1".to_string(),
        lease_until_ms: 1234,
      })
      .await;

    assert_eq!(
      resp,
      QuorumResponse::LeaseAck {
        term: 2,
        high_watermark: 0
      }
    );
    assert_eq!(
      leader_state.read(),
      LeaderState::Follower {
        leader: "l1".to_string(),
        term: 2
      }
    );

    let st = service.get_state().await;
    assert_eq!(st.current_term, 2);
    assert_eq!(st.voted_term, 2);
  }

  #[tokio::test]
  async fn lease_renew_rejects_lower_than_voted_term() {
    let path = temp_state_path("lease-nack-low");
    let leader_state = WatchableValue::new(LeaderState::Down);
    let service =
      QuorumService::new(path.to_string_lossy().to_string(), leader_state.clone()).unwrap();

    let _ = service
      .handle(QuorumRequest::VoteRequest {
        proposed_term: 3,
        candidate_id: "c1".to_string(),
      })
      .await;

    let resp = service
      .handle(QuorumRequest::LeaseRenew {
        term: 2,
        leader_id: "l1".to_string(),
        lease_until_ms: 1234,
      })
      .await;

    assert_eq!(
      resp,
      QuorumResponse::LeaseNack {
        term: 2,
        voted_term: 3
      }
    );
  }

  #[tokio::test]
  async fn lease_renew_rejects_second_leader_same_term() {
    let path = temp_state_path("lease-nack-dual");
    let leader_state = WatchableValue::new(LeaderState::Down);
    let service =
      QuorumService::new(path.to_string_lossy().to_string(), leader_state.clone()).unwrap();

    let resp1 = service
      .handle(QuorumRequest::LeaseRenew {
        term: 1,
        leader_id: "l1".to_string(),
        lease_until_ms: 1000,
      })
      .await;
    assert!(matches!(resp1, QuorumResponse::LeaseAck { .. }));

    let resp2 = service
      .handle(QuorumRequest::LeaseRenew {
        term: 1,
        leader_id: "l2".to_string(),
        lease_until_ms: 2000,
      })
      .await;

    assert_eq!(
      resp2,
      QuorumResponse::LeaseNack {
        term: 1,
        voted_term: 1
      }
    );
  }

  #[tokio::test]
  async fn reserve_block_advances_high_watermark() {
    let path = temp_state_path("reserve-ack");
    let leader_state = WatchableValue::new(LeaderState::Down);
    let service =
      QuorumService::new(path.to_string_lossy().to_string(), leader_state.clone()).unwrap();

    let resp = service
      .handle(QuorumRequest::ReserveBlock {
        term: 1,
        start_seq: 1,
        end_seq: 10,
      })
      .await;

    assert_eq!(
      resp,
      QuorumResponse::ReserveAck {
        term: 1,
        high_watermark: 10
      }
    );

    let st = service.get_state().await;
    assert_eq!(st.high_watermark, 10);
  }
}
