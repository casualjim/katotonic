pub mod cluster;
pub mod config;
pub mod election;
pub mod lease;
pub mod reserve;
pub mod rpc;
pub mod service;
pub mod state;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LeaderState {
  Down,
  Candidate { term: u64 },
  Leader { term: u64, lease_valid: bool },
  Follower { leader: String, term: u64 },
}

impl LeaderState {
  pub fn is_leader(&self) -> bool {
    matches!(self, LeaderState::Leader { .. })
  }

  pub fn term(&self) -> u64 {
    match self {
      LeaderState::Down => 0,
      LeaderState::Candidate { term }
      | LeaderState::Leader { term, .. }
      | LeaderState::Follower { term, .. } => *term,
    }
  }
}

pub use election::track_leader;
pub use rpc::{QuorumRequest, QuorumResponse};
pub use service::QuorumService;
pub use state::QuorumState;
