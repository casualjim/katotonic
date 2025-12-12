use std::{
  collections::HashSet,
  env, fs,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use katotonic::quorum::{LeaderState, QuorumRequest, QuorumResponse, QuorumService};
use katotonic::WatchableValue;

fn temp_state_path(label: &str) -> std::path::PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  let mut dir = env::temp_dir();
  dir.push(format!(
    "katotonic-sim-test-{label}-{}-{nanos}",
    std::process::id()
  ));
  fs::create_dir_all(&dir).unwrap();
  dir.join("state.cbor")
}

#[derive(Clone)]
struct SimNode {
  name: String,
  leader_state: WatchableValue<LeaderState>,
  service: Arc<QuorumService>,
  dir: std::path::PathBuf,
}

impl SimNode {
  fn new(name: &str) -> Self {
    let path = temp_state_path(name);
    let dir = path.parent().unwrap().to_path_buf();
    let leader_state = WatchableValue::new(LeaderState::Down);
    let service =
      Arc::new(QuorumService::new(path.to_string_lossy().to_string(), leader_state.clone()).unwrap());
    Self {
      name: name.to_string(),
      leader_state,
      service,
      dir,
    }
  }
}

struct SimNetwork {
  allowed: HashSet<(String, String)>,
}

impl SimNetwork {
  fn full_mesh(nodes: &[SimNode]) -> Self {
    let mut allowed = HashSet::new();
    for from in nodes {
      for to in nodes {
        if from.name != to.name {
          allowed.insert((from.name.clone(), to.name.clone()));
        }
      }
    }
    Self { allowed }
  }

  fn allow_pairs(pairs: &[(&str, &str)]) -> Self {
    let mut allowed = HashSet::new();
    for (from, to) in pairs {
      allowed.insert((from.to_string(), to.to_string()));
    }
    Self { allowed }
  }

  fn allows(&self, from: &str, to: &str) -> bool {
    self.allowed.contains(&(from.to_string(), to.to_string()))
  }
}

struct ElectionOutcome {
  won: bool,
}

fn majority(n: usize) -> usize {
  n / 2 + 1
}

async fn simulate_election_attempt(
  candidate: &SimNode,
  peers: &[SimNode],
  net: &SimNetwork,
  term: u64,
) -> ElectionOutcome {
  let mut yes = 1usize; // self vote
  let mut highest_seen_term = term;
  let mut max_hw = 0u64;
  let total = peers.len() + 1;
  let needed = majority(total);

  for peer in peers {
    if !net.allows(&candidate.name, &peer.name) {
      continue;
    }

    let resp = peer
      .service
      .handle(QuorumRequest::VoteRequest {
        proposed_term: term,
        candidate_id: candidate.name.clone(),
      })
      .await;

    match resp {
      QuorumResponse::VoteYes {
        term: ack_term,
        high_watermark,
        ..
      } => {
        highest_seen_term = highest_seen_term.max(ack_term);
        if ack_term == term {
          yes += 1;
          max_hw = max_hw.max(high_watermark);
        }
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
        panic!("unexpected response: {other:?}");
      }
    }
  }

  if highest_seen_term > term {
    candidate.service.observe_higher_voted_term(highest_seen_term).await.unwrap();
    candidate.leader_state.write(LeaderState::Down);
    return ElectionOutcome {
      won: false,
    };
  }

  if yes >= needed {
    candidate
      .service
      .install_leader_term(term, max_hw)
      .await
      .unwrap();
    candidate.leader_state.write(LeaderState::Leader {
      term,
      lease_valid: false,
    });
    ElectionOutcome {
      won: true,
    }
  } else {
    ElectionOutcome {
      won: false,
    }
  }
}

#[tokio::test]
async fn sim_concurrent_elections_yield_single_leader() {
  let n1 = SimNode::new("n1");
  let n2 = SimNode::new("n2");
  let n3 = SimNode::new("n3");
  let nodes = vec![n1.clone(), n2.clone(), n3.clone()];
  let net = SimNetwork::full_mesh(&nodes);

  let term1 = n1.service.bump_term_self_vote().await.unwrap();
  n1.leader_state.write(LeaderState::Candidate { term: term1 });
  let term2 = n2.service.bump_term_self_vote().await.unwrap();
  n2.leader_state.write(LeaderState::Candidate { term: term2 });

  assert_eq!(term1, term2);

  let out1 = simulate_election_attempt(&n1, &[n2.clone(), n3.clone()], &net, term1).await;
  let out2 = simulate_election_attempt(&n2, &[n1.clone(), n3.clone()], &net, term2).await;

  assert!(out1.won ^ out2.won, "exactly one candidate should win");
  let winner_term = if out1.won { term1 } else { term2 };

  let st1 = n1.service.get_state().await;
  let st2 = n2.service.get_state().await;
  let st3 = n3.service.get_state().await;
  assert_eq!(winner_term, st1.current_term.max(st1.voted_term).max(st2.current_term).max(st2.voted_term));
  assert!(st3.voted_term >= winner_term);
}

#[tokio::test]
async fn sim_partition_allows_majority_election_only() {
  let n1 = SimNode::new("n1");
  let n2 = SimNode::new("n2");
  let n3 = SimNode::new("n3");

  let net = SimNetwork::allow_pairs(&[("n1", "n2"), ("n2", "n1")]);

  let term1 = n1.service.bump_term_self_vote().await.unwrap();
  n1.leader_state.write(LeaderState::Candidate { term: term1 });
  let out1 = simulate_election_attempt(&n1, &[n2.clone(), n3.clone()], &net, term1).await;
  assert!(out1.won, "majority side should elect a leader");

  let term3 = n3.service.bump_term_self_vote().await.unwrap();
  n3.leader_state.write(LeaderState::Candidate { term: term3 });
  let out3 = simulate_election_attempt(&n3, &[n1.clone(), n2.clone()], &net, term3).await;
  assert!(!out3.won, "minority side must not elect leader");
}

#[tokio::test]
async fn sim_crash_mid_election_allows_same_term_win_but_blocks_second_leader() {
  let n1 = SimNode::new("n1");
  let n2 = SimNode::new("n2");
  let n3 = SimNode::new("n3");
  let nodes = vec![n1.clone(), n2.clone(), n3.clone()];
  let net = SimNetwork::full_mesh(&nodes);

  let term1 = n1.service.bump_term_self_vote().await.unwrap();
  n1.leader_state.write(LeaderState::Candidate { term: term1 });
  // n1 "crashes" before sending votes; no attempt.

  let term2a = n2.service.bump_term_self_vote().await.unwrap();
  n2.leader_state.write(LeaderState::Candidate { term: term2a });
  let out2a = simulate_election_attempt(&n2, &[n1.clone(), n3.clone()], &net, term2a).await;
  assert!(out2a.won, "majority should be able to elect despite one self-vote crash");

  let out1a = simulate_election_attempt(&n1, &[n2.clone(), n3.clone()], &net, term1).await;
  assert!(!out1a.won, "a second leader cannot win in the same term");

  let term1b = n1.service.bump_term_self_vote().await.unwrap();
  n1.leader_state.write(LeaderState::Candidate { term: term1b });
  let out1b = simulate_election_attempt(&n1, &[n2.clone(), n3.clone()], &net, term1b).await;
  assert!(out1b.won, "higher-term election should succeed after crash");

  let st = n1.service.get_state().await;
  assert_eq!(st.current_term, term1b);
}

impl Drop for SimNode {
  fn drop(&mut self) {
    let _ = fs::remove_dir_all(&self.dir);
  }
}
