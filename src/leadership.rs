use std::sync::Arc;

use chitchat::{Chitchat, ChitchatConfig, NodeState};
use tokio::{
  sync::Mutex,
  time::{sleep, Duration},
};

// Node metadata for leader election
#[derive(Clone)]
struct NodeMetadata {
  node_id: String,
  is_leader: bool,
  heartbeat: u64,
}

// Leader election logic
async fn leader_election(chitchat: Arc<Mutex<Chitchat>>, node_metadata: Arc<NodeMetadata>) {
  let mut cc = chitchat.lock().await;
  let state = cc.self_node_state();
  state.set("leader", "this is the leader");
}

async fn start_election(chitchat: &mut Chitchat, node_metadata: &mut NodeMetadata) {}
