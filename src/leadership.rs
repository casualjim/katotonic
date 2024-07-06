use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::info;

use crate::{disco::Discovery, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Node {
  node_id: usize,
  promised_id: Option<u64>,
  accepted_id: Option<u64>,
  accepted_value: Option<String>,
}

impl Node {
  fn new(node_id: usize) -> Self {
    Self {
      node_id,
      promised_id: None,
      accepted_id: None,
      accepted_value: None,
    }
  }

  async fn prepare(&mut self, proposal_id: u64) -> (bool, Option<u64>, Option<String>) {
    info!(
      "Node {} received prepare request with proposal_id {}",
      self.node_id, proposal_id
    );
    if self.promised_id.map_or(true, |id| proposal_id > id) {
      self.promised_id = Some(proposal_id);
      return (true, self.accepted_id, self.accepted_value.clone());
    }
    (false, self.accepted_id, self.accepted_value.clone())
  }

  async fn accept(&mut self, proposal_id: u64, value: String) -> bool {
    info!(
      "Node {} received accept request with proposal_id {} at promised_id {:?}",
      self.node_id, proposal_id, self.promised_id
    );
    if self.promised_id.map_or(true, |id| proposal_id >= id) {
      self.promised_id = Some(proposal_id);
      self.accepted_id = Some(proposal_id);
      self.accepted_value = Some(value);
      return true;
    }
    false
  }
}

#[derive(Serialize, Deserialize, Debug)]
enum Message {
  Prepare(u64),
  Accept(u64, String),
}

#[derive(Serialize, Deserialize, Debug)]
enum Response {
  Prepare(bool, Option<u64>, Option<String>),
  Accept(bool),
}

struct Proposer {
  proposal_id: u64,
  value: String,
  discovery: Arc<dyn Discovery>,
  tx: mpsc::Sender<(usize, Message, oneshot::Sender<Response>)>,
}

impl Proposer {
  fn new(
    proposal_id: u64,
    value: String,
    discovery: Arc<dyn Discovery>,
    tx: mpsc::Sender<(usize, Message, oneshot::Sender<Response>)>,
  ) -> Self {
    Proposer {
      proposal_id,
      value,
      discovery,
      tx,
    }
  }

  fn quorum_size(&self, num_nodes: usize) -> usize {
    (num_nodes / 2) + 1
  }

  async fn propose(&mut self) -> Result<bool> {
    // Prepare phase
    let mut promises = 0;
    let mut highest_accepted_id = None;
    let mut highest_accepted_value = None;

    for member in self.discovery.members().await? {
      let (resp_tx, resp_rx) = oneshot::channel();
      if self
        .tx
        .send((
          member.id as usize,
          Message::Prepare(self.proposal_id),
          resp_tx,
        ))
        .await
        .is_err()
      {
        return Ok(false);
      }
      let resp = resp_rx.await;
      if let Ok(Response::Prepare(success, accepted_id, accepted_value)) = resp {
        if success {
          promises += 1;
          if let Some(accepted_id) = accepted_id {
            if highest_accepted_id.map_or(true, |id| accepted_id > id) {
              highest_accepted_id = Some(accepted_id);
              highest_accepted_value = accepted_value;
            }
          }
        }
      }
    }

    if promises < self.quorum_size(self.discovery.member_count().await?) {
      return Ok(false);
    }

    if let Some(value) = highest_accepted_value {
      self.value = value;
    }

    // Accept phase
    let mut accepts = 0;
    for member in self.discovery.members().await? {
      let (resp_tx, resp_rx) = oneshot::channel();
      if self
        .tx
        .send((
          member.id as usize,
          Message::Accept(self.proposal_id, self.value.clone()),
          resp_tx,
        ))
        .await
        .is_err()
      {
        return Ok(false);
      }
      if let Ok(Response::Accept(success)) = resp_rx.await {
        if success {
          accepts += 1;
        }
      }
    }

    let quorum_size = self.quorum_size(self.discovery.member_count().await?);
    let consensus_achieved = accepts >= quorum_size;
    Ok(consensus_achieved)
  }
}

async fn node_task<F>(
  mut rx: mpsc::Receiver<(usize, Message, oneshot::Sender<Response>)>,
  state: Arc<Mutex<HashMap<usize, Node>>>,
  should_drop_or_reorder: F,
) where
  F: Fn(usize, &Message) -> bool + Send + 'static,
{
  while let Some((id, msg, resp_tx)) = rx.recv().await {
    if should_drop_or_reorder(id, &msg) {
      info!("Dropping or reordering message for node {}", id);
      continue; // Drop or reorder the message
    }

    let mut nodes = state.lock().await;
    if let Some(node) = nodes.get_mut(&id) {
      match msg {
        Message::Prepare(proposal_id) => {
          let (success, accepted_id, accepted_value) = node.prepare(proposal_id).await;
          let _ = resp_tx.send(Response::Prepare(success, accepted_id, accepted_value));
        }
        Message::Accept(proposal_id, value) => {
          let result = node.accept(proposal_id, value).await;
          let _ = resp_tx.send(Response::Accept(result));
        }
      }
    } else {
      // If node does not exist, respond with failure
      match msg {
        Message::Prepare(_) => {
          let _ = resp_tx.send(Response::Prepare(false, None, None));
        }
        Message::Accept(_, _) => {
          let _ = resp_tx.send(Response::Accept(false));
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use async_trait::async_trait;
  use tokio::task::JoinHandle;

  use super::*;
  use crate::{disco::Member, Result};

  struct MockDiscovery {
    members: Vec<Member>,
  }

  impl MockDiscovery {
    fn from_iter(iter: impl Iterator<Item = Member>) -> Self {
      Self {
        members: iter.collect(),
      }
    }
  }

  #[async_trait]
  impl Discovery for MockDiscovery {
    async fn members(&self) -> Result<Vec<Member>> {
      Ok(self.members.clone())
    }
  }

  fn keep_all_messages(_: usize, _: &Message) -> bool {
    false
  }

  #[tokio::test]
  async fn test_basic_functionality() {
    info!("test_basic_functionality");
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer = Proposer::new(1, "value".to_string(), disco, tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(result.unwrap());
  }

  #[tokio::test]
  async fn test_majority_rule() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for i in 0..2 {
      let node = Node::new(i as usize);
      nodes.lock().await.insert(i as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer = Proposer::new(1, "value".to_string(), disco, tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(!result.unwrap());
  }

  #[tokio::test]
  async fn test_promise_handling() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer1 = Proposer::new(1, "value1".to_string(), disco.clone(), tx.clone());
    let result1 = proposer1.propose().await;
    assert!(result1.is_ok());
    assert!(result1.unwrap());

    let mut proposer2 = Proposer::new(0, "value2".to_string(), disco.clone(), tx.clone());
    let result2 = proposer2.propose().await;
    assert!(result2.is_ok());
    assert!(!result2.unwrap());
  }

  #[tokio::test]
  async fn test_acceptance_handling() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    let disco: Arc<dyn Discovery> =
      Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
        Member {
          id: i,
          addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
        }
      })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer1 = Proposer::new(1, "value1".to_string(), Arc::clone(&disco), tx.clone());
    let result1 = proposer1.propose().await;
    assert!(result1.is_ok());
    assert!(result1.unwrap());

    let mut proposer2 = Proposer::new(2, "value2".to_string(), disco, tx.clone());
    let result2 = proposer2.propose().await;
    assert!(result2.is_ok());
    assert!(result2.unwrap());
  }

  #[tokio::test]
  async fn test_multiple_proposers() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    let disco: Arc<dyn Discovery> =
      Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
        Member {
          id: i,
          addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
        }
      })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let disco = Arc::new(disco);
    let mut proposer1 = Proposer::new(1, "value1".to_string(), Arc::clone(&disco), tx.clone());
    let mut proposer2 = Proposer::new(2, "value2".to_string(), Arc::clone(&disco), tx.clone());

    let handle1: JoinHandle<Result<bool>> = tokio::spawn(async move { proposer1.propose().await });
    let handle2: JoinHandle<Result<bool>> = tokio::spawn(async move { proposer2.propose().await });

    let result1 = handle1.await.unwrap();
    let result2 = handle2.await.unwrap();

    assert!(result1.is_ok());
    assert!(result2.is_ok());
    assert!(result1.unwrap() || result2.unwrap());
  }

  #[tokio::test]
  async fn test_node_failures() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    let disco: Arc<dyn Discovery> =
      Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
        Member {
          id: i,
          addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
        }
      })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    // Simulate node failure by removing node 0 and 1 from the map
    nodes.lock().await.remove(&0);
    nodes.lock().await.remove(&1);

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer = Proposer::new(1, "value".to_string(), disco, tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(result.unwrap());
  }

  #[tokio::test]
  async fn test_message_drops_and_reordering() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, |id, msg| {
      // Drop messages to node 0 and 1
      if id == 0 || id == 1 {
        return true;
      }
      // Reorder messages
      if let Message::Prepare(_) = msg {
        return true; // Simulate dropping all prepare messages
      }
      false
    }));

    let mut proposer = Proposer::new(1, "value".to_string(), disco.clone(), tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(!result.unwrap()); // Should not achieve consensus with dropped messages
  }

  #[tokio::test]
  async fn test_high_concurrency() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(64);

    let disco = Arc::new(MockDiscovery::from_iter((0..10).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, |_, _| false));

    let mut handles = vec![];
    for i in 1..=10 {
      let tx_clone = tx.clone();
      let disco_clone = disco.clone();
      let handle: JoinHandle<Result<bool>> = tokio::spawn(async move {
        let mut proposal = Proposer::new(i, format!("value{}", i), disco_clone, tx_clone);
        proposal.propose().await
      });
      handles.push(handle);
    }

    let mut successes = 0;
    for handle in handles {
      let result = handle.await.unwrap();
      if result.is_ok() && result.unwrap() {
        successes += 1;
      }
    }

    // Expect some proposals to fail due to concurrency
    assert!(successes > 0);

    // Verify final state of each node
    let final_nodes = nodes.lock().await;
    let mut final_accepted_id = None;
    let mut final_accepted_value = None;

    for node in final_nodes.values() {
      if let Some(accepted_id) = node.accepted_id {
        if final_accepted_id.is_none() || accepted_id > final_accepted_id.unwrap() {
          final_accepted_id = Some(accepted_id);
          final_accepted_value = node.accepted_value.clone();
        }
      }
    }

    assert!(final_accepted_id.is_some());
    assert!(final_accepted_value.is_some());
    assert_eq!(final_accepted_value.as_ref().unwrap(), "value10");

    for node in final_nodes.values() {
      if let Some(accepted_id) = node.accepted_id {
        assert_eq!(accepted_id, final_accepted_id.unwrap());
        assert_eq!(node.accepted_value, final_accepted_value);
      } else {
        panic!("Node {} does not have an accepted value", node.node_id);
      }
    }
  }

  #[tokio::test]
  async fn test_network_partitions() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(64);

    let disco = Arc::new(MockDiscovery::from_iter((0..11).into_iter().map(|i| {
      let port = 9000 + i;
      Member {
        id: i,
        addr: format!("127.0.0.1:{}", port).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, |id, _| {
      // Simulate partition by dropping messages for half the nodes
      id < 5
    }));

    let mut handles = vec![];
    for i in 1..=11 {
      let tx_clone = tx.clone();
      let disco_clone = disco.clone();
      let handle: JoinHandle<Result<bool>> = tokio::spawn(async move {
        let mut proposal = Proposer::new(i, format!("value{}", i), disco_clone, tx_clone);
        proposal.propose().await
      });
      handles.push(handle);
    }

    let mut successes = 0;
    for handle in handles {
      let result = handle.await.unwrap();
      if result.is_ok() && result.unwrap() {
        successes += 1;
      }
    }

    // Expect some proposals to fail due to partition
    assert!(successes > 0);

    // Verify final state of each node in the majority partition
    let final_nodes = nodes.lock().await;

    let mut final_accepted_id = None;
    let mut final_accepted_value = None;

    for id in 6..11 {
      if let Some(node) = final_nodes.get(&id) {
        if let Some(accepted_id) = node.accepted_id {
          if final_accepted_id.is_none() || accepted_id > final_accepted_id.unwrap() {
            final_accepted_id = Some(accepted_id);
            final_accepted_value = node.accepted_value.clone();
          }
        }
      }
    }

    assert!(final_accepted_id.is_some());
    assert!(final_accepted_value.is_some());

    for id in 5..10 {
      if let Some(node) = final_nodes.get(&id) {
        if let Some(accepted_id) = node.accepted_id {
          assert_eq!(accepted_id, final_accepted_id.unwrap());
          assert_eq!(
            node.accepted_value.as_ref().unwrap(),
            final_accepted_value.as_ref().unwrap()
          );
        } else {
          panic!("Node {} does not have an accepted value", node.node_id);
        }
      }
    }
  }

  #[tokio::test]
  async fn test_node_failures_and_recovery() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(64);

    let disco = Arc::new(MockDiscovery::from_iter((0..10).into_iter().map(|i| {
      let port = 9000 + i;
      Member {
        id: i,
        addr: format!("127.0.0.1:{}", port).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.lock().await.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, |id, _| {
      // Simulate node failure by dropping messages for certain nodes
      id == 0 || id == 1
    }));

    let mut handles = vec![];
    for i in 1..=10 {
      let tx_clone = tx.clone();
      let disco_clone = disco.clone();
      let handle: JoinHandle<Result<bool>> = tokio::spawn(async move {
        let mut proposal = Proposer::new(i, format!("value{}", i), disco_clone, tx_clone);
        proposal.propose().await
      });
      handles.push(handle);
    }

    let mut successes = 0;
    for handle in handles {
      let result = handle.await.unwrap();
      if result.is_ok() && result.unwrap() {
        successes += 1;
      }
    }

    // Expect some proposals to fail due to node failures
    assert!(successes > 0);

    // Verify final state of each node
    let final_nodes = nodes.lock().await;
    let mut final_accepted_id = None;
    let mut final_accepted_value = None;

    for id in 2..11 {
      if let Some(node) = final_nodes.get(&id) {
        if let Some(accepted_id) = node.accepted_id {
          if final_accepted_id.is_none() || accepted_id > final_accepted_id.unwrap() {
            final_accepted_id = Some(accepted_id);
            final_accepted_value = node.accepted_value.clone();
          }
        }
      }
    }

    assert!(final_accepted_id.is_some());
    assert!(final_accepted_value.is_some());

    for id in 2..11 {
      if let Some(node) = final_nodes.get(&id) {
        if let Some(accepted_id) = node.accepted_id {
          assert_eq!(accepted_id, final_accepted_id.unwrap());
          assert_eq!(
            node.accepted_value.as_ref().unwrap(),
            final_accepted_value.as_ref().unwrap()
          );
        } else {
          panic!("Node {} does not have an accepted value", node.node_id);
        }
      }
    }
  }
}
