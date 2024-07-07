use std::{collections::HashSet, fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::StreamExt as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
  sync::{mpsc, oneshot, watch, Mutex},
  time::sleep,
};
use tracing::info;
use ulid::{Generator, Ulid};

use super::disco::Discovery;
use crate::{Error, Result};

#[async_trait]
pub trait Consensus: Send + Sync {
  type Item: Send + Sync;
  async fn propose(&self, value: Self::Item) -> Result<()>;
  fn subscribe(&self) -> watch::Receiver<Self::Item>;
}

#[derive(Clone)]
struct Client<T>
where
  T: Serialize + DeserializeOwned + Send + Sync,
{
  generator: Arc<Mutex<Generator>>,
  tx: mpsc::Sender<(usize, Message<T>, oneshot::Sender<Response<T>>)>,
  discovery: Arc<dyn Discovery + 'static>,
  leader: watch::Sender<T>,
}

#[async_trait]
impl<T> Consensus for Client<T>
where
  T: Serialize + DeserializeOwned + Clone + Send + Sync,
{
  type Item = T;

  fn subscribe(&self) -> watch::Receiver<Self::Item> {
    self.leader.subscribe()
  }

  async fn propose(&self, value: Self::Item) -> Result<()> {
    loop {
      let new_id = self.generator.lock().await.generate();
      match new_id {
        Ok(proposal_id) => {
          let mut proposer = Proposer::new(
            proposal_id,
            value,
            Arc::clone(&self.discovery),
            self.tx.clone(),
          );
          let result = proposer.propose().await?;
          if result {
            return Ok(());
          }
          return Err(Error::ConsensusNotAchieved);
        }
        Err(_) => {
          sleep(Duration::from_millis(1)).await;
        }
      }
    }
  }
}

pub(super) async fn run<
  T: Serialize + Send + Sync + DeserializeOwned + Clone + Default + 'static,
>(
  discovery: Arc<dyn Discovery>,
) -> Result<Arc<dyn Consensus<Item = T>>> {
  let membership = Arc::clone(&discovery);

  let (tx, rx) = mpsc::channel(32);
  let nodes = Arc::new(DashMap::<usize, Node<T>>::new());

  let nodes_clone = Arc::clone(&nodes);
  tokio::spawn(async move {
    let mut watcher = membership.membership_changes().await;
    while let Some(change) = watcher.next().await {
      if let Ok(change) = change {
        info!("Membership change: {:?}", change);
        let old_keys = nodes_clone
          .iter()
          .map(|kv| *kv.key())
          .collect::<HashSet<_>>();
        let mut seen_ids = HashSet::new();
        for member in &change {
          let node = Node::new(member.id as usize);
          nodes_clone.insert(member.id as usize, node);
          seen_ids.insert(member.id as usize);
        }
        old_keys.difference(&seen_ids).for_each(|v| {
          nodes_clone.remove(v);
        });
      }
    }
  });

  tokio::spawn(node_task(rx, nodes, |_, _| false));

  let generator = Arc::new(Mutex::const_new(Generator::new()));

  Ok(Arc::new(Client {
    tx,
    discovery,
    generator,
    leader: watch::channel(T::default()).0,
  }))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Node<T> {
  node_id: usize,
  promised_id: Option<Ulid>,
  accepted_id: Option<Ulid>,
  accepted_value: Option<T>,
}

impl<T> Node<T>
where
  T: Clone,
{
  fn new(node_id: usize) -> Self {
    Self {
      node_id,
      promised_id: None,
      accepted_id: None,
      accepted_value: None,
    }
  }

  async fn prepare(&mut self, proposal_id: Ulid) -> (bool, Option<Ulid>, Option<T>) {
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

  async fn accept(&mut self, proposal_id: Ulid, value: T) -> bool {
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
enum Message<T: Serialize> {
  Prepare(Ulid),
  Accept(Ulid, T),
}

#[derive(Serialize, Deserialize, Debug)]
enum Response<T> {
  Prepare(bool, Option<Ulid>, Option<T>),
  Accept(bool),
}

struct Proposer<T>
where
  T: Serialize,
{
  proposal_id: Ulid,
  value: T,
  discovery: Arc<dyn Discovery>,
  tx: mpsc::Sender<(usize, Message<T>, oneshot::Sender<Response<T>>)>,
}

impl<T> Proposer<T>
where
  T: Serialize + Send + Sync + Clone + DeserializeOwned,
{
  fn new(
    proposal_id: Ulid,
    value: T,
    discovery: Arc<dyn Discovery>,
    tx: mpsc::Sender<(usize, Message<T>, oneshot::Sender<Response<T>>)>,
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

  async fn prepare(&mut self) -> Result<(usize, Option<T>)> {
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
        return Ok((0, None));
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
    Ok((promises, highest_accepted_value))
  }

  async fn propose(&mut self) -> Result<bool> {
    let (promises, highest_accepted_value) = self.prepare().await?;

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

async fn node_task<T, F>(
  mut rx: mpsc::Receiver<(usize, Message<T>, oneshot::Sender<Response<T>>)>,
  state: Arc<DashMap<usize, Node<T>>>,
  should_drop_or_reorder: F,
) where
  T: Serialize + Send + Sync + Clone + DeserializeOwned,
  F: Fn(usize, &Message<T>) -> bool + Send,
{
  while let Some((id, msg, resp_tx)) = rx.recv().await {
    if should_drop_or_reorder(id, &msg) {
      info!("Dropping or reordering message for node {}", id);
      continue; // Drop or reorder the message
    }

    if let Some(mut node) = state.get_mut(&id) {
      match msg {
        Message::Prepare(proposal_id) => {
          let (success, accepted_id, accepted_value) = node.value_mut().prepare(proposal_id).await;
          let _ = resp_tx.send(Response::Prepare(success, accepted_id, accepted_value));
        }
        Message::Accept(proposal_id, value) => {
          let result = node.value_mut().accept(proposal_id, value).await;
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
  use futures::stream::BoxStream;
  use tokio::task::JoinHandle;
  use ulid::Generator;

  use super::*;
  use crate::{cluster::disco::Member, Result};

  #[derive(Debug, Clone)]
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

    async fn membership_changes(&self) -> BoxStream<'static, Result<Vec<Member>>> {
      unimplemented!()
    }

    async fn myself(&self) -> Result<Member> {
      unimplemented!()
    }
  }

  fn keep_all_messages<T: Serialize>(_: usize, _: &Message<T>) -> bool {
    false
  }

  #[tokio::test]
  async fn test_basic_functionality() {
    info!("test_basic_functionality");
    let nodes = Arc::new(DashMap::new());
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer = Proposer::new(Ulid::new(), "value".to_string(), disco, tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(result.unwrap());
  }

  #[tokio::test]
  async fn test_majority_rule() {
    let nodes = Arc::new(DashMap::new());
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for i in 0..2 {
      let node = Node::new(i as usize);
      nodes.insert(i as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer = Proposer::new(Ulid::new(), "value".to_string(), disco, tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(!result.unwrap());
  }

  #[tokio::test]
  async fn test_promise_handling() {
    let nodes = Arc::new(DashMap::new());
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));
    let mut gen = Generator::new();

    let proposal_id2 = gen.generate().unwrap();
    let proposal_id = gen.generate().unwrap();

    let mut proposer1 = Proposer::new(proposal_id, "value1".to_string(), disco.clone(), tx.clone());
    let result1 = proposer1.propose().await;
    assert!(result1.is_ok());
    assert!(result1.unwrap());

    let mut proposer2 = Proposer::new(
      proposal_id2,
      "value2".to_string(),
      disco.clone(),
      tx.clone(),
    );
    let result2 = proposer2.propose().await;
    assert!(result2.is_ok());
    assert!(!result2.unwrap());
  }

  #[tokio::test]
  async fn test_acceptance_handling() {
    let nodes = Arc::new(DashMap::new());
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
      nodes.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut gen = Generator::new();

    let mut proposer1 = Proposer::new(
      gen.generate().unwrap(),
      "value1".to_string(),
      Arc::clone(&disco),
      tx.clone(),
    );
    let result1 = proposer1.propose().await;
    assert!(result1.is_ok());
    assert!(result1.unwrap());

    let mut proposer2 = Proposer::new(
      gen.generate().unwrap(),
      "value2".to_string(),
      disco,
      tx.clone(),
    );
    let result2 = proposer2.propose().await;
    assert!(result2.is_ok());
    assert!(result2.unwrap());
  }

  #[tokio::test]
  async fn test_multiple_proposers() {
    let nodes = Arc::new(DashMap::new());
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
      nodes.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let disco = Arc::new(disco);
    let mut proposer1 = Proposer::new(
      Ulid::new(),
      "value1".to_string(),
      Arc::clone(&disco),
      tx.clone(),
    );
    let mut proposer2 = Proposer::new(
      Ulid::new(),
      "value2".to_string(),
      Arc::clone(&disco),
      tx.clone(),
    );

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
    let nodes = Arc::new(DashMap::new());
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
      nodes.insert(member.id as usize, node);
    }

    // Simulate node failure by removing node 0 and 1 from the map
    nodes.remove(&0);
    nodes.remove(&1);

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, keep_all_messages));

    let mut proposer = Proposer::new(Ulid::new(), "value".to_string(), disco, tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(result.unwrap());
  }

  #[tokio::test]
  async fn test_message_drops_and_reordering() {
    let nodes = Arc::new(DashMap::new());
    let (tx, rx) = mpsc::channel(32);

    let disco = Arc::new(MockDiscovery::from_iter((0..5).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.insert(member.id as usize, node);
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

    let mut proposer = Proposer::new(Ulid::new(), "value".to_string(), disco.clone(), tx.clone());

    let result = proposer.propose().await;
    assert!(result.is_ok());
    assert!(!result.unwrap()); // Should not achieve consensus with dropped messages
  }

  #[tokio::test]
  async fn test_high_concurrency() {
    let nodes = Arc::new(DashMap::new());
    let (tx, rx) = mpsc::channel(64);

    let disco = Arc::new(MockDiscovery::from_iter((0..10).into_iter().map(|i| {
      Member {
        id: i,
        addr: format!("127.0.0.1:900{}", i).parse().unwrap(),
      }
    })));

    for member in disco.members().await.unwrap() {
      let node = Node::new(member.id as usize);
      nodes.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, |_, _| false));

    let mut gen = Generator::new();

    let mut handles = vec![];
    for i in 1..=10 {
      let tx_clone = tx.clone();
      let disco_clone = disco.clone();
      let proposal_id = gen.generate().unwrap();
      let handle: JoinHandle<Result<bool>> = tokio::spawn(async move {
        let mut proposal = Proposer::new(proposal_id, format!("value{}", i), disco_clone, tx_clone);
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
    let final_nodes = nodes;
    let mut final_accepted_id = None;
    let mut final_accepted_value = None;

    for node_ref in final_nodes.iter() {
      let node = node_ref.value();
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

    for node_ref in final_nodes.iter() {
      let node = node_ref.value();
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
    let nodes = Arc::new(DashMap::new());
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
      nodes.insert(member.id as usize, node);
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
        let mut proposal = Proposer::new(Ulid::new(), format!("value{}", i), disco_clone, tx_clone);
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
    let final_nodes = nodes;

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
    let nodes = Arc::new(DashMap::new());
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
      nodes.insert(member.id as usize, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone, |id, _| {
      // Simulate node failure by dropping messages for certain nodes
      id == 0 || id == 1
    }));

    let mut gen = Generator::new();
    let mut handles = vec![];
    for i in 1..=10 {
      let tx_clone = tx.clone();
      let disco_clone = disco.clone();
      let proposal_id = gen.generate().unwrap();
      let handle: JoinHandle<Result<bool>> = tokio::spawn(async move {
        let mut proposal = Proposer::new(proposal_id, format!("value{}", i), disco_clone, tx_clone);
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
    let final_nodes = nodes;
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
