use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Member {
  pub addr: SocketAddr,
}

#[async_trait]
pub trait Discovery {
  async fn members(&self) -> Result<Vec<Member>>;
}

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
    if self.promised_id.is_none() || proposal_id > self.promised_id.unwrap() {
      self.promised_id = Some(proposal_id);
      return (true, self.accepted_id, self.accepted_value.clone());
    }
    (false, self.accepted_id, self.accepted_value.clone())
  }

  async fn accept(&mut self, proposal_id: u64, value: String) -> bool {
    if self.promised_id.is_none() || proposal_id >= self.promised_id.unwrap() {
      self.promised_id = Some(proposal_id);
      self.accepted_id = Some(proposal_id);
      self.accepted_value = Some(value);
      return true;
    }
    false
  }
}

#[derive(Serialize, Deserialize)]
enum Message {
  Prepare(u64),
  Accept(u64, String),
}

#[derive(Serialize, Deserialize)]
enum Response {
  Prepare((bool, Option<u64>, Option<String>)),
  Accept(bool),
}
struct Proposer {
  proposal_id: u64,
  value: String,
  tx: mpsc::Sender<(usize, Message, oneshot::Sender<Response>)>,
}

impl Proposer {
  fn new(
    proposal_id: u64,
    value: String,
    tx: mpsc::Sender<(usize, Message, oneshot::Sender<Response>)>,
  ) -> Self {
    Proposer {
      proposal_id,
      value,
      tx,
    }
  }

  async fn propose(&mut self) -> Result<bool> {
    // Prepare phase
    let mut promises = 0;
    let mut highest_accepted_id = None;
    let mut highest_accepted_value = None;

    for node_id in 0..5 {
      let (resp_tx, resp_rx) = oneshot::channel();
      self
        .tx
        .send((node_id, Message::Prepare(self.proposal_id), resp_tx))
        .await?;
      if let Response::Prepare((success, accepted_id, accepted_value)) = resp_rx.await? {
        if success {
          promises += 1;
          if let Some(accepted_id) = accepted_id {
            if highest_accepted_id.is_none() || accepted_id > highest_accepted_id.unwrap() {
              highest_accepted_id = Some(accepted_id);
              highest_accepted_value = accepted_value;
            }
          }
        }
      }
    }

    if promises < 3 {
      // Majority check
      return Ok(false);
    }

    // If there was a previously accepted value, use it
    if let Some(value) = highest_accepted_value {
      self.value = value;
    }

    // Accept phase
    let mut accepts = 0;
    for node_id in 0..5 {
      let (resp_tx, resp_rx) = oneshot::channel();
      self
        .tx
        .send((
          node_id,
          Message::Accept(self.proposal_id, self.value.clone()),
          resp_tx,
        ))
        .await?;
      if let Response::Accept(success) = resp_rx.await? {
        if success {
          accepts += 1;
        }
      }
    }

    if accepts < 3 {
      // Majority check
      return Ok(false);
    }

    Ok(true) // Consensus achieved
  }
}

async fn node_task(
  mut rx: mpsc::Receiver<(usize, Message, oneshot::Sender<Response>)>,
  state: Arc<Mutex<HashMap<usize, Node>>>,
) {
  while let Some((id, msg, resp_tx)) = rx.recv().await {
    let mut nodes = state.lock().await;
    let node = nodes.get_mut(&id).unwrap();
    match msg {
      Message::Prepare(proposal_id) => {
        let result = node.prepare(proposal_id).await;
        let _ = resp_tx.send(Response::Prepare(result));
      }
      Message::Accept(proposal_id, value) => {
        let result = node.accept(proposal_id, value).await;
        let _ = resp_tx.send(Response::Accept(result));
      }
    }
  }
}

#[tokio::main]
async fn main() -> Result<()> {
  tracing_subscriber::fmt::init();

  let nodes = Arc::new(Mutex::new(HashMap::new()));
  let (tx, rx) = mpsc::channel(32);

  for i in 0..5 {
    let node = Node::new(i);
    nodes.lock().await.insert(i, node);
  }

  let nodes_clone = Arc::clone(&nodes);
  tokio::spawn(node_task(rx, nodes_clone));

  // Create a proposer and propose a value
  let mut proposer = Proposer::new(1, "value".to_string(), tx.clone());
  if proposer.propose().await? {
    println!("Consensus achieved!");
  } else {
    println!("Consensus failed.");
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use tokio::task::JoinHandle;

  use super::*;

  #[tokio::test]
  async fn test_proposer() {
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = mpsc::channel(32);

    for i in 0..5 {
      let node = Node::new(i);
      nodes.lock().await.insert(i, node);
    }

    let nodes_clone = Arc::clone(&nodes);
    tokio::spawn(node_task(rx, nodes_clone));

    let mut proposer = Proposer::new(1, "value".to_string(), tx.clone());

    let handle: JoinHandle<Result<bool>> = tokio::spawn(async move { proposer.propose().await });

    let result = handle.await.unwrap();
    assert!(result.is_ok());
    assert!(result.unwrap());
  }
}
