use std::sync::Arc;

use async_trait::async_trait;
use ulid::Ulid;

use super::{
  consensus::{self, Consensus},
  disco::Discovery,
};
use crate::Result;

pub async fn run(disco: Arc<dyn Discovery>) -> Result<()> {
  let consensus = consensus::run::<Ulid>(disco.clone()).await?;

  let _ = ElectionService::new(0, disco, consensus);

  Ok(())
}

#[cfg_attr(test, mry::mry)]
#[async_trait]
pub trait LeaderElection: Send + Sync {
  async fn run(&self) -> Result<()>;
  fn leader(&self) -> usize;
}

pub struct ElectionService {
  discovery: Arc<dyn Discovery>,
  consensus: Arc<dyn Consensus<Item = Ulid>>,
  id: u64,
}

impl ElectionService {
  pub fn new(
    id: u64,
    discovery: Arc<dyn Discovery>,
    consensus: Arc<dyn Consensus<Item = Ulid>>,
  ) -> Self {
    Self {
      discovery,
      consensus,
      id,
    }
  }
}
