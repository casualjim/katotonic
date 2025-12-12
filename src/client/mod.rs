#[cfg(feature = "smol")]
mod smol;
mod tokio;

use crate::KatotonicId;
use async_trait::async_trait;
#[cfg(feature = "smol")]
pub use smol::Client as SmolClient;
pub use tokio::Client;

#[async_trait]
pub trait AsyncClient {
  async fn next_id(&self) -> crate::Result<KatotonicId>;
}
