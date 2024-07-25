use std::sync::Arc;

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::instrument;

use crate::{bully::PeerState, disco::ChitchatDiscovery, generate_monotonic_id};

#[instrument(skip(tls_stream, discovery, leader))]
pub async fn handle_client<IO: AsyncRead + AsyncWrite + Unpin>(
  tls_stream: &mut IO,
  discovery: Arc<ChitchatDiscovery>,
  leader: PeerState,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  tls_stream.read_exact(&mut buf).await?;

  loop {
    match leader {
      PeerState::Leader(_, _) => {
        let new_id = generate_monotonic_id();
        let response = new_id.to_bytes();
        // message type 1
        let response_type = &[1u8];

        tls_stream.write_all(response_type).await?;
        tls_stream.write_all(&response).await?;
        break;
      }
      PeerState::Follower(ref name, generation) => {
        if let Some(leader_node) = discovery.api_addr_for(name, generation).await {
          let response = leader_node.into_bytes();
          // message type 2, response length
          let response_len = &[2, response.len() as u8];

          tls_stream.write_all(response_len).await?;
          tls_stream.write_all(&response).await?;
          break;
        }
        continue;
      }
      _ => {
        // we're not sure who the leader is. Either because we're still bootstrapping
        // or we're inside a leader election cycle.
        continue;
      }
    }
  }

  Ok(())
}
