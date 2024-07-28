use std::{sync::Arc, time::Duration};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::sleep;
use tracing::{error, instrument};

use super::RedirectInfo;
use crate::{bully::PeerState, disco::ChitchatDiscovery, generate_monotonic_id, WatchableValue};

#[instrument(skip(tls_stream, discovery, leader))]
pub async fn handle_client<IO: AsyncRead + AsyncWrite + Unpin>(
  tls_stream: &mut IO,
  discovery: Arc<ChitchatDiscovery>,
  leader: WatchableValue<PeerState>,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  tls_stream.read_exact(&mut buf).await?;

  match buf[0] {
    1 => loop {
      match leader.read() {
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
            let redirect_info = RedirectInfo {
              leader: leader_node,
              followers: discovery.app_addrs_without(name).await,
            };
            let response_bytes = match serde_cbor::to_vec(&redirect_info) {
              Ok(bytes) => bytes,
              Err(e) => {
                error!("Failed to serialize response: {:?}", e);
                continue;
              }
            };

            let response_len = response_bytes.len() as u32;

            // message type 2
            tls_stream.write_all(&[2]).await?;
            // response length
            tls_stream.write_all(&response_len.to_be_bytes()).await?;
            // response
            tls_stream.write_all(&response_bytes).await?;
            break;
          }
          sleep(Duration::from_millis(100)).await;
          continue;
        }
        _ => {
          // we're not sure who the leader is. Either because we're still bootstrapping
          // or we're inside a leader election cycle.
          sleep(Duration::from_millis(100)).await;
          continue;
        }
      }
    },
    _ => {
      error!("Unknown message type: {}", buf[0]);
    }
  }

  Ok(())
}
