use std::{sync::Arc, time::Duration};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::sleep;
use tracing::{error, info, instrument};

use super::RedirectInfo;
use crate::{
  cbor, disco::ChitchatDiscovery, quorum::LeaderState, quorum::QuorumService, ServerConfig,
  WatchableValue,
};

#[instrument(skip(tls_stream, discovery, quorum_leader, quorum_service, conf))]
pub async fn handle_client<IO: AsyncRead + AsyncWrite + Unpin>(
  tls_stream: &mut IO,
  discovery: Arc<ChitchatDiscovery>,
  quorum_leader: WatchableValue<LeaderState>,
  quorum_service: Arc<QuorumService>,
  conf: Arc<ServerConfig>,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  tls_stream.read_exact(&mut buf).await?;

  match buf[0] {
    1 => loop {
      let state = quorum_leader.read();
      let term = state.term();
      if matches!(
        state,
        LeaderState::Leader {
          lease_valid: true,
          ..
        }
      ) {
        match quorum_service
          .next_id(conf.as_ref(), Arc::clone(&discovery), state.term())
          .await
        {
          Ok(new_id) => {
            let response = new_id.into_bytes();
            // message type 1
            let response_type = &[1u8];

            tls_stream.write_all(response_type).await?;
            tls_stream.write_all(&response).await?;
            break;
          }
          Err(e) => {
            error!("Failed to issue ID: {e}");
            sleep(Duration::from_millis(50)).await;
            continue;
          }
        }
      }

      if let LeaderState::Follower { leader, .. } = state
        && let Some(leader_addr) = discovery.api_addr_for_latest(&leader).await {
          let redirect_info = RedirectInfo {
            term,
            leader: leader_addr,
            followers: discovery.app_addrs_without(&leader).await,
          };
          let response_bytes = match cbor::to_vec(&redirect_info) {
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

      // we're not sure who the leader is. Either because we're still bootstrapping
      // or we're inside a leader election cycle.
      sleep(Duration::from_millis(100)).await;
      continue;
    },
    u8::MAX => {
      info!("ping request received");
      // message type 255: ping
      let response = [u8::MAX];
      tls_stream.write_all(&response).await?;
      info!("pong response sent");
    }
    _ => {
      error!("Unknown message type: {}", buf[0]);
    }
  }

  Ok(())
}
