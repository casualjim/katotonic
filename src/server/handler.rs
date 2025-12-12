use std::{sync::Arc, time::Duration};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::sleep;
use tracing::{error, info, instrument};

use super::RedirectInfo;
use crate::{
  bully::PeerState, cbor, disco::ChitchatDiscovery, quorum::QuorumService, quorum::LeaderState,
  ServerConfig, WatchableValue,
};

#[instrument(skip(tls_stream, discovery, quorum_leader, quorum_service, bully_leader, conf))]
pub async fn handle_client<IO: AsyncRead + AsyncWrite + Unpin>(
  tls_stream: &mut IO,
  discovery: Arc<ChitchatDiscovery>,
  quorum_leader: WatchableValue<LeaderState>,
  quorum_service: Arc<QuorumService>,
  bully_leader: WatchableValue<PeerState>,
  conf: Arc<ServerConfig>,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  tls_stream.read_exact(&mut buf).await?;

  match buf[0] {
    1 => loop {
      let state = quorum_leader.read();
      if matches!(state, LeaderState::Leader { lease_valid: true, .. }) {
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

      match bully_leader.read() {
        PeerState::Follower(ref name, generation) => {
          if let Some(leader_node) = discovery.api_addr_for(name, generation).await {
            let redirect_info = RedirectInfo {
              leader: leader_node,
              followers: discovery.app_addrs_without(name).await,
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
