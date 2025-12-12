use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{debug, error};

use super::handler::handle_client;
use crate::{
  bully::PeerState, disco::ChitchatDiscovery, quorum::LeaderState, quorum::QuorumService,
  server_tls_config, ServerConfig, WatchableValue,
};

pub async fn run_server(
  conf: ServerConfig,
  discovery: Arc<ChitchatDiscovery>,
  quorum_leader_tracker: WatchableValue<LeaderState>,
  quorum_service: Arc<QuorumService>,
  bully_leader_tracker: WatchableValue<PeerState>,
) -> anyhow::Result<()> {
  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, Some(&conf.ca))?);
  let acceptor = TlsAcceptor::from(config);

  let conf_arc = Arc::new(conf.clone());
  let listener = TcpListener::bind(conf.addr()?)
    .await
    .expect("Failed to bind to address");

  loop {
    let (stream, peer_addr) = listener.accept().await?;
    let acceptor = acceptor.clone();
    let discovery = Arc::clone(&discovery);
    let quorum_leader_tracker = quorum_leader_tracker.clone();
    let quorum_service = Arc::clone(&quorum_service);
    let bully_leader_tracker = bully_leader_tracker.clone();
    let conf_arc = Arc::clone(&conf_arc);

    tokio::spawn(async move {
      match acceptor.accept(stream).await {
        Ok(tls_stream) => {
          let mut tls_stream = tls_stream.compat();
          loop {
            if let Err(e) = handle_client(
              &mut tls_stream,
              discovery.clone(),
              quorum_leader_tracker.clone(),
              quorum_service.clone(),
              bully_leader_tracker.clone(),
              conf_arc.clone(),
            )
            .await
            {
              match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                  debug!("Client from {} disconnected", peer_addr);
                  break;
                }
                _ => {
                  error!("Error handling client from {}: {}", peer_addr, e);
                  break;
                }
              }
            }
          }
        }
        Err(e) => {
          error!("TLS accept error: {}", e);
        }
      }
    });
  }
}
