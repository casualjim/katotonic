use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{error, info};

use super::handler::handle_client;
use crate::{bully::PeerState, disco::ChitchatDiscovery, server_tls_config, WatchableValue};

pub async fn run_server(
  conf: crate::ServerConfig,
  discovery: Arc<ChitchatDiscovery>,
  leader_tracker: WatchableValue<PeerState>,
) -> anyhow::Result<()> {
  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, Some(&conf.ca))?);
  let acceptor = TlsAcceptor::from(config);

  let listener = TcpListener::bind(conf.addr()?)
    .await
    .expect("Failed to bind to address");

  loop {
    let (stream, peer_addr) = listener.accept().await?;
    let acceptor = acceptor.clone();
    let discovery = Arc::clone(&discovery);
    let leader_tracker = leader_tracker.clone();

    tokio::spawn(async move {
      match acceptor.accept(stream).await {
        Ok(tls_stream) => {
          let mut tls_stream = tls_stream.compat();
          loop {
            if let Err(e) =
              handle_client(&mut tls_stream, discovery.clone(), leader_tracker.clone()).await
            {
              match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                  info!("Client from {} disconnected", peer_addr);
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
