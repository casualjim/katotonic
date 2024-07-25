use std::sync::Arc;

use futures_rustls::TlsAcceptor;
use smol::net::TcpListener;
use tokio::sync::watch::Receiver;
use tracing::{error, info};

use super::handler::handle_client;
use crate::{bully::PeerState, disco::ChitchatDiscovery, server_tls_config, Result};

pub async fn run_server(
  conf: crate::ServerConfig,
  discovery: Arc<ChitchatDiscovery>,
  leader_tracker: Receiver<PeerState>,
) -> Result<()> {
  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, Some(&conf.ca))?);

  let acceptor = TlsAcceptor::from(config);
  let listener = TcpListener::bind(conf.addr()?)
    .await
    .expect("Failed to bind to address");

  loop {
    let leader_tracker = leader_tracker.clone();
    let discovery = Arc::clone(&discovery);
    let (stream, peer_addr) = listener.accept().await?;
    let acceptor = acceptor.clone();

    smolscale::spawn(async move {
      match acceptor.accept(stream).await {
        Ok(mut tls_stream) => {
          let leader_tracker = leader_tracker.clone();
          loop {
            let leader = leader_tracker.borrow().clone();
            if let Err(e) = handle_client(&mut tls_stream, discovery.clone(), leader).await {
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
    })
    .detach();
  }
}
