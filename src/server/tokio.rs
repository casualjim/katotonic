use std::sync::Arc;

use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  sync::watch::Receiver,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::{error, info};

use crate::{
  bully::PeerState,
  disco::{ApiClientFactory, ChitchatDiscovery, TokioClientFactory},
  generate_monotonic_id, server_tls_config, Error, Result,
};

pub async fn run_server(
  conf: crate::ServerConfig,
  discovery: Arc<ChitchatDiscovery>,
  leader_tracker: Receiver<PeerState>,
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
        Ok(mut tls_stream) => loop {
          let leader = leader_tracker.borrow().clone();
          if let Err(e) = handle_client(&mut tls_stream, discovery.clone(), leader).await {
            match e {
              Error::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("Client from {} disconnected", peer_addr);
                break;
              }
              _ => {
                error!("Error handling client from {}: {}", peer_addr, e);
                break;
              }
            }
          }
        },
        Err(e) => {
          error!("TLS accept error: {}", e);
        }
      }
    });
  }
}

async fn handle_client(
  tls_stream: &mut TlsStream<TcpStream>,
  discovery: Arc<ChitchatDiscovery>,
  leader: PeerState,
) -> Result<()> {
  let mut buf = [0; 1];
  tls_stream.read_exact(&mut buf).await?;

  let new_id = loop {
    match leader {
      PeerState::Leader(_, _) => {
        break generate_monotonic_id();
      }
      PeerState::Follower(name, generation) => {
        let client = discovery
          .api_client(&(name, generation), TokioClientFactory)
          .await?;
        break client.next_id().await?;
      }
      PeerState::Participant(_, _) | PeerState::Down => {
        continue;
      }
    }
  };

  let response = new_id.to_bytes();
  tls_stream.write_all(&response).await?;
  Ok(())
}
