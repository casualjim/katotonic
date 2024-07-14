use std::sync::Arc;

use chitchat::{spawn_chitchat, transport::UdpTransport};
use clap::Parser;
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer as _};
use ulidd::{bully, disco::ChitchatDiscovery, server_tls_config};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let console_layer = console_subscriber::spawn();

  let env_filter = tracing_subscriber::EnvFilter::from_default_env();
  let subscriber = tracing_subscriber::fmt::layer()
    .pretty()
    .with_test_writer()
    .with_filter(env_filter);

  tracing_subscriber::registry()
    .with(subscriber)
    .with(console_layer)
    .init();

  let conf = ulidd::ServerConfig::parse();

  let chitchat_config: chitchat::ChitchatConfig = (&conf).into();
  info!(
    "chitchat config cluster_id={} chitchat_id={:?} seed_nodes={:?}",
    chitchat_config.cluster_id, chitchat_config.chitchat_id, chitchat_config.seed_nodes
  );

  let tags: Vec<(String, String)> = vec![
    ("role".to_string(), "server".to_string()),
    (
      "cluster_addr".to_string(),
      conf
        .cluster_addr
        .map(|v| v.to_string())
        .unwrap_or("".to_string()),
    ),
    ("api_addr".to_string(), conf.addr.clone()),
    ("server_name".to_string(), "localhost".to_string()),
  ];

  let handle = spawn_chitchat(chitchat_config, tags, &UdpTransport).await?;
  let cc = handle.chitchat();

  let discovery = ChitchatDiscovery::new(cc.clone());
  let _leader_tracker = bully::track_leader(conf.clone(), Arc::new(discovery), None).await?;

  run_server(conf).await?;
  Ok(())
}

async fn handle_client(tls_stream: &mut TlsStream<TcpStream>) -> ulidd::Result<()> {
  let mut buf = [0; 1];
  tls_stream.read_exact(&mut buf).await?;
  let new_id = ulidd::generate_monotonic_id();
  let response = new_id.to_bytes();
  tls_stream.write_all(&response).await?;
  Ok(())
}

async fn run_server(conf: ulidd::ServerConfig) -> anyhow::Result<()> {
  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, conf.ca.as_ref())?);
  let acceptor = TlsAcceptor::from(config);

  let listener = TcpListener::bind(conf.addr()?)
    .await
    .expect("Failed to bind to address");

  loop {
    let (stream, peer_addr) = listener.accept().await?;
    let acceptor = acceptor.clone();

    tokio::spawn(async move {
      match acceptor.accept(stream).await {
        Ok(mut tls_stream) => loop {
          if let Err(e) = handle_client(&mut tls_stream).await {
            match e {
              ulidd::Error::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("Client from {} disconnected", peer_addr);
                break;
              }
              _ => {
                error!("Error handling client from {}: {}", peer_addr, e);
                break;
              }
            }
            // error!("Error handling client from {}: {}", peer_addr, e);            break;
          }
        },
        Err(e) => {
          error!("TLS accept error: {}", e);
        }
      }
    });
  }
}
