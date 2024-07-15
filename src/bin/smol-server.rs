use std::sync::Arc;

use clap::Parser;
use futures_rustls::TlsAcceptor;
use smol::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer as _};
use ulidd::server_tls_config;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
  let env_filter = tracing_subscriber::EnvFilter::from_default_env();
  let subscriber = tracing_subscriber::fmt::layer()
    .pretty()
    .with_filter(env_filter);

  tracing_subscriber::registry().with(subscriber).init();

  let conf = ulidd::ServerConfig::parse();
  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, Some(&conf.ca))?);

  smolscale::block_on(async move {
    let acceptor = TlsAcceptor::from(config);
    let listener = TcpListener::bind(conf.addr()?)
      .await
      .expect("Failed to bind to address");

    loop {
      let (stream, peer_addr) = listener.accept().await?;
      let acceptor = acceptor.clone();

      smolscale::spawn(async move {
        match acceptor.accept(stream).await {
          Ok(mut tls_stream) => loop {
            if let Err(e) = handle_client(&mut tls_stream).await {
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
          },
          Err(e) => {
            error!("TLS accept error: {}", e);
          }
        }
      })
      .detach();
    }
  })
}

async fn handle_client(
  tls_stream: &mut futures_rustls::server::TlsStream<TcpStream>,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  tls_stream.read_exact(&mut buf).await?;
  let new_id = ulidd::generate_monotonic_id();
  let response = new_id.to_bytes();
  tls_stream.write_all(&response).await?;
  Ok(())
}
