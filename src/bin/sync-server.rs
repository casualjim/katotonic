use std::{
  io::{Read as _, Write as _},
  net::{TcpListener, TcpStream},
  sync::Arc,
};

use clap::Parser as _;
use katotonic::{bully::PeerState, server_tls_config};
use rustls::{ServerConnection, Stream};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
  let console_layer = console_subscriber::spawn();

  let env_filter = tracing_subscriber::EnvFilter::from_default_env();
  let subscriber = tracing_subscriber::fmt::layer()
    .pretty()
    .with_filter(env_filter);

  tracing_subscriber::registry()
    .with(subscriber)
    .with(console_layer)
    .init();

  let conf = katotonic::ServerConfig::parse();
  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, Some(&conf.ca))?);
  let listener = TcpListener::bind("127.0.0.1:9000").expect("Failed to bind to address");

  let pool = threadpool::Builder::new()
    .num_threads(600)
    .thread_name("server-client".to_string())
    .build();

  for stream in listener.incoming() {
    match stream {
      Ok(mut stream) => {
        let config = config.clone();
        pool.execute(move || {
          let mut conn = ServerConnection::new(config).expect("Failed to create connection");

          // Complete the TLS handshake
          loop {
            match conn.complete_io(&mut stream) {
              Ok(_) => break,
              Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
              Err(e) => {
                error!("Error completing I/O: {}", e);
                return;
              }
            }
          }
          let mut tls_stream: Stream<ServerConnection, std::net::TcpStream> =
            Stream::new(&mut conn, &mut stream);

          loop {
            if let Err(e) = handle_client(&mut tls_stream) {
              error!("Error handling client: {}", e);
              break;
            }
          }
        });
      }
      Err(e) => {
        eprintln!("Failed to accept connection: {}", e);
      }
    }
  }
  Ok(())
}

fn handle_client(
  stream: &mut Stream<ServerConnection, TcpStream>,
  leader_state: WatchableValue<PeerState>,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  stream.read_exact(&mut buf)?;
  let response = katotonic::generate_monotonic_id().to_bytes();
  stream.write_all(&response)?;
  Ok(())
}
