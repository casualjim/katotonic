use std::{
  io::{Read as _, Write as _},
  net::{TcpListener, TcpStream},
  sync::Arc,
};

use chitchat::ChitchatHandle;
use clap::Parser as _;
use katotonic::{
  bully::{self, PeerState},
  disco::ChitchatDiscovery,
  server::{spawn_discovery, RedirectInfo},
  server_tls_config, WatchableValue,
};
use rustls::{ServerConnection, Stream};
#[cfg(not(target_env = "msvc"))] use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;
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
  let addr = conf.addr()?;

  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, Some(&conf.ca))?);

  let (membership, discovery, leader_tracker) = run_background_processes(conf)?;
  let listener = TcpListener::bind(addr).expect("Failed to bind to address");

  let pool = threadpool::Builder::new()
    .num_threads(600)
    .thread_name("server-client".to_string())
    .build();

  for stream in listener.incoming() {
    match stream {
      Ok(mut stream) => {
        let config = config.clone();
        let discovery = discovery.clone();
        let leader_tracker = leader_tracker.clone();
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
            if let Err(e) =
              handle_client(&mut tls_stream, discovery.clone(), leader_tracker.clone())
            {
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

  membership.shutdown()?;
  Ok(())
}

fn handle_client(
  stream: &mut Stream<ServerConnection, TcpStream>,
  discovery: SyncDiscovery,
  leader_state: WatchableValue<PeerState>,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  stream.read_exact(&mut buf)?;
  match buf[0] {
    1 => loop {
      let leader_state = leader_state.clone();
      let discovery = discovery.clone();
      match leader_state.read() {
        PeerState::Leader(_, _) => {
          let response = katotonic::generate_monotonic_id().to_bytes();
          let response_type = &[1u8];
          stream.write_all(response_type)?;
          stream.write_all(&response)?;
          break;
        }
        PeerState::Follower(ref name, generation) => {
          if let Some(redirect_info) = discovery.redirect_info(name, generation) {
            let response_bytes = match serde_cbor::to_vec(&redirect_info) {
              Ok(bytes) => bytes,
              Err(e) => {
                error!("Failed to serialize response: {:?}", e);
                break;
              }
            };

            let response_len = response_bytes.len() as u32;

            stream.write_all(&[2])?;
            stream.write_all(&response_len.to_be_bytes())?;
            stream.write_all(&response_bytes)?;
            break;
          }
          continue;
        }
        _ => {
          continue;
        }
      }
    },
    n => {
      error!("Unknown message type: {n}");
    }
  }

  Ok(())
}

fn run_background_processes(
  conf: katotonic::ServerConfig,
) -> anyhow::Result<(SyncMembership, SyncDiscovery, WatchableValue<PeerState>)> {
  let rt = Arc::new(Runtime::new()?);

  let (membership, discovery, leader_tracker) = rt.block_on(async {
    let (membership, discovery) = spawn_discovery(conf.clone()).await?;
    let leader_tracker = bully::track_leader(conf.clone(), discovery.clone(), None).await?;
    Ok::<_, anyhow::Error>((membership, discovery, leader_tracker))
  })?;

  Ok((
    SyncMembership {
      rt: Arc::clone(&rt),
      membership,
    },
    SyncDiscovery {
      rt: Arc::clone(&rt),
      discovery,
    },
    leader_tracker,
  ))
}

#[derive(Clone)]
struct SyncDiscovery {
  rt: Arc<Runtime>,
  discovery: Arc<ChitchatDiscovery>,
}

impl SyncDiscovery {
  pub fn redirect_info(&self, name: &str, generation: u64) -> Option<RedirectInfo> {
    self.rt.block_on({
      let discovery = Arc::clone(&self.discovery);
      async move {
        let leader_node = discovery.api_addr_for(name, generation).await;
        if let Some(ln_name) = leader_node {
          let followers = discovery.app_addrs_without(&ln_name).await;
          Some(RedirectInfo {
            leader: ln_name,
            followers,
          })
        } else {
          None
        }
      }
    })
  }
}

struct SyncMembership {
  rt: Arc<Runtime>,
  membership: ChitchatHandle,
}

impl SyncMembership {
  pub fn shutdown(self) -> anyhow::Result<()> {
    self.rt.block_on(self.membership.shutdown())
  }
}
