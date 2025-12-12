use std::{
  io::{Read as _, Write as _},
  net::{TcpListener, TcpStream},
  sync::Arc,
  time::Duration,
};

use chitchat::ChitchatHandle;
use clap::Parser as _;
use katotonic::{
  cbor,
  disco::ChitchatDiscovery,
  quorum::{LeaderState, QuorumService},
  server::{spawn_discovery, RedirectInfo},
  server_tls_config, WatchableValue,
};
use mimalloc::MiMalloc;
use rustls::{ServerConnection, Stream};
use tokio::runtime::Runtime;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, Layer as _};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
  let conf_arc = Arc::new(conf.clone());
  let quorum_state = katotonic::quorum::QuorumState::load(&conf.state_path)?;
  info!(
    "loaded quorum state term={} voted_term={} high_watermark={} config_version={}",
    quorum_state.current_term,
    quorum_state.voted_term,
    quorum_state.high_watermark,
    quorum_state.config_version
  );
  let addr = conf.addr()?;

  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, Some(&conf.ca))?);

  let (membership, discovery, quorum_leader, quorum_service) =
    run_background_processes(conf.clone())?;
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
        let quorum_leader = quorum_leader.clone();
        let quorum_service = quorum_service.clone();
        let conf_arc = conf_arc.clone();
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
            if let Err(e) = handle_client(
              &mut tls_stream,
              discovery.clone(),
              quorum_leader.clone(),
              quorum_service.clone(),
              conf_arc.clone(),
            ) {
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
  quorum_leader: WatchableValue<LeaderState>,
  quorum_service: Arc<QuorumService>,
  conf: Arc<katotonic::ServerConfig>,
) -> std::io::Result<()> {
  let mut buf = [0; 1];
  stream.read_exact(&mut buf)?;
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
        let issued = discovery.rt.block_on({
          let service = quorum_service.clone();
          let disco = Arc::clone(&discovery.discovery);
          let conf = conf.clone();
          async move { service.next_id(conf.as_ref(), disco, state.term()).await }
        });
        match issued {
          Ok(id) => {
            let response = id.into_bytes();
            let response_type = &[1u8];
            stream.write_all(response_type)?;
            stream.write_all(&response)?;
            break;
          }
          Err(e) => {
            error!("Failed to issue ID: {e}");
            std::thread::sleep(Duration::from_millis(50));
            continue;
          }
        }
      }

      if let LeaderState::Follower { leader, .. } = state {
        if let Some(redirect_info) = discovery.redirect_info(&leader, term) {
          let response_bytes = match cbor::to_vec(&redirect_info) {
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
      }
      std::thread::sleep(Duration::from_millis(100));
      continue;
    },
    n => {
      error!("Unknown message type: {n}");
    }
  }

  Ok(())
}

fn run_background_processes(conf: katotonic::ServerConfig) -> anyhow::Result<BackgroundHandles> {
  let rt = Arc::new(Runtime::new()?);

  let (membership, discovery, quorum_leader, quorum_service) = rt.block_on(async {
    let (membership, discovery) = spawn_discovery(conf.clone()).await?;
    let (quorum_leader, quorum_service) =
      katotonic::quorum::track_leader(conf.clone(), discovery.clone()).await?;
    Ok::<_, anyhow::Error>((membership, discovery, quorum_leader, quorum_service))
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
    quorum_leader,
    quorum_service,
  ))
}

type BackgroundHandles = (
  SyncMembership,
  SyncDiscovery,
  WatchableValue<LeaderState>,
  Arc<QuorumService>,
);

#[derive(Clone)]
struct SyncDiscovery {
  rt: Arc<Runtime>,
  discovery: Arc<ChitchatDiscovery>,
}

impl SyncDiscovery {
  pub fn redirect_info(&self, leader: &str, term: u64) -> Option<RedirectInfo> {
    let leader_name = leader.to_string();
    self.rt.block_on({
      let discovery = Arc::clone(&self.discovery);
      async move {
        let leader_addr = discovery.api_addr_for_latest(&leader_name).await?;
        let followers = discovery.app_addrs_without(&leader_name).await;
        Some(RedirectInfo {
          term,
          leader: leader_addr,
          followers,
        })
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
