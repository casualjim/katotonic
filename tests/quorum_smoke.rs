use std::{
  env, fs,
  net::{SocketAddr, TcpListener as StdTcpListener},
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use chitchat::{spawn_chitchat, transport::UdpTransport, ChitchatHandle};
use tokio::time::{sleep, timeout};

use katotonic::{
  client::Client,
  client::AsyncClient,
  disco::{ChitchatDiscovery, Discovery},
  quorum::{self, LeaderState, QuorumService, QuorumState},
  server,
  ClientConfig, SeedNode, ServerConfig, WatchableValue,
};

fn free_port() -> u16 {
  let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
  let port = listener.local_addr().unwrap().port();
  drop(listener);
  port
}

fn temp_state_path(label: &str, node_id: &str) -> std::path::PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos();
  let mut dir = env::temp_dir();
  dir.push(format!(
    "katotonic-quorum-smoke-{label}-{node_id}-{}-{nanos}",
    std::process::id()
  ));
  fs::create_dir_all(&dir).unwrap();
  dir.join("state.cbor")
}

async fn spawn_test_discovery(
  conf: ServerConfig,
) -> anyhow::Result<(ChitchatHandle, std::sync::Arc<ChitchatDiscovery>)> {
  let chitchat_config: chitchat::ChitchatConfig = (&conf).into();
  let quorum_state = QuorumState::load(&conf.state_path).unwrap_or_default();

  let tags: Vec<(String, String)> = vec![
    ("role".to_string(), "server".to_string()),
    (
      "cluster_addr".to_string(),
      conf.cluster_addr.clone().unwrap_or_default(),
    ),
    ("api_addr".to_string(), conf.addr.clone()),
    ("api_server_name".to_string(), "localhost".to_string()),
    ("cluster_server_name".to_string(), "localhost".to_string()),
    (
      "config_version".to_string(),
      quorum_state.config_version.to_string(),
    ),
  ];

  let handle = spawn_chitchat(chitchat_config, tags, &UdpTransport).await?;
  let cc = handle.chitchat();
  Ok((handle, std::sync::Arc::new(ChitchatDiscovery::new(cc, conf))))
}

async fn wait_for_api(addr: SocketAddr) {
  timeout(Duration::from_secs(3), async move {
    loop {
      if tokio::net::TcpStream::connect(addr).await.is_ok() {
        return;
      }
      sleep(Duration::from_millis(50)).await;
    }
  })
  .await
  .expect("api bind timeout");
}

async fn wait_for_all_members(
  discoveries: &[std::sync::Arc<ChitchatDiscovery>],
  expected: usize,
) {
  timeout(Duration::from_secs(5), async move {
    loop {
      let mut all_ready = true;
      for d in discoveries {
        match d.members().await {
          Ok(m) if m.len() >= expected => {}
          _ => {
            all_ready = false;
            break;
          }
        }
      }
      if all_ready {
        return;
      }
      sleep(Duration::from_millis(50)).await;
    }
  })
  .await
  .expect("membership converge timeout");
}

async fn wait_for_leader(
  states: &[WatchableValue<LeaderState>],
) -> (usize, u64) {
  timeout(Duration::from_secs(5), async move {
    loop {
      for (i, s) in states.iter().enumerate() {
        if let LeaderState::Leader {
          term,
          lease_valid: true,
        } = s.read()
        {
          return (i, term);
        }
      }
      sleep(Duration::from_millis(50)).await;
    }
  })
  .await
  .expect("leader election timeout")
}

struct Node {
  conf: ServerConfig,
  _chitchat: ChitchatHandle,
  discovery: std::sync::Arc<ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
  _service: std::sync::Arc<QuorumService>,
  state_dir: std::path::PathBuf,
}

async fn spawn_node(
  label: &str,
  node_id: &str,
  api_port: u16,
  cluster_port: u16,
  seeds: Vec<SocketAddr>,
) -> Node {
  let path = temp_state_path(label, node_id);
  let state_dir = path.parent().unwrap().to_path_buf();
  QuorumState::default().persist(&path).unwrap();

  let mut conf = ServerConfig::for_tests().unwrap();
  conf.node_id = Some(node_id.to_string());
  conf.cluster_id = label.to_string();
  conf.addr = format!("127.0.0.1:{api_port}");
  conf.cluster_addr = Some(format!("127.0.0.1:{cluster_port}"));
  conf.seed = seeds
    .into_iter()
    .map(|addr| SeedNode {
      server_name: "localhost".to_string(),
      addr,
    })
    .collect();
  conf.state_path = path.to_string_lossy().to_string();

  conf.block_size = 100;
  conf.gossip_interval = Duration::from_millis(100);

  let (chitchat, discovery) = spawn_test_discovery(conf.clone()).await.unwrap();

  let (leader_state, quorum_service) =
    quorum::election::track_leader(conf.clone(), std::sync::Arc::clone(&discovery))
      .await
      .unwrap();

  let server_conf = conf.clone();
  let server_discovery = std::sync::Arc::clone(&discovery);
  let server_state = leader_state.clone();
  let server_service = std::sync::Arc::clone(&quorum_service);
  tokio::spawn(async move {
    let _ = server::run_server(
      server_conf,
      server_discovery,
      server_state,
      server_service,
    )
    .await;
  });

  wait_for_api(conf.addr().unwrap()).await;

  Node {
    conf,
    _chitchat: chitchat,
    discovery,
    leader_state,
    _service: quorum_service,
    state_dir,
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn steady_state_ids_are_globally_monotonic() {
  let label = format!("quorum-smoke-{}", std::process::id());

  let api_port1 = free_port();
  let api_port2 = free_port();
  let api_port3 = free_port();
  let cluster_port1 = free_port();
  let cluster_port2 = free_port();
  let cluster_port3 = free_port();

  let seed1: SocketAddr = format!("127.0.0.1:{cluster_port1}").parse().unwrap();

  let n1 = spawn_node(&label, "n1", api_port1, cluster_port1, vec![]).await;
  let n2 = spawn_node(&label, "n2", api_port2, cluster_port2, vec![seed1]).await;
  let n3 = spawn_node(&label, "n3", api_port3, cluster_port3, vec![seed1]).await;

  wait_for_all_members(&[n1.discovery.clone(), n2.discovery.clone(), n3.discovery.clone()], 3).await;

  let states = vec![n1.leader_state.clone(), n2.leader_state.clone(), n3.leader_state.clone()];
  let (_leader_idx, _term) = wait_for_leader(&states).await;

  let mk_client = |addr: String| ClientConfig {
    cert: Some("tests/certs/katotonic.client-client.pem".to_string()),
    key: Some("tests/certs/katotonic.client-client-key.pem".to_string()),
    ca: "tests/certs/rootCA.pem".to_string(),
    addr,
    server_name: Some("localhost".to_string()),
  };

  let c1 = Client::new(mk_client(n1.conf.addr.clone()), 2).await.unwrap();
  let c2 = Client::new(mk_client(n2.conf.addr.clone()), 2).await.unwrap();
  let c3 = Client::new(mk_client(n3.conf.addr.clone()), 2).await.unwrap();
  let clients = [c1, c2, c3];

  let mut ids = Vec::new();
  for i in 0..60usize {
    let id = clients[i % clients.len()].next_id().await.unwrap();
    ids.push(id);
  }

  for window in ids.windows(2) {
    assert!(
      window[0].as_bytes() < window[1].as_bytes(),
      "ids not monotonic: {} >= {}",
      window[0],
      window[1]
    );
  }

  fs::remove_dir_all(n1.state_dir).unwrap();
  fs::remove_dir_all(n2.state_dir).unwrap();
  fs::remove_dir_all(n3.state_dir).unwrap();
}
