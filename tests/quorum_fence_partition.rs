use std::{
  env, fs,
  net::{SocketAddr, TcpListener as StdTcpListener},
  sync::Arc,
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use chitchat::ChitchatHandle;
use chitchat::{spawn_chitchat, transport::UdpTransport};
use tokio::{
  io::{AsyncReadExt as _, AsyncWriteExt as _},
  net::TcpStream,
  time::{sleep, timeout},
};
use tokio_rustls::TlsConnector;

use katotonic::{
  client_tls_config,
  disco::{ChitchatDiscovery, Discovery},
  quorum::{cluster::run_cluster_server, lease, LeaderState, QuorumService, QuorumState},
  server,
  ServerConfig, WatchableValue,
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
    "katotonic-fence-test-{label}-{node_id}-{}-{nanos}",
    std::process::id()
  ));
  fs::create_dir_all(&dir).unwrap();
  dir.join("state.cbor")
}

async fn wait_for_state<F>(state: &WatchableValue<LeaderState>, pred: F)
where
  F: Fn(&LeaderState) -> bool,
{
  timeout(Duration::from_secs(5), async {
    loop {
      if pred(&state.read()) {
        return;
      }
      state.wait_for_change_async().await;
    }
  })
  .await
  .expect("timeout waiting for leader state");
}

struct Node {
  conf: ServerConfig,
  discovery_handle: ChitchatHandle,
  discovery: Arc<ChitchatDiscovery>,
  leader_state: WatchableValue<LeaderState>,
  service: Arc<QuorumService>,
  cluster_task: tokio::task::JoinHandle<()>,
  api_task: tokio::task::JoinHandle<()>,
  lease_task: tokio::task::JoinHandle<()>,
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
    .map(|addr| katotonic::SeedNode {
      server_name: "localhost".to_string(),
      addr,
    })
    .collect();
  conf.state_path = path.to_string_lossy().to_string();

  conf.gossip_interval = Duration::from_millis(50);
  conf.lease_duration_ms = 250;
  conf.renew_interval_ms = 50;

  let (discovery_handle, discovery) = spawn_test_discovery(conf.clone()).await;

  let leader_state = WatchableValue::new(LeaderState::Down);
  let service = Arc::new(QuorumService::new(conf.state_path.clone(), leader_state.clone()).unwrap());

  let cluster_conf = conf.clone();
  let cluster_service = Arc::clone(&service);
  let cluster_task = tokio::spawn(async move {
    let _ = run_cluster_server(cluster_conf, cluster_service).await;
  });

  let api_conf = conf.clone();
  let api_discovery = Arc::clone(&discovery);
  let api_state = leader_state.clone();
  let api_service = Arc::clone(&service);
  let api_task = tokio::spawn(async move {
    let _ = server::run_server(api_conf, api_discovery, api_state, api_service).await;
  });

  let lease_conf = conf.clone();
  let lease_discovery = Arc::clone(&discovery);
  let lease_state = leader_state.clone();
  let lease_service = Arc::clone(&service);
  let lease_task = tokio::spawn(async move {
    let _ = lease::track_lease(lease_conf, lease_discovery, lease_state, lease_service).await;
  });

  Node {
    conf,
    discovery_handle,
    discovery,
    leader_state,
    service,
    cluster_task,
    api_task,
    lease_task,
    state_dir,
  }
}

async fn spawn_test_discovery(conf: ServerConfig) -> (ChitchatHandle, Arc<ChitchatDiscovery>) {
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

  let handle = spawn_chitchat(chitchat_config, tags, &UdpTransport)
    .await
    .unwrap();
  let cc = handle.chitchat();
  (handle, Arc::new(ChitchatDiscovery::new(cc, conf)))
}

async fn read_id_or_redirect_once(api_addr: SocketAddr) -> std::io::Result<Option<[u8; 16]>> {
  let config = client_tls_config(
    Some("tests/certs/katotonic.client-client.pem"),
    Some("tests/certs/katotonic.client-client-key.pem"),
    "tests/certs/rootCA.pem",
  )
  .unwrap();
  let connector = TlsConnector::from(Arc::new(config));
  let stream = TcpStream::connect(api_addr).await?;
  let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
  let mut tls = connector.connect(server_name, stream).await?;

  tls.write_all(&[1u8]).await?;
  let mut resp_type = [0u8; 1];
  tls.read_exact(&mut resp_type).await?;

  match resp_type[0] {
    1 => {
      let mut buf = [0u8; 16];
      tls.read_exact(&mut buf).await?;
      Ok(Some(buf))
    }
    2 => Ok(None),
    other => Err(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      format!("unexpected response type: {other}"),
    )),
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_leader_stops_serving_after_lease_expiry() {
  let label = format!("quorum-fence-{}", std::process::id());

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

  timeout(Duration::from_secs(5), async {
    loop {
      let counts = futures::future::join3(
        n1.discovery.member_count(),
        n2.discovery.member_count(),
        n3.discovery.member_count(),
      )
      .await;
      if counts.0.unwrap_or(0) >= 3 && counts.1.unwrap_or(0) >= 3 && counts.2.unwrap_or(0) >= 3 {
        break;
      }
      sleep(Duration::from_millis(50)).await;
    }
  })
  .await
  .expect("membership converge timeout");

  // Force n1 to be leader for term 1 and let lease loop acquire majority.
  n1.service.install_leader_term(1, 0).await.unwrap();
  n1.leader_state.write(LeaderState::Leader {
    term: 1,
    lease_valid: false,
  });

  wait_for_state(&n1.leader_state, |s| {
    matches!(s, LeaderState::Leader { lease_valid: true, .. })
  })
  .await;

  let id1 = read_id_or_redirect_once(n1.conf.addr().unwrap())
    .await
    .unwrap()
    .expect("expected leader to serve an id");

  // Partition n1 from quorum on the cluster port by aborting followers' cluster servers.
  n2.cluster_task.abort();
  n3.cluster_task.abort();

  // Wait for lease expiry to force step-down.
  wait_for_state(&n1.leader_state, |s| matches!(s, LeaderState::Down)).await;

  // Now the isolated former leader must not serve IDs.
  let api_addr = n1.conf.addr().unwrap();
  let res = timeout(Duration::from_millis(300), read_id_or_redirect_once(api_addr)).await;
  assert!(
    res.is_err(),
    "expected request to time out (no serving) after lease expiry; got: {res:?}"
  );

  // Keep at least one sanity check that IDs are not zeroed nonsense.
  assert!(id1.iter().any(|b| *b != 0));

  // cleanup
  n1.cluster_task.abort();
  n1.api_task.abort();
  n1.lease_task.abort();
  n2.api_task.abort();
  n2.lease_task.abort();
  n3.api_task.abort();
  n3.lease_task.abort();
  drop(n1.discovery_handle);
  drop(n2.discovery_handle);
  drop(n3.discovery_handle);
  let _ = fs::remove_dir_all(n1.state_dir);
  let _ = fs::remove_dir_all(n2.state_dir);
  let _ = fs::remove_dir_all(n3.state_dir);
}
