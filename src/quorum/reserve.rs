use std::{borrow::Cow, sync::Arc, time::Duration};

use futures::future::join_all;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use super::{
  config::JointQuorum, election, rpc::QuorumPeer, QuorumRequest, QuorumResponse, QuorumService,
};
use crate::{disco::Discovery, Result, ServerConfig};

const RESERVE_RPC_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RESERVE_ATTEMPTS: usize = 3;

pub async fn reserve_next_block(
  conf: &ServerConfig,
  discovery: Arc<crate::disco::ChitchatDiscovery>,
  service: &QuorumService,
  term: u64,
  block_size: u64,
) -> Result<(u64, u64)> {
  let this_node = discovery.myself().await?;
  let members = discovery.members().await?;
  let state = service.get_state().await;
  let local_config_version = state.config_version;
  let quorum_cfg = JointQuorum::new(local_config_version, &members);
  let joint_majority = quorum_cfg.joint_majority();
  let old_majority = quorum_cfg.old_majority();

  let peers = members
    .into_iter()
    .filter(|m| m.name != this_node.name)
    .collect::<Vec<_>>();

  let client_config = election::cluster_client_config(conf)?;

  let mut base_hw = state.high_watermark;
  for attempt in 1..=MAX_RESERVE_ATTEMPTS {
    let start_seq = base_hw.saturating_add(1);
    let end_seq = start_seq
      .checked_add(block_size.saturating_sub(1))
      .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "block end overflow"))?;

    debug!(
      term,
      attempt,
      start_seq,
      end_seq,
      joint_majority,
      old_majority,
      joint = quorum_cfg.is_joint(),
      peers = peers.len(),
      "reserving block"
    );

    if peers.is_empty() {
      service.advance_high_watermark_to(end_seq).await?;
      return Ok((start_seq, end_seq));
    }

    let mut tasks = Vec::new();
    for peer in peers.iter().cloned() {
      let addr = Cow::Owned(peer.addr.to_string());
      let server_name = election::server_name_for(&peer);
      let peer_client = QuorumPeer::new_tls(addr, client_config.clone(), server_name);

      let is_old = quorum_cfg.is_old(&peer.name);
      tasks.push(tokio::spawn(async move {
        (
          is_old,
          timeout(
            RESERVE_RPC_TIMEOUT,
            peer_client.send(QuorumRequest::ReserveBlock {
              term,
              start_seq,
              end_seq,
            }),
          )
          .await,
        )
      }));
    }

    let mut yes = 1usize; // self-ack
    let mut old_yes = if quorum_cfg.is_old(&this_node.name) {
      1usize
    } else {
      0usize
    };
    let mut max_hw_seen = base_hw;
    let mut highest_term_seen = term;

    for res in join_all(tasks).await {
      match res {
        Ok((is_old, Ok(Ok(response)))) => match response {
          QuorumResponse::ReserveAck {
            term: ack_term,
            high_watermark,
          } => {
            highest_term_seen = highest_term_seen.max(ack_term);
            if ack_term == term {
              yes += 1;
              if is_old {
                old_yes += 1;
              }
              max_hw_seen = max_hw_seen.max(high_watermark);
            }
          }
          QuorumResponse::ReserveNack {
            term: nack_term,
            high_watermark,
          } => {
            highest_term_seen = highest_term_seen.max(nack_term);
            max_hw_seen = max_hw_seen.max(high_watermark);
          }
          other => {
            debug!(?other, "unexpected quorum response during reserve");
          }
        },
        Ok((_is_old, Ok(Err(e)))) => {
          debug!("reserve request failed: {e}");
        }
        Ok((_is_old, Err(_elapsed))) => {
          debug!("reserve request timed out");
        }
        Err(join_err) => {
          debug!("reserve task join error: {join_err}");
        }
      }
    }

    if highest_term_seen > term {
      service.observe_higher_voted_term(highest_term_seen).await?;
      return Err(std::io::Error::other("higher term observed during reserve").into());
    }

    if max_hw_seen > end_seq {
      service.advance_high_watermark_to(max_hw_seen).await?;
      base_hw = max_hw_seen;
      continue;
    }

    let has_joint_majority = yes >= joint_majority;
    let has_old_majority = !quorum_cfg.is_joint() || old_yes >= old_majority;

    if has_joint_majority && has_old_majority {
      let new_hw = service.advance_high_watermark_to(end_seq).await?;
      info!(
        term,
        yes,
        joint_majority,
        old_yes,
        old_majority,
        joint = quorum_cfg.is_joint(),
        start_seq,
        end_seq,
        new_hw,
        "reserved block"
      );
      return Ok((start_seq, end_seq));
    }

    warn!(
      term,
      yes,
      joint_majority,
      old_yes,
      old_majority,
      joint = quorum_cfg.is_joint(),
      start_seq,
      end_seq,
      "reserve failed"
    );
    return Err(
      std::io::Error::new(std::io::ErrorKind::TimedOut, "failed to reserve block").into(),
    );
  }

  Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "reserve retries exhausted").into())
}

#[cfg(test)]
mod tests {
  use std::{
    env, fs,
    net::{SocketAddr, TcpListener as StdTcpListener},
    time::{Duration, SystemTime, UNIX_EPOCH},
  };

  use chitchat::{spawn_chitchat, transport::UdpTransport, ChitchatHandle};
  use tokio::time::{sleep, timeout};

  use super::*;
  use crate::{
    disco::ChitchatDiscovery,
    quorum::{cluster::run_cluster_server, LeaderState, QuorumService, QuorumState},
    SeedNode, ServerConfig, WatchableValue,
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
      "katotonic-reserve-test-{label}-{node_id}-{}-{nanos}",
      std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir.join("state.cbor")
  }

  fn make_conf(
    node_id: &str,
    cluster_id: &str,
    api_port: u16,
    cluster_port: u16,
    seeds: Vec<SocketAddr>,
    state_path: String,
  ) -> ServerConfig {
    let mut conf = ServerConfig::for_tests().unwrap();
    conf.node_id = Some(node_id.to_string());
    conf.cluster_id = cluster_id.to_string();
    conf.addr = format!("127.0.0.1:{api_port}");
    conf.cluster_addr = Some(format!("127.0.0.1:{cluster_port}"));
    conf.seed = seeds
      .into_iter()
      .map(|addr| SeedNode {
        server_name: "localhost".to_string(),
        addr,
      })
      .collect();
    conf.state_path = state_path;
    conf
  }

  async fn spawn_test_discovery(
    conf: ServerConfig,
  ) -> anyhow::Result<(ChitchatHandle, Arc<ChitchatDiscovery>)> {
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
    Ok((handle, Arc::new(ChitchatDiscovery::new(cc, conf))))
  }

  async fn wait_for_members(
    discovery: Arc<ChitchatDiscovery>,
    expected: usize,
  ) -> crate::Result<()> {
    timeout(Duration::from_secs(3), async move {
      loop {
        if discovery.members().await?.len() >= expected {
          return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
      }
    })
    .await
    .map_err(|_| std::io::Error::other("membership timeout"))?
  }

  #[tokio::test]
  async fn reserve_advances_high_watermark_on_majority() {
    let cluster_id = format!("reserve-majority-{}", std::process::id());

    let api_port1 = free_port();
    let api_port2 = free_port();
    let api_port3 = free_port();
    let cluster_port1 = free_port();
    let cluster_port2 = free_port();
    let cluster_port3 = free_port();

    let seed1: SocketAddr = format!("127.0.0.1:{cluster_port1}").parse().unwrap();

    let path1 = temp_state_path("majority", "n1");
    let path2 = temp_state_path("majority", "n2");
    let path3 = temp_state_path("majority", "n3");

    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path1)
    .unwrap();
    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path2)
    .unwrap();
    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path3)
    .unwrap();

    let conf1 = make_conf(
      "n1",
      &cluster_id,
      api_port1,
      cluster_port1,
      vec![],
      path1.to_string_lossy().to_string(),
    );
    let conf2 = make_conf(
      "n2",
      &cluster_id,
      api_port2,
      cluster_port2,
      vec![seed1],
      path2.to_string_lossy().to_string(),
    );
    let conf3 = make_conf(
      "n3",
      &cluster_id,
      api_port3,
      cluster_port3,
      vec![seed1],
      path3.to_string_lossy().to_string(),
    );

    let leader_state1 = WatchableValue::new(LeaderState::Down);
    let leader_service =
      QuorumService::new(conf1.state_path.clone(), leader_state1.clone()).unwrap();

    let follower_state2 = WatchableValue::new(LeaderState::Down);
    let follower_service2 =
      Arc::new(QuorumService::new(conf2.state_path.clone(), follower_state2).unwrap());
    let follower_state3 = WatchableValue::new(LeaderState::Down);
    let follower_service3 =
      Arc::new(QuorumService::new(conf3.state_path.clone(), follower_state3).unwrap());

    tokio::spawn(run_cluster_server(conf2.clone(), Arc::clone(&follower_service2)));
    tokio::spawn(run_cluster_server(conf3.clone(), Arc::clone(&follower_service3)));

    let (_h1, discovery1) = spawn_test_discovery(conf1.clone()).await.unwrap();
    let (_h2, _discovery2) = spawn_test_discovery(conf2).await.unwrap();
    let (_h3, _discovery3) = spawn_test_discovery(conf3).await.unwrap();

    wait_for_members(Arc::clone(&discovery1), 3).await.unwrap();

    let (start, end) = reserve_next_block(
      &conf1,
      Arc::clone(&discovery1),
      &leader_service,
      1,
      10,
    )
    .await
    .unwrap();

    assert_eq!((start, end), (1, 10));
    assert_eq!(leader_service.get_state().await.high_watermark, 10);
    assert_eq!(follower_service2.get_state().await.high_watermark, 10);
    assert_eq!(follower_service3.get_state().await.high_watermark, 10);

    let dir1 = path1.parent().unwrap().to_path_buf();
    let dir2 = path2.parent().unwrap().to_path_buf();
    let dir3 = path3.parent().unwrap().to_path_buf();
    fs::remove_dir_all(dir1).unwrap();
    fs::remove_dir_all(dir2).unwrap();
    fs::remove_dir_all(dir3).unwrap();
  }

  #[tokio::test]
  async fn reserve_retries_when_peer_has_higher_high_watermark() {
    let cluster_id = format!("reserve-retry-{}", std::process::id());

    let api_port1 = free_port();
    let api_port2 = free_port();
    let api_port3 = free_port();
    let cluster_port1 = free_port();
    let cluster_port2 = free_port();
    let cluster_port3 = free_port();

    let seed1: SocketAddr = format!("127.0.0.1:{cluster_port1}").parse().unwrap();

    let path1 = temp_state_path("retry", "n1");
    let path2 = temp_state_path("retry", "n2");
    let path3 = temp_state_path("retry", "n3");

    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path1)
    .unwrap();
    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 50,
      config_version: 0,
    }
    .persist(&path2)
    .unwrap();
    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path3)
    .unwrap();

    let conf1 = make_conf(
      "n1",
      &cluster_id,
      api_port1,
      cluster_port1,
      vec![],
      path1.to_string_lossy().to_string(),
    );
    let conf2 = make_conf(
      "n2",
      &cluster_id,
      api_port2,
      cluster_port2,
      vec![seed1],
      path2.to_string_lossy().to_string(),
    );
    let conf3 = make_conf(
      "n3",
      &cluster_id,
      api_port3,
      cluster_port3,
      vec![seed1],
      path3.to_string_lossy().to_string(),
    );

    let leader_state1 = WatchableValue::new(LeaderState::Down);
    let leader_service =
      QuorumService::new(conf1.state_path.clone(), leader_state1.clone()).unwrap();

    let follower_state2 = WatchableValue::new(LeaderState::Down);
    let follower_service2 =
      Arc::new(QuorumService::new(conf2.state_path.clone(), follower_state2).unwrap());
    let follower_state3 = WatchableValue::new(LeaderState::Down);
    let follower_service3 =
      Arc::new(QuorumService::new(conf3.state_path.clone(), follower_state3).unwrap());

    tokio::spawn(run_cluster_server(conf2.clone(), Arc::clone(&follower_service2)));
    tokio::spawn(run_cluster_server(conf3.clone(), Arc::clone(&follower_service3)));

    let (_h1, discovery1) = spawn_test_discovery(conf1.clone()).await.unwrap();
    let (_h2, _discovery2) = spawn_test_discovery(conf2).await.unwrap();
    let (_h3, _discovery3) = spawn_test_discovery(conf3).await.unwrap();

    wait_for_members(Arc::clone(&discovery1), 3).await.unwrap();

    let (start, end) = reserve_next_block(
      &conf1,
      Arc::clone(&discovery1),
      &leader_service,
      1,
      10,
    )
    .await
    .unwrap();

    assert_eq!((start, end), (51, 60));
    assert_eq!(leader_service.get_state().await.high_watermark, 60);
    assert_eq!(follower_service2.get_state().await.high_watermark, 60);
    assert_eq!(follower_service3.get_state().await.high_watermark, 60);

    let dir1 = path1.parent().unwrap().to_path_buf();
    let dir2 = path2.parent().unwrap().to_path_buf();
    let dir3 = path3.parent().unwrap().to_path_buf();
    fs::remove_dir_all(dir1).unwrap();
    fs::remove_dir_all(dir2).unwrap();
    fs::remove_dir_all(dir3).unwrap();
  }

  #[tokio::test]
  async fn reserve_aborts_on_higher_term_observed() {
    let cluster_id = format!("reserve-higher-term-{}", std::process::id());

    let api_port1 = free_port();
    let api_port2 = free_port();
    let api_port3 = free_port();
    let cluster_port1 = free_port();
    let cluster_port2 = free_port();
    let cluster_port3 = free_port();

    let seed1: SocketAddr = format!("127.0.0.1:{cluster_port1}").parse().unwrap();

    let path1 = temp_state_path("higher-term", "n1");
    let path2 = temp_state_path("higher-term", "n2");
    let path3 = temp_state_path("higher-term", "n3");

    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path1)
    .unwrap();
    QuorumState {
      current_term: 2,
      voted_term: 2,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path2)
    .unwrap();
    QuorumState {
      current_term: 1,
      voted_term: 1,
      high_watermark: 0,
      config_version: 0,
    }
    .persist(&path3)
    .unwrap();

    let conf1 = make_conf(
      "n1",
      &cluster_id,
      api_port1,
      cluster_port1,
      vec![],
      path1.to_string_lossy().to_string(),
    );
    let conf2 = make_conf(
      "n2",
      &cluster_id,
      api_port2,
      cluster_port2,
      vec![seed1],
      path2.to_string_lossy().to_string(),
    );
    let conf3 = make_conf(
      "n3",
      &cluster_id,
      api_port3,
      cluster_port3,
      vec![seed1],
      path3.to_string_lossy().to_string(),
    );

    let leader_state1 = WatchableValue::new(LeaderState::Down);
    let leader_service =
      QuorumService::new(conf1.state_path.clone(), leader_state1.clone()).unwrap();

    let follower_state2 = WatchableValue::new(LeaderState::Down);
    let follower_service2 =
      Arc::new(QuorumService::new(conf2.state_path.clone(), follower_state2).unwrap());
    let follower_state3 = WatchableValue::new(LeaderState::Down);
    let follower_service3 =
      Arc::new(QuorumService::new(conf3.state_path.clone(), follower_state3).unwrap());

    tokio::spawn(run_cluster_server(conf2.clone(), Arc::clone(&follower_service2)));
    tokio::spawn(run_cluster_server(conf3.clone(), Arc::clone(&follower_service3)));

    let (_h1, discovery1) = spawn_test_discovery(conf1.clone()).await.unwrap();
    let (_h2, _discovery2) = spawn_test_discovery(conf2).await.unwrap();
    let (_h3, _discovery3) = spawn_test_discovery(conf3).await.unwrap();

    wait_for_members(Arc::clone(&discovery1), 3).await.unwrap();

    let err = reserve_next_block(
      &conf1,
      Arc::clone(&discovery1),
      &leader_service,
      1,
      10,
    )
    .await
    .unwrap_err();

    assert!(
      err.to_string().contains("higher term observed"),
      "unexpected error: {err}"
    );
    let st = leader_service.get_state().await;
    assert_eq!(st.voted_term, 2);
    assert_eq!(st.high_watermark, 0);

    let dir1 = path1.parent().unwrap().to_path_buf();
    let dir2 = path2.parent().unwrap().to_path_buf();
    let dir3 = path3.parent().unwrap().to_path_buf();
    fs::remove_dir_all(dir1).unwrap();
    fs::remove_dir_all(dir2).unwrap();
    fs::remove_dir_all(dir3).unwrap();
  }
}
