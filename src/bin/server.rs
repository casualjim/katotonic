use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chitchat::{spawn_chitchat, transport::UdpTransport};
use clap::Parser;
use futures::StreamExt;
use tokio::time::sleep;
use tracing::{info, instrument};
use ulidd::{
  cluster::disco::ChitchatDiscovery,
  protocol::{Request, Response},
  server::{self, Handler},
};

struct MonotonicHandler(Arc<tokio::sync::Mutex<ulid::Generator>>);

impl std::fmt::Debug for MonotonicHandler {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("MonotonicHandler").finish()
  }
}

impl MonotonicHandler {
  fn new(generator: ulid::Generator) -> Self {
    Self(Arc::new(tokio::sync::Mutex::new(generator)))
  }

  async fn generate(&self) -> Result<ulid::Ulid, ulid::MonotonicError> {
    self.0.lock().await.generate()
  }
}

impl Default for MonotonicHandler {
  fn default() -> Self {
    Self::new(ulid::Generator::new())
  }
}

#[async_trait]
impl Handler for MonotonicHandler {
  #[instrument]
  async fn handle(&self, request: Request) -> ulidd::Result<Response> {
    match request {
      Request::NewId(id) => {
        for _ in 0..3 {
          match self.generate().await {
            Ok(ulid) => {
              return Ok(Response::Id(id, ulid.to_bytes()));
            }
            Err(ulid::MonotonicError::Overflow) => {
              sleep(Duration::from_millis(1)).await;
            }
          }
        }
        return Ok(Response::Error(id, "unable to generate id".to_string()));
      }
      Request::Heartbeat => Ok(Response::HeartbeatAck),
    }
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::init();

  let conf = ulidd::ServerConfig::parse();

  let chitchat_config: chitchat::ChitchatConfig = (&conf).into();
  info!(
    "chitchat config cluster_id={} chitchat_id={:?} seed_nodes={:?}",
    chitchat_config.cluster_id, chitchat_config.chitchat_id, chitchat_config.seed_nodes
  );
  let handle = spawn_chitchat(chitchat_config, vec![], &UdpTransport).await?;
  let cc = handle.chitchat();

  let mut change_watcher = cc.lock().await.live_nodes_watcher();
  let _disc = ChitchatDiscovery::new(cc.clone());

  tokio::spawn(async move {
    // let mut interval = tokio::time::interval(Duration::from_secs(5));
    // loop {
    //   interval.tick().await;
    //   let members = disc.members().await.unwrap();
    //   info!("members: {:?}", members);
    // }
    while let Some(change) = change_watcher.next().await {
      // let members = change
      //   .iter()
      //   .map(|(k, _)| k.gossip_advertise_addr)
      //   .collect::<Vec<_>>();
      // info!("members: {:?}", members);
      info!("change: {:?}", change);
    }
  });

  // let ccb = cc.clone();
  // tokio::spawn(async move {
  //   let guard = ccb.lock().await;
  //   let mut watcher = guard.live_nodes_watcher();
  //   drop(guard);
  //   while let Some(node) = watcher.next().await {
  //     info!("live node: {:?}", node);
  //   }
  // });

  // tokio::spawn(join_mesh(conf.clone()));
  server::run(conf, MonotonicHandler::default()).await?;
  handle.shutdown().await?;
  Ok(())
}

// async fn join_mesh(conf: ulidd::ServerConfig) -> ulidd::Result<()> {
//   let client_cert_file = conf
//     .cluster_client_cert
//     .expect("need a cluster client cert");
//   let client_key_file = conf.cluster_client_key.expect("need a cluster client key");
//   let server_cert_file = &conf.cluster_cert;
//   let server_key_file = &conf.cluster_key;
//   let ca_file: &str = &conf.cluster_ca;

//   let (client_certs, client_key) = ulidd::keypair(client_cert_file, client_key_file)?;
//   let (server_certs, server_key) = ulidd::keypair(server_cert_file, server_key_file)?;
//   let ca_certs = root_store(ca_file)?;

//   let client_tls_config = ClientConfig::builder()
//     .with_root_certificates(ca_certs.clone())
//     .with_client_auth_cert(client_certs, client_key)
//     .expect("unable to create client auth tls config");

//   let server_tls_config = ServerConfig::builder()
//     .with_client_cert_verifier(
//       WebPkiClientVerifier::builder(Arc::new(ca_certs))
//         .build()
//         .expect("enable to build client verifier"),
//     )
//     .with_single_cert(server_certs, server_key)
//     .expect("unable to create server tls config");

//   // let acceptor = TlsAcceptor::from()
//   let connector = TlsConnector::from(Arc::new(client_tls_config));
//   let acceptor = TlsAcceptor::from(Arc::new(server_tls_config));

//   // let pk = SecretKey::from([0; 32]);
//   let server_name = ServerName::try_from("localhost").expect("unable to create servername for SNI");
//   let mut transport_opts: NetTransportOptions<
//     SmolStr,
//     SocketAddrResolver<TokioRuntime>,
//     Tls<TokioRuntime>,
//   > = NetTransportOptions::with_stream_layer_options(
//     SmolStr::new(Ulid::new().to_string().as_str()),
//     TlsOptions::new(server_name, acceptor, connector),
//   );
//   let addr = conf
//     .cluster_addr
//     .expect("need a cluster address")
//     .parse::<SocketAddr>()?;
//   transport_opts.add_bind_address(addr);

//   let opts = Options::new().with_tags([("role", "server")].into_iter());

//   let serf: ruserf::Serf<
//     NetTransport<SmolStr, SocketAddrResolver<TokioRuntime>, Tls<TokioRuntime>, Lpe<_, _>, _>,
//     ruserf::delegate::CompositeDelegate<_, _>,
//   > = ruserf::Serf::new(transport_opts, opts)
//     .await
//     .expect("failed to run membership transport");

//   let member = serf.members().await;
//   let server_role = SmolStr::new_inline("server");
//   let servers = member
//     .iter()
//     .filter(|mbr| mbr.tags().get("role") == Some(&server_role))
//     .filter(|mbr| mbr.status() == &MemberStatus::Alive)
//     .collect::<Vec<_>>();

//   info!("servers: {:?}", servers);
//   if !conf.bootstrap {
//     serf
//       .join(
//         Node::new(
//           "peer_addr=01J1PGHSQ7QNDW5CEQSQRHR3TJ".into(),
//           MaybeResolvedAddress::resolved("127.0.0.1:9100".parse::<SocketAddr>().unwrap()),
//         ),
//         true,
//       )
//       .await
//       .expect("unable to join");
//   }
//   let _ = serf.shutdown_rx().recv().await;
//   Ok(())
// }

// /// A default implementation of the `MergeDelegate` trait.
// #[derive(Debug, Clone, Copy)]
// pub struct UlidMergeDelegate<I, A>(std::marker::PhantomData<(I, A)>);

// impl<I, A> Default for UlidMergeDelegate<I, A> {
//   fn default() -> Self {
//     Self(Default::default())
//   }
// }

// impl<I, A> MergeDelegate for UlidMergeDelegate<I, A>
// where
//   I: Id,
//   A: CheapClone + Send + Sync + Debug + 'static,
// {
//   type Error = std::convert::Infallible;
//   type Id = I;
//   type Address = A;

//   async fn notify_merge(
//     &self,
//     members: TinyVec<Member<Self::Id, Self::Address>>,
//   ) -> Result<(), Self::Error> {
//     info!("merge: {members:?}");
//     Ok(())
//   }
// }

// pub fn root_store<P: AsRef<Path>>(ca: P) -> ulidd::Result<RootCertStore> {
//   let file = File::open(ca)?;
//   let mut reader = BufReader::new(file);
//   let cert_chain = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
//   let mut root_store = RootCertStore::empty();
//   root_store.add_parsable_certificates(cert_chain.into_iter());
//   Ok(root_store)
// }

// struct MemberManager;

// impl Delegate for MemberManager {
//   type Id = u128;

//   type Address = SocketAddr;
// }

// impl MergeDelegate for MemberManager {
//   type Error = std::convert::Infallible;
//   type Id = u128;
//   type Address = SocketAddr;

//   fn notify_merge(
//     &self,
//     members: TinyVec<Member<Self::Id, Self::Address>>,
//   ) -> impl Future<Output = Result<(), Self::Error>> + Send {
//     async move {
//       for member in members {
//         println!("{:?}", member);
//       }
//       Ok(())
//     }
//   }
// }

// impl ReconnectDelegate for MemberManager {
//   type Id = u128;

//   type Address = SocketAddr;

//   fn reconnect_timeout(
//     &self,
//     member: &Member<Self::Id, Self::Address>,
//     timeout: Duration,
//   ) -> Duration {
//     println!("{:?} {:?}", member, timeout);
//     timeout
//   }
// }
