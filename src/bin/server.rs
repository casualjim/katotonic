use std::{fs::File, io::BufReader, net::SocketAddr, path::Path, sync::Arc};

use clap::Parser;
use ruserf::{
  agnostic::tokio::TokioRuntime,
  net::{
    resolver::socket_addr::SocketAddrResolver,
    security::SecretKey,
    stream_layer::tls::{
      rustls::{server::WebPkiClientVerifier, ClientConfig, RootCertStore, ServerConfig},
      Tls, TlsAcceptor, TlsConnector, TlsOptions,
    },
    Lpe, MaybeResolvedAddress, NetTransport, NetTransportOptions, Node,
  },
  Options,
};
use rustls::pki_types::ServerName;
use smol_str::SmolStr;
use tracing::instrument;
use ulid::Ulid;
use ulidd::{
  cluster,
  protocol::{Request, Response},
  server,
};

#[instrument]
async fn handler(request: Request) -> ulidd::Result<Response> {
  let response = match request {
    Request::NewId(id) => {
      let new_data = Ulid::new().to_bytes();
      Response::Id(id, new_data)
    }
    Request::Heartbeat => Response::HeartbeatAck,
  };
  Ok(response)
}

#[tokio::main]
async fn main() -> ulidd::Result<()> {
  tracing_subscriber::fmt::init();

  let conf = ulidd::ServerConfig::parse();

  // let client_cert_file = "ulidd.client-client.pem";
  // let client_key_file = "ulidd.client-client-key.pem";
  // let server_cert_file = "ulidd.service+3.pem";
  // let server_key_file = "ulidd.service+3-key.pem";
  // let ca_file: &str = "/Users/ivan/Library/Application Support/mkcert/rootCA.pem";

  // let (client_certs, client_key) = ulidd::keypair(client_cert_file, client_key_file)?;
  // let (server_certs, server_key) = ulidd::keypair(server_cert_file, server_key_file)?;
  // let ca_certs = root_store(ca_file)?;

  // let client_tls_config = ClientConfig::builder()
  //   .with_root_certificates(ca_certs.clone())
  //   .with_client_auth_cert(client_certs, client_key)
  //   .expect("unable to create client auth tls config");

  // let server_tls_config = ServerConfig::builder()
  //   .with_client_cert_verifier(
  //     WebPkiClientVerifier::builder(Arc::new(ca_certs))
  //       .build()
  //       .expect("enable to build client verifier"),
  //   )
  //   .with_single_cert(server_certs, server_key)
  //   .expect("unable to create server tls config");

  // // let acceptor = TlsAcceptor::from()
  // let connector = TlsConnector::from(Arc::new(client_tls_config));
  // let acceptor = TlsAcceptor::from(Arc::new(server_tls_config));

  // // let pk = SecretKey::from([0; 32]);
  // let server_name = ServerName::try_from("127.0.0.1").expect("unable to create servername for SNI");
  // let mut transport_opts: NetTransportOptions<
  //   SmolStr,
  //   SocketAddrResolver<TokioRuntime>,
  //   Tls<TokioRuntime>,
  // > = NetTransportOptions::with_stream_layer_options(
  //   SmolStr::new(Ulid::new().to_string().as_str()),
  //   TlsOptions::new(server_name, acceptor, connector),
  // );
  // transport_opts.add_bind_address("127.0.0.1:9101".parse::<std::net::SocketAddr>().unwrap());

  // let opts = Options::new();
  // let serf: ruserf::Serf<
  //   NetTransport<SmolStr, SocketAddrResolver<TokioRuntime>, Tls<TokioRuntime>, Lpe<_, _>, _>,
  //   ruserf::delegate::CompositeDelegate<_, _>,
  // > = ruserf::Serf::new(transport_opts, opts)
  //   .await
  //   .expect("failed to run membership transport");
  // // serf.shutdown_rx()
  // let member = serf.members().await;
  // println!("{:?}", member);
  // serf
  //   .join(
  //     Node::new(
  //       "01J1KVYXWXVTWK5M0JAB9J4SBY".into(),
  //       MaybeResolvedAddress::resolved("127.0.0.1:9100".parse::<SocketAddr>().unwrap()),
  //     ),
  //     true,
  //   )
  //   .await
  //   .expect("unable to join");

  tokio::spawn(cluster::run(conf.clone()));
  server::run(conf, handler).await?;
  Ok(())
}

pub fn root_store<P: AsRef<Path>>(ca: P) -> ulidd::Result<RootCertStore> {
  let file = File::open(ca)?;
  let mut reader = BufReader::new(file);
  let cert_chain = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
  let mut root_store = RootCertStore::empty();
  root_store.add_parsable_certificates(cert_chain.into_iter());
  Ok(root_store)
}
