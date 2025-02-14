// Copyright (c) 2024 Ivan Porto Carrero
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{
  fs::File,
  io::{self, BufReader},
  net::{SocketAddr, ToSocketAddrs},
  path::Path,
  sync::Arc,
  time::{self, Duration},
};

use chitchat::{ChitchatConfig, ChitchatId};
use clap::Parser;
use rand::Rng as _;
use rustls::{
  pki_types::{CertificateDer, PrivateKeyDer},
  RootCertStore,
};
use ulid::Ulid;

use crate::{Error, Result};

#[derive(Parser)]
pub struct ClientConfig {
  #[clap(long, default_value = "tests/certs/ulidd.client-client.pem")]
  pub cert: Option<String>,
  #[clap(long, default_value = "tests/certs/ulidd.client-client-key.pem")]
  pub key: Option<String>,
  #[clap(long, default_value = "tests/certs/rootCA.pem")]
  pub ca: String,
  #[clap(long, default_value = "localhost:9000")]
  pub addr: String,

  #[clap(long, default_value = "localhost")]
  pub server_name: Option<String>,
}

impl ClientConfig {
  pub fn new() -> crate::Result<Self> {
    Ok(Self::try_parse()?)
  }

  pub fn root_store(&self) -> Result<RootCertStore> {
    root_store(&self.ca)
  }

  pub fn keypair(&self) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    match (self.cert.as_ref(), self.key.as_ref()) {
      (Some(cert), Some(key)) => keypair(cert, key).map(Some),
      _ => Ok(None),
    }
  }

  pub fn addr(&self) -> Result<SocketAddr> {
    Ok(
      self
        .addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid address"))?,
    )
  }

  pub fn server_name(&self) -> &str {
    self
      .server_name
      .as_deref()
      .unwrap_or_else(|| match self.addr.parse::<SocketAddr>() {
        Err(_) => self.addr.rsplit_once(':').unwrap().0,
        Ok(_) => panic!("Invalid server name"),
      })
  }
}

#[derive(Debug, Clone)]
pub struct SeedNode {
  pub server_name: String,
  pub addr: SocketAddr,
}

#[derive(Parser, Debug, Clone)]
pub struct ServerConfig {
  #[clap(long, default_value = "tests/certs/ulidd.service+3.pem")]
  pub cert: String,
  #[clap(long, default_value = "tests/certs/ulidd.service+3-key.pem")]
  pub key: String,
  #[clap(long, default_value = "tests/certs/ulidd.client-client-key.pem")]
  pub client_key: Option<String>,
  #[clap(long, default_value = "tests/certs/ulidd.client-client.pem")]
  pub client_cert: Option<String>,
  #[clap(long, default_value = "tests/certs/rootCA.pem")]
  pub ca: String,
  #[clap(long, default_value = "127.0.0.1:9000")]
  pub addr: String,
  #[clap(long, default_value = "false")]
  pub bootstrap: bool,
  #[clap(long, value_parser = parse_seed_node, value_delimiter = ',')]
  pub seed: Vec<SeedNode>,
  #[clap(long, default_value = "tests/certs/ulidd.service+3.pem")]
  pub cluster_cert: String,
  #[clap(long, default_value = "tests/certs/ulidd.service+3-key.pem")]
  pub cluster_key: String,
  #[clap(long, default_value = "tests/certs/ulidd.client-client-key.pem")]
  pub cluster_client_key: Option<String>,
  #[clap(long, default_value = "tests/certs/ulidd.client-client.pem")]
  pub cluster_client_cert: Option<String>,
  #[clap(long, default_value = "tests/certs/rootCA.pem")]
  pub cluster_ca: String,
  #[clap(long, default_value = "127.0.0.1:9100")]
  pub cluster_addr: Option<String>,
  #[clap(long, default_value = "default")]
  pub cluster_id: String,
  #[clap(long)]
  pub node_id: Option<String>,

  #[clap(long, default_value = "500ms", value_parser = humantime::parse_duration)]
  pub gossip_interval: time::Duration,
  #[clap(long)]
  pub gossip_addr: Option<SocketAddr>,
}

impl From<&ServerConfig> for ChitchatConfig {
  fn from(value: &ServerConfig) -> Self {
    let gossip_addr = value.cluster_addr().unwrap().unwrap();
    let mut rng = rand::thread_rng();
    let node_id = value
      .node_id
      .clone()
      .unwrap_or_else(|| Ulid::new().to_string());

    let generation = rng.gen_range(1_000..65_000);
    // let generation = 0;
    let chitchat_id = ChitchatId::new(node_id, generation, gossip_addr);
    ChitchatConfig {
      chitchat_id: chitchat_id,
      cluster_id: value.cluster_id.clone(),
      gossip_interval: value.gossip_interval,
      listen_addr: gossip_addr,
      seed_nodes: value.seed.iter().map(|s| s.addr.to_string()).collect(),
      failure_detector_config: chitchat::FailureDetectorConfig {
        dead_node_grace_period: Duration::from_secs(3600), // 1 hour
        initial_interval: Duration::from_millis(500),
        max_interval: Duration::from_millis(1500),
        ..Default::default()
      },
      marked_for_deletion_grace_period: 60_000,
    }
  }
}

fn parse_seed_node(s: &str) -> Result<SeedNode> {
  // Find the last occurrence of ':' to split the port part
  let colon_pos = s.rfind(':').ok_or(io::Error::new(
    io::ErrorKind::InvalidInput,
    "Invalid format: missing port",
  ))?;
  let seed_no_port = &s[..colon_pos];
  let port_str = &s[colon_pos + 1..];

  // Parse the port part
  let port = port_str
    .parse::<u16>()
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid port number"))?;

  // Check for the optional SNI name
  let parts: Vec<&str> = seed_no_port.split('=').collect();
  let (sni_name, host) = if parts.len() == 2 {
    (parts[0], parts[1])
  } else if parts.len() == 1 {
    (parts[0], parts[0])
  } else {
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      "Invalid format: multiple '=' found",
    ))?;
  };

  // Ensure the host is correctly formatted for IPv6
  let addr = format!("{}:{}", host, port).parse::<SocketAddr>()?;
  Ok(SeedNode {
    server_name: sni_name.to_string(),
    addr,
  })
}

impl ServerConfig {
  pub fn new() -> crate::Result<Self> {
    Ok(Self::try_parse()?)
  }

  pub fn root_store(&self) -> Result<Option<RootCertStore>> {
    root_store(self.ca.as_str()).map(Some)
  }

  pub fn keypair(&self) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    keypair(&self.cert, &self.key).map(Some)
  }

  pub fn addr(&self) -> Result<SocketAddr> {
    Ok(
      self
        .addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid address"))?,
    )
  }

  pub fn server_name(&self) -> &str {
    match self.addr.parse::<SocketAddr>() {
      Err(_) => self
        .addr
        .rsplit_once(':')
        .map(|(name, _)| name)
        .expect("Invalid server name"),
      Ok(_) => panic!("Invalid server name"),
    }
  }

  pub fn cluster_addr(&self) -> Result<Option<SocketAddr>> {
    Ok(
      self
        .cluster_addr
        .as_deref()
        .and_then(|addr| addr.to_socket_addrs().unwrap().next()),
    )
  }

  pub fn cluster_server_name(&self) -> Option<String> {
    self
      .cluster_addr
      .clone()
      .and_then(|addr| match addr.parse::<SocketAddr>() {
        Err(_) => addr.rsplit_once(':').map(|(name, _)| name.to_string()),
        Ok(_) => None,
      })
  }
}

pub fn keypair<P: AsRef<Path>>(
  cert: P,
  key: P,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
  let cert_file = File::open(cert)?;
  let mut cert_reader = BufReader::new(cert_file);
  let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

  let key_file = File::open(key)?;
  let mut key_reader = BufReader::new(key_file);

  if let Some(key) = rustls_pemfile::private_key(&mut key_reader)? {
    Ok((certs, key))
  } else {
    Err(Error::Io(io::Error::new(
      io::ErrorKind::InvalidInput,
      "No private key found",
    )))
  }
}

pub fn root_store<P: AsRef<Path>>(ca: P) -> Result<RootCertStore> {
  let file = File::open(ca)?;
  let mut reader = BufReader::new(file);
  let cert_chain = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
  let mut root_store = RootCertStore::empty();
  root_store.add_parsable_certificates(cert_chain.into_iter());
  Ok(root_store)
}

pub fn server_tls_config<P: AsRef<Path>>(
  cert: P,
  key: P,
  ca: Option<P>,
) -> Result<rustls::ServerConfig> {
  let (certs, private_key) = keypair(cert, key)?;
  let cfg_builder = rustls::ServerConfig::builder();

  let cfg_builder = match ca {
    Some(ca) => cfg_builder.with_client_cert_verifier(
      rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store(ca)?))
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
    ),
    None => cfg_builder.with_no_client_auth(),
  };

  Ok(
    cfg_builder
      .with_single_cert(certs, private_key)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
  )
}

pub fn client_tls_config<P: AsRef<Path>>(
  cert: Option<P>,
  key: Option<P>,
  ca: P,
) -> Result<rustls::ClientConfig> {
  let root_store = root_store(ca)?;
  let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

  let builder = match (cert, key) {
    (Some(cert), Some(key)) => {
      let (cert, key) = keypair(cert, key)?;
      builder
        .with_client_auth_cert(cert, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
    }
    _ => builder.with_no_client_auth(),
  };

  Ok(builder)
}
