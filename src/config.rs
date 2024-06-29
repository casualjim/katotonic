// Copyright (c) 2024 Ivan Porto Carrero
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{
  fs::File,
  io::{self, BufReader},
  path::Path,
};

use crate::{Error, Result};
use clap::Parser;
use rustls::{
  pki_types::{CertificateDer, PrivateKeyDer},
  RootCertStore,
};
use std::net::SocketAddr;

#[derive(Parser)]
pub struct ClientConfig {
  #[clap(long, default_value = "ulidd.client-client.pem")]
  pub cert: Option<String>,
  #[clap(long, default_value = "ulidd.client-client-key.pem")]
  pub key: Option<String>,
  #[clap(
    long,
    default_value = "/Users/ivan/Library/Application Support/mkcert/rootCA.pem"
  )]
  pub ca: String,
  #[clap(long, default_value = "localhost:9000")]
  pub addr: String,
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

  pub fn addr(&self) -> &str {
    &self.addr
  }

  pub fn server_name(&self) -> &str {
    self.addr.split(':').next().unwrap()
  }
}

#[derive(Parser)]
pub struct ServerConfig {
  #[clap(long, default_value = "ulidd.service+3.pem")]
  pub cert: String,
  #[clap(long, default_value = "ulidd.service+3-key.pem")]
  pub key: String,
  #[clap(
    long,
    default_value = "/Users/ivan/Library/Application Support/mkcert/rootCA.pem"
  )]
  pub ca: Option<String>,
  #[clap(long, default_value = "127.0.0.1:9000")]
  pub addr: String,
}

impl ServerConfig {
  pub fn new() -> crate::Result<Self> {
    Ok(Self::try_parse()?)
  }

  pub fn root_store(&self) -> Result<Option<RootCertStore>> {
    if let Some(ca) = self.ca.as_ref() {
      root_store(ca).map(Some)
    } else {
      Ok(None)
    }
  }

  pub fn keypair(&self) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    keypair(&self.cert, &self.key).map(Some)
  }

  pub fn addr(&self) -> Result<SocketAddr> {
    Ok(self.addr.parse()?)
  }

  pub fn server_name(&self) -> &str {
    self.addr.split(':').next().unwrap()
  }
}

fn keypair<P: AsRef<Path>>(
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

fn root_store<P: AsRef<Path>>(ca: P) -> Result<RootCertStore> {
  let file = File::open(ca)?;
  let mut reader = BufReader::new(file);
  let cert_chain = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
  let mut root_store = RootCertStore::empty();
  root_store.add_parsable_certificates(cert_chain.into_iter());
  Ok(root_store)
}
