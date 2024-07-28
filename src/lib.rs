#[macro_use]
mod defer;
pub mod bully;
pub mod client;
mod config;
pub mod disco;
mod idgen;
pub mod server;
mod state;
pub mod transport;
use std::io;

use client::pool::PoolError;
pub use config::*;
pub use idgen::generate_ulid as generate_monotonic_id;
use rustls::pki_types::InvalidDnsNameError;
pub use state::WatchableValue;
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("IO error: {0}")]
  Io(#[from] io::Error),
  #[error("TLS error: {0}")]
  Tls(#[from] rustls::Error),
  #[error("Serde error: {0}")]
  Serde(#[from] serde_cbor::Error),
  #[error("Serde error: {0}")]
  SerdeCbor(#[from] tokio_serde_cbor::Error),
  #[error("Unexpected response: {0}")]
  UnexpectedResponse(String),
  #[error("parse: {0}")]
  Parse(#[from] clap::Error),
  #[error("socket addr: {0}")]
  SocketAddr(#[from] std::net::AddrParseError),
  #[error("DNS resolution error: {0}")]
  Dns(#[from] trust_dns_resolver::error::ResolveError),
  #[error("Node not found: {0}")]
  NodeNotFound(String),
  #[error("Consensus not achieved")]
  ConsensusNotAchieved,
  #[error("Invalid server name: {0}")]
  InvalidServerName(#[from] InvalidDnsNameError),
  #[error("Connection pool initialization failed")]
  PoolInitializationFailed,
  #[error("unable to get a connection from the pool")]
  PoolAcquireConnectionFailed,
  #[cfg(feature = "smol")]
  #[error("Smol send error: {0}")]
  SmolRecvError(#[from] smol::channel::RecvError),
  #[error("Kanal receive error: {0}")]
  KanalRecv(#[from] kanal::ReceiveError),
  #[error("Kanal send error: {0}")]
  KanalSend(#[from] kanal::SendError),
  #[error("Invalid pong reponse")]
  InvalidPongResponse,
  #[error("get connection timeout")]
  PoolGetTimeout,
}

impl From<PoolError> for Error {
  fn from(e: PoolError) -> Self {
    match e {
      PoolError::GetTimeout => Error::PoolGetTimeout,
      PoolError::KanalSend(e) => Error::KanalSend(e),
      PoolError::KanalRecv(e) => Error::KanalRecv(e),
    }
  }
}

#[cfg(test)]
mod tests {
  use ctor::ctor;
  use tracing_subscriber::prelude::*;

  #[ctor]
  fn init_color_backtrace() {
    // let console_layer = console_subscriber::spawn();

    let env_filter = tracing_subscriber::EnvFilter::from_default_env();
    let subscriber = tracing_subscriber::fmt::layer()
      .pretty()
      .with_test_writer()
      .with_filter(env_filter);

    tracing_subscriber::registry()
      .with(subscriber)
      // .with(console_layer)
      .init();
    color_backtrace::install();
  }
}
