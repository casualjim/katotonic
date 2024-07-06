pub mod client;
mod config;
pub mod disco;
pub mod leadership;
pub mod protocol;
pub mod server;
// mod ulidd;
use std::io;

pub use config::*;
use thiserror::Error;
use ulid::Ulid;

// pub use ulidd::*;

#[async_trait::async_trait]
pub trait IdGenerator {
  async fn next_id(&self) -> Result<Ulid>;
}

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
}

#[cfg(test)]
mod tests {
  use ctor::ctor;

  #[ctor]
  fn init_color_backtrace() {
    let subscriber = tracing_subscriber::fmt::fmt()
      .with_max_level(tracing::Level::ERROR)
      .with_test_writer()
      .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");
    color_backtrace::install();
  }
}
