pub mod cluster;
mod config;
mod ulidd;
use std::io;

pub use config::*;
use thiserror::Error;
pub use ulidd::*;

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
}
