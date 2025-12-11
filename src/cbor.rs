use std::io;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

pub type Result<T, E = CborError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum CborError {
  #[error("CBOR encode error: {0}")]
  Encode(#[from] ciborium::ser::Error<io::Error>),
  #[error("CBOR decode error: {0}")]
  Decode(#[from] ciborium::de::Error<io::Error>),
}

pub fn to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>> {
  let mut buffer = Vec::new();
  ciborium::ser::into_writer(value, &mut buffer)?;
  Ok(buffer)
}

pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
  let reader = io::Cursor::new(bytes);
  let value = ciborium::de::from_reader(reader)?;
  Ok(value)
}

