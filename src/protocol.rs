use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
  NewId(u64),
  Heartbeat,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
  Id(u64, [u8; 16]),
  HeartbeatAck,
  Error(u64, String),
}

impl Response {
  pub(crate) fn get_request_id(&self) -> u64 {
    match self {
      Response::Id(id, _) => *id,
      Response::HeartbeatAck => 0,
      Response::Error(id, _) => *id,
    }
  }
}
