use serde::{Deserialize, Serialize};

use std::{borrow::Cow, sync::Arc};

use rustls::pki_types::ServerName;

use crate::{
  cbor_codec::Codec,
  transport::{TlsTransport, Transport},
  Result,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QuorumRequest {
  Ping,

  VoteRequest {
    proposed_term: u64,
    candidate_id: String,
  },

  LeaseRenew {
    term: u64,
    leader_id: String,
    lease_until_ms: u64,
  },

  ReserveBlock {
    term: u64,
    start_seq: u64,
    end_seq: u64,
  },

  CatchupRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QuorumResponse {
  Pong,

  VoteYes {
    term: u64,
    high_watermark: u64,
    config_version: u64,
  },
  VoteNo {
    term: u64,
    voted_term: u64,
    high_watermark: u64,
    config_version: u64,
  },

  LeaseAck {
    term: u64,
    high_watermark: u64,
  },
  LeaseNack {
    term: u64,
    voted_term: u64,
  },

  ReserveAck {
    term: u64,
    high_watermark: u64,
  },
  ReserveNack {
    term: u64,
    high_watermark: u64,
  },

  CatchupResponse {
    term: u64,
    high_watermark: u64,
    config_version: u64,
  },

  Error {
    code: u16,
    message: String,
  },
}

#[derive(Clone)]
pub struct QuorumPeer {
  transport: Arc<dyn Transport<Request = QuorumRequest, Response = QuorumResponse>>,
}

impl QuorumPeer {
  pub fn new_tls(
    addr: Cow<'static, str>,
    client_config: rustls::ClientConfig,
    server_name: ServerName<'static>,
  ) -> Self {
    let transport = TlsTransport::with_codec(
      addr,
      client_config,
      server_name,
      Codec::<QuorumResponse, QuorumRequest>::new(),
    );
    Self {
      transport: Arc::new(transport),
    }
  }

  pub async fn send(&self, request: QuorumRequest) -> Result<QuorumResponse> {
    self.transport.send(request).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_cbor_roundtrip_requests_and_responses() {
    let requests = vec![
      QuorumRequest::Ping,
      QuorumRequest::VoteRequest {
        proposed_term: 2,
        candidate_id: "n1".to_string(),
      },
      QuorumRequest::LeaseRenew {
        term: 3,
        leader_id: "leader".to_string(),
        lease_until_ms: 1234,
      },
      QuorumRequest::ReserveBlock {
        term: 3,
        start_seq: 10,
        end_seq: 20,
      },
      QuorumRequest::CatchupRequest,
    ];

    for req in requests {
      let bytes = crate::cbor::to_vec(&req).unwrap();
      let decoded: QuorumRequest = crate::cbor::from_slice(&bytes).unwrap();
      assert_eq!(req, decoded);
    }

    let responses = vec![
      QuorumResponse::Pong,
      QuorumResponse::VoteYes {
        term: 2,
        high_watermark: 100,
        config_version: 1,
      },
      QuorumResponse::VoteNo {
        term: 2,
        voted_term: 2,
        high_watermark: 100,
        config_version: 1,
      },
      QuorumResponse::LeaseAck {
        term: 3,
        high_watermark: 200,
      },
      QuorumResponse::ReserveAck {
        term: 3,
        high_watermark: 250,
      },
      QuorumResponse::CatchupResponse {
        term: 3,
        high_watermark: 250,
        config_version: 2,
      },
      QuorumResponse::Error {
        code: 501,
        message: "not implemented".to_string(),
      },
    ];

    for resp in responses {
      let bytes = crate::cbor::to_vec(&resp).unwrap();
      let decoded: QuorumResponse = crate::cbor::from_slice(&bytes).unwrap();
      assert_eq!(resp, decoded);
    }
  }
}
