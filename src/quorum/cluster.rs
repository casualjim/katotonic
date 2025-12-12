use std::sync::Arc;

use futures::{SinkExt, StreamExt as _};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Decoder;
use tracing::{error, info};

use super::{QuorumRequest, QuorumResponse, QuorumService};
use crate::{cbor_codec::Codec, server_tls_config, Result, ServerConfig};

pub async fn run_cluster_server(conf: ServerConfig, service: Arc<QuorumService>) -> Result<()> {
  let codec: Codec<QuorumRequest, QuorumResponse> = Codec::new();
  let config = Arc::new(server_tls_config(
    &conf.cluster_cert,
    &conf.cluster_key,
    Some(&conf.cluster_ca),
  )?);
  let acceptor = TlsAcceptor::from(config);

  let Some(addr) = conf.cluster_addr.clone() else {
    info!("cluster_addr not set; quorum cluster server disabled");
    return Ok(());
  };
  info!(%addr, "quorum cluster listening");

  let listener = TcpListener::bind(addr).await?;
  loop {
    let (stream, _) = listener.accept().await?;
    let acceptor = acceptor.clone();
    let codec = codec.clone();
    let service = Arc::clone(&service);
    tokio::spawn(async move {
      match acceptor.accept(stream).await {
        Ok(stream) => {
          let (mut sender, mut receiver) = codec.framed(stream).split();
          while let Some(maybe_req) = receiver.next().await {
            match maybe_req {
              Ok(req) => {
                let resp = service.handle(req).await;
                if let Err(e) = sender.send(resp).await {
                  error!("failed to send quorum response: {e}");
                  break;
                }
              }
              Err(e) => {
                error!("failed to decode quorum request: {e}");
              }
            }
          }
        }
        Err(e) => {
          error!("TLS accept error on quorum cluster port: {e}");
        }
      }
    });
  }
}
