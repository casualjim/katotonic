use std::sync::Arc;

use async_trait::async_trait;
use futures::{Future, SinkExt as _, StreamExt as _};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_serde_cbor::Codec;
use tokio_util::codec::Decoder;
use tracing::{info, instrument};

use crate::{
  protocol::{Request, Response},
  server_tls_config, Result, ServerConfig,
};

#[async_trait]
pub trait Handler: Send + Sync {
  async fn handle(&self, request: Request) -> Result<Response>;
}

#[async_trait]
impl<F, Fut> Handler for F
where
  F: Fn(Request) -> Fut + Send + Sync,
  Fut: Future<Output = Result<Response>> + Send,
{
  async fn handle(&self, request: Request) -> Result<Response> {
    self(request).await
  }
}

#[instrument(skip(stream, handler))]
async fn handle_client<H>(
  stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
  handler: Arc<H>,
) -> Result<()>
where
  H: Handler + 'static,
{
  let codec: Codec<Request, Response> = tokio_serde_cbor::Codec::new();
  let (mut server_sender, mut server_receiver) = codec.framed(stream).split();

  while let Some(Ok(request)) = server_receiver.next().await {
    let request_id = match &request {
      Request::NewId(id) => *id,
      Request::Heartbeat => 0,
    };
    match handler.handle(request).await {
      Ok(response) => {
        server_sender.send(response).await.unwrap();
      }
      Err(e) => {
        server_sender
          .send(Response::Error(request_id, e.to_string()))
          .await
          .unwrap();
      }
    }
  }

  Ok(())
}

pub async fn run<H>(conf: ServerConfig, handler: H) -> Result<()>
where
  H: Handler + 'static,
{
  let config = Arc::new(server_tls_config(&conf.cert, &conf.key, conf.ca.as_ref())?);
  let acceptor = TlsAcceptor::from(config);

  // Bind to the address
  let addr = conf.addr()?;
  info!(%addr, "Listening");
  let listener = TcpListener::bind(addr).await?;

  let handler = Arc::new(handler);

  loop {
    let (stream, _) = listener.accept().await?;
    let acceptor = acceptor.clone();

    // Spawn a task to handle the client
    // let handler = Arc::new(move |request| Box::pin(hh(request)));
    let handler = handler.clone();
    tokio::spawn(async move {
      let stream = acceptor.accept(stream).await.unwrap();
      handle_client(stream, handler).await.unwrap();
    });
  }
}
