use futures::{SinkExt, StreamExt};
use std::{io, sync::Arc};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tokio_serde_cbor::Codec;
use tokio_util::codec::Decoder;
use tracing::{info, instrument};
use ulid::Ulid;
use ulidd::protocol::{Request, Response};

#[instrument(skip(stream))]
async fn handle_client(
  stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> ulidd::Result<()> {
  let codec: Codec<Request, Response> = tokio_serde_cbor::Codec::new();
  let (mut server_sender, mut server_receiver) = codec.framed(stream).split();

  while let Some(Ok(request)) = server_receiver.next().await {
    let response = match request {
      Request::NewId(id) => {
        let new_data = Ulid::new().to_bytes();
        Response::Id(id, new_data)
      }
      Request::Heartbeat => Response::HeartbeatAck,
    };
    server_sender.send(response).await.unwrap();
  }

  Ok(())
}

#[tokio::main]
async fn main() -> ulidd::Result<()> {
  tracing_subscriber::fmt::init();

  let conf = ulidd::ServerConfig::new()?;
  let (certs, private_key) = conf.keypair()?.expect("tls keypair not found");
  let cfg_builder = ServerConfig::builder();

  let cfg_builder = match conf.root_store()? {
    Some(root_store) => cfg_builder.with_client_cert_verifier(
      rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .expect("enable to build client verifier"),
    ),
    None => cfg_builder.with_no_client_auth(),
  };

  // Configure the server
  let config = Arc::new(
    cfg_builder
      .with_single_cert(certs, private_key)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
  );
  let acceptor = TlsAcceptor::from(config);

  // Bind to the address
  let addr = conf.addr()?;
  info!(%addr, "Listening");
  let listener = TcpListener::bind(addr).await?;

  loop {
    let (stream, _) = listener.accept().await?;
    let acceptor = acceptor.clone();

    // Spawn a task to handle the client
    tokio::spawn(async move {
      let stream = acceptor.accept(stream).await.unwrap();
      handle_client(stream).await.unwrap();
    });
  }
}
