use tracing::instrument;
use ulid::Ulid;
use ulidd::{
  protocol::{Request, Response},
  server,
};

#[instrument]
async fn handler(request: Request) -> ulidd::Result<Response> {
  let response = match request {
    Request::NewId(id) => {
      let new_data = Ulid::new().to_bytes();
      Response::Id(id, new_data)
    }
    Request::Heartbeat => Response::HeartbeatAck,
  };
  Ok(response)
}

#[tokio::main]
async fn main() -> ulidd::Result<()> {
  tracing_subscriber::fmt::init();

  let conf = ulidd::ServerConfig::new()?;

  server::run(conf, handler).await?;
  Ok(())
}
