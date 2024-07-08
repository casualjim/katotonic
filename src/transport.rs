use std::{borrow::Cow, io, sync::Arc};

use async_trait::async_trait;
use futures::{SinkExt as _, StreamExt as _};
use serde::{Deserialize, Serialize};
use tokio::sync::{
  mpsc::{Receiver, Sender},
  RwLock,
};
use tokio_serde_cbor::Codec;
use tokio_util::codec::Decoder;

use crate::{Error, Result};

#[async_trait]
pub trait Transport: Send + Sync {
  type Request: Send + Sync + Serialize + for<'de> Deserialize<'de>;
  type Response: Send + Sync + Serialize + for<'de> Deserialize<'de>;

  async fn send(&self, msg: Self::Request) -> Result<Self::Response>;
}

pub struct ChannelTransport<I, O> {
  sender: tokio::sync::mpsc::Sender<I>,
  receiver: Arc<RwLock<tokio::sync::mpsc::Receiver<O>>>,
}

impl<I, O> ChannelTransport<I, O>
where
  I: Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  pub fn new(sender: Sender<I>, receiver: Receiver<O>) -> Self {
    Self {
      sender,
      receiver: Arc::new(RwLock::new(receiver)),
    }
  }
}

#[async_trait]
impl<I, O> Transport for ChannelTransport<I, O>
where
  I: Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  type Request = I;
  type Response = O;

  async fn send(&self, msg: Self::Request) -> Result<Self::Response> {
    if let Err(_) = self.sender.send(msg).await {
      return Err(Error::Io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "unexpected EOF",
      )));
    }
    let mut receiver = self.receiver.write().await;
    if let Some(response) = receiver.recv().await {
      Ok(response)
    } else {
      Err(Error::Io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "unexpected EOF",
      )))
    }
  }
}

pub struct TcpTransport<'a, I, O> {
  addr: Cow<'a, str>,
  codec: Codec<O, I>,
}

impl<'a, I, O> TcpTransport<'a, I, O>
where
  I: Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  pub fn new(addr: Cow<'a, str>) -> Self {
    Self {
      addr,
      codec: Codec::new(),
    }
  }

  pub fn with_codec(addr: Cow<'a, str>, codec: Codec<O, I>) -> Self {
    Self { addr, codec }
  }
}

#[async_trait]
impl<'a, I, O> Transport for TcpTransport<'a, I, O>
where
  I: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>,
  O: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
  type Request = I;
  type Response = O;

  async fn send(&self, req: Self::Request) -> Result<Self::Response> {
    let stream = tokio::net::TcpStream::connect(self.addr.as_ref()).await?;
    let codec = self.codec.clone();
    let (mut sender, mut receiver) = codec.framed(stream).split();
    sender.send(req).await?;
    if let Some(response) = receiver.next().await {
      Ok(response?)
    } else {
      Err(Error::Io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "unexpected EOF",
      )))
    }
  }
}

#[cfg(test)]
mod tests {
  use tokio::sync::{mpsc, oneshot};
  use tokio_serde_cbor::Codec;

  use super::*;

  #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
  struct TestRequest {
    value: String,
  }

  #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
  struct TestResponse {
    value: String,
  }

  #[tokio::test]
  async fn test_channel_transport() {
    let (req_tx, req_rx) = mpsc::channel(1);
    let (resp_tx, mut resp_rx) = mpsc::channel(1);
    let transport = ChannelTransport::new(resp_tx, req_rx);
    let msg = TestRequest {
      value: "test".to_string(),
    };

    let (response_value_tx, response_value_rx) = oneshot::channel();
    let msg_to_send = msg.clone();
    tokio::spawn(async move {
      let response = transport.send(msg_to_send).await;
      response_value_tx.send(response).unwrap();
    });

    assert_eq!(msg, resp_rx.recv().await.unwrap());
    req_tx
      .send(TestResponse {
        value: "1".to_string(),
      })
      .await
      .unwrap();
    assert_eq!(
      response_value_rx.await.unwrap().unwrap(),
      TestResponse {
        value: "1".to_string(),
      }
    );
  }

  #[tokio::test]
  async fn test_tcp_transport() {
    let addr = "127.0.0.1:0".to_string();
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
      let (stream, _) = listener.accept().await.unwrap();
      let (mut sender, mut receiver) = Codec::<TestRequest, TestResponse>::new()
        .framed(stream)
        .split();
      if let Some(req) = receiver.next().await {
        let req = req.unwrap();
        assert_eq!(req.value, "test");
        sender
          .send(TestResponse {
            value: req.value.to_string(),
          })
          .await
          .unwrap();
      }
    });

    let codec = Codec::<TestResponse, TestRequest>::new();
    let transport = TcpTransport::with_codec(Cow::Owned(addr.to_string()), codec);
    let msg = TestRequest {
      value: "test".to_string(),
    };
    let response = transport.send(msg).await.unwrap();
    assert_eq!(
      response,
      TestResponse {
        value: "test".to_string()
      }
    );
  }
}
