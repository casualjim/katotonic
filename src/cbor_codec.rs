use std::{io, marker::PhantomData};

use bytes::{Bytes, BytesMut};
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::{cbor, Error};

#[derive(Clone, Debug)]
pub struct Codec<I, O> {
  inner: LengthDelimitedCodec,
  _phantom: PhantomData<(I, O)>,
}

impl<I, O> Codec<I, O> {
  pub fn new() -> Self {
    Self {
      inner: LengthDelimitedCodec::new(),
      _phantom: PhantomData,
    }
  }

  pub fn with_max_frame_length(max_frame_length: usize) -> Self {
    let mut inner = LengthDelimitedCodec::new();
    inner.set_max_frame_length(max_frame_length);
    Self {
      inner,
      _phantom: PhantomData,
    }
  }
}

impl<I, O> Default for Codec<I, O> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, O> Decoder for Codec<I, O>
where
  I: DeserializeOwned,
{
  type Item = I;
  type Error = Error;

  fn decode(&mut self, src: &mut BytesMut) -> std::result::Result<Option<Self::Item>, Self::Error> {
    let maybe_frame = self.inner.decode(src).map_err(Error::from)?;
    match maybe_frame {
      Some(frame) => {
        let item = cbor::from_slice(frame.as_ref())?;
        Ok(Some(item))
      }
      None => Ok(None),
    }
  }
}

impl<I, O> Encoder<O> for Codec<I, O>
where
  O: Serialize,
{
  type Error = Error;

  fn encode(
    &mut self,
    item: O,
    dst: &mut BytesMut,
  ) -> std::result::Result<(), Self::Error> {
    let bytes = cbor::to_vec(&item)?;
    self
      .inner
      .encode(Bytes::from(bytes), dst)
      .map_err(|e| Error::Io(io::Error::new(e.kind(), e.to_string())))?;
    Ok(())
  }
}

