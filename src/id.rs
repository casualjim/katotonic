use uuid::Uuid;

pub type KatotonicId = Uuid;

const TERM_BITS: u32 = 48;
const SEQ_BITS: u32 = 74;
const PAYLOAD_BITS: u32 = TERM_BITS + SEQ_BITS; // 122

const TERM_MASK: u64 = (1u64 << TERM_BITS) - 1;
const SEQ_MASK: u128 = (1u128 << SEQ_BITS) - 1;

const FIXED_VERSION_START: u32 = 48;
const FIXED_VERSION_END: u32 = 51;
const FIXED_VARIANT_BITS: [u32; 2] = [64, 65];

#[inline]
fn is_fixed_bit(bit_index: u32) -> bool {
  (FIXED_VERSION_START..=FIXED_VERSION_END).contains(&bit_index)
    || FIXED_VARIANT_BITS.contains(&bit_index)
}

#[inline]
fn set_uuid_bit(bytes: &mut [u8; 16], bit_index: u32, value: bool) {
  if !value {
    return;
  }
  let byte_index = (bit_index / 8) as usize;
  let bit_in_byte = 7 - (bit_index % 8);
  bytes[byte_index] |= 1u8 << bit_in_byte;
}

#[inline]
fn get_uuid_bit(bytes: &[u8; 16], bit_index: u32) -> bool {
  let byte_index = (bit_index / 8) as usize;
  let bit_in_byte = 7 - (bit_index % 8);
  (bytes[byte_index] & (1u8 << bit_in_byte)) != 0
}

/// Encode `(term, seq)` into a RFC4122 UUIDv8 Katotonic ID.
///
/// Ordering of the raw 16 bytes equals ordering of `(term, seq)`.
pub fn encode_v8(term: u64, seq: u128) -> Uuid {
  debug_assert!(term <= TERM_MASK, "term out of range (48 bits)");
  debug_assert!(seq <= SEQ_MASK, "seq out of range (74 bits)");

  let term = term & TERM_MASK;
  let seq = seq & SEQ_MASK;

  let payload: u128 = ((term as u128) << SEQ_BITS) | seq;

  let mut bytes = [0u8; 16];
  let mut written = 0u32;

  for uuid_bit in 0u32..128u32 {
    if is_fixed_bit(uuid_bit) {
      continue;
    }
    let payload_bit_index = PAYLOAD_BITS - 1 - written;
    let bit_set = ((payload >> payload_bit_index) & 1) == 1;
    set_uuid_bit(&mut bytes, uuid_bit, bit_set);
    written += 1;
  }

  debug_assert_eq!(written, PAYLOAD_BITS);

  Uuid::new_v8(bytes)
}

/// Decode a Katotonic UUIDv8 into `(term, seq)`.
pub fn decode_v8(id: Uuid) -> (u64, u128) {
  let bytes = *id.as_bytes();
  let mut payload: u128 = 0;

  for uuid_bit in 0u32..128u32 {
    if is_fixed_bit(uuid_bit) {
      continue;
    }
    payload = (payload << 1) | (get_uuid_bit(&bytes, uuid_bit) as u128);
  }

  let term = (payload >> SEQ_BITS) as u64;
  let seq = payload & SEQ_MASK;
  (term, seq)
}

/// Returns true if the UUID has version 8 and RFC4122 variant.
pub fn is_katotonic_v8(id: Uuid) -> bool {
  let bytes = id.as_bytes();
  (bytes[6] & 0xF0) == 0x80 && (bytes[8] & 0xC0) == 0x80
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_round_trip_encode_decode() {
    let cases: &[(u64, u128)] = &[
      (0, 0),
      (1, 1),
      (42, 999),
      (0x1234_5678_9ABC, 0x1_2345_6789_ABCDu128),
      (TERM_MASK, SEQ_MASK),
    ];

    for (term, seq) in cases {
      let id = encode_v8(*term, *seq);
      let (decoded_term, decoded_seq) = decode_v8(id);
      assert_eq!((*term & TERM_MASK), decoded_term);
      assert_eq!((*seq & SEQ_MASK), decoded_seq);
      assert!(is_katotonic_v8(id));
    }
  }

  #[test]
  fn test_fixed_bits() {
    let id = encode_v8(7, 123456);
    let bytes = id.as_bytes();
    assert_eq!(bytes[6] >> 4, 0x8, "version nibble should be 8");
    assert_eq!(bytes[8] & 0xC0, 0x80, "variant should be RFC4122");
  }

  #[test]
  fn test_bytewise_order_matches_term_seq() {
    let tuples: Vec<(u64, u128)> = vec![
      (0, 0),
      (0, 1),
      (0, 10),
      (1, 0),
      (1, 1),
      (2, 0),
      (2, (1u128 << 73) - 1),
      (TERM_MASK, 0),
      (TERM_MASK, SEQ_MASK),
    ];

    let ids: Vec<Uuid> = tuples.iter().map(|(t, s)| encode_v8(*t, *s)).collect();
    let mut sorted = ids.clone();
    sorted.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(ids, sorted);
  }
}
