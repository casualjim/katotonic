# Katotonic ID Format — UUIDv8

Katotonic IDs are RFC 4122 UUIDs, **version 8** (custom). They are opaque, 16‑byte values whose ordering encodes the cluster‑wide `(term, seq)` order.

## Goals

- A single, formal on‑wire and stored ID format.
- Total ordering by raw byte lexicographic comparison.
- No reliance on wall‑clock time or legacy time‑based semantics.

## High‑Level Definition

- **Type:** RFC 4122 UUID, version **8**, variant **RFC4122**.
- **Variable payload bits:** 122 bits (all UUID bits except version and variant).
- **Payload meaning:** big‑endian concatenation of:
  - `term` — 48 bits (election epoch).
  - `seq` — 74 bits (monotonic counter within term).
- **Ordering:** lexicographic order of the raw 16 UUID bytes equals lexicographic order of `(term, seq)`.
- **Backwards compatibility:** legacy IDs are removed; mixed legacy/UUID clusters are unsupported.

## Bit Layout

Define UUID bits `b0..b127` as **network order**: `b0` is the most‑significant bit of byte 0, `b7` is the least‑significant bit of byte 0, etc.

Fixed bits:

- **Version:** bits **48–51** are `1000` (UUID v8).
- **Variant:** bits **64–65** are `10` (RFC4122 variant).

All other bits are variable payload bits, in increasing bit index order:

```
bits 0..47     : payload[0..47]    (term high → low)
bits 48..51    : 1000             (version)
bits 52..63    : payload[48..59]
bits 64..65    : 10               (variant)
bits 66..127   : payload[60..121] (seq remainder)
```

Payload construction:

```
payload = (term << 74) | seq
term: 48 bits, seq: 74 bits
```

## Encoding Algorithm

Given `term` and `seq`:

1. Validate ranges:
   - `term < 2^48`
   - `seq  < 2^74`
2. Build a 122‑bit payload `P = (term << 74) | seq`, interpreted big‑endian.
3. Start with 128‑bit zero UUID.
4. For UUID bit index `i` from 0 to 127:
   - Skip fixed positions `48..51` and `64..65`.
   - Otherwise take the next most‑significant bit of `P` and write it into UUID bit `i`.
5. After all payload bits are inserted, set fixed bits:
   - Set bits 48–51 to `1000`.
   - Set bits 64–65 to `10`.

This mapping ensures that comparing UUIDs as raw 16‑byte arrays corresponds to comparing `(term, seq)` lexicographically.

## Decoding Algorithm

Given an ID:

1. Optionally verify fixed bits match v8 + RFC4122 variant.
2. Read UUID bits `0..127` in order, skipping fixed positions `48..51` and `64..65`, and pack them into a 122‑bit payload `P` big‑endian.
3. Recover:
   - `term = P >> 74`
   - `seq  = P & ((1<<74) - 1)`

## Comparison Caveat (Important)

Some platforms/libraries (notably those exposing GUID field ordering) compare UUIDs by re‑ordering fields rather than raw bytes. Katotonic ordering **requires comparing the raw 16 bytes in network order**. Clients and storage systems must not use GUID field‑reordered comparisons.
