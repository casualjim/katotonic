# Quorum‑Fenced UUIDv8 Sequencer — Implementation Task Plan

This plan is the concrete work breakdown for shipping quorum‑fenced sequencing with a single, formal UUIDv8 ID format. It is ordered for execution and progress tracking.

---

## Milestone 0 — Formal UUIDv8 ID Format + Remove ULID Everywhere

### M0.1 Write the ID spec doc
- [ ] Add `docs/id-format-uuidv8.md`.
- [ ] Specify:
  - [ ] ID is RFC 4122 UUID, **version 8**, variant RFC4122.
  - [ ] Payload ordering is `(term, seq)` big‑endian over **122 variable bits**.
  - [ ] Fixed bits: version bits (48–51) = `1000`, variant (64–65) = `10`.
  - [ ] `term` uses 48 bits, `seq` uses 74 bits.
  - [ ] Lexicographic order on raw 16 bytes == global order.
  - [ ] No wall‑clock or ULID semantics; IDs are opaque.
  - [ ] Backward compatibility policy: ULID removed; mixed clusters unsupported.
- [ ] Link this doc from `docs/quorum-fenced-sequencer.md` (replace ULID language).

**Acceptance**
- Spec is clear enough to reimplement in Rust/Go without guessing.
- Explicitly warns about GUID field‑reordered comparisons.

---

### M0.2 Add Rust UUIDv8 helpers
- [ ] Add `src/id.rs` and export it publicly.
- [ ] Implement:
  - [ ] `pub type KatotonicId = uuid::Uuid`
  - [ ] `pub fn encode_v8(term: u64, seq: u128) -> uuid::Uuid`
  - [ ] `pub fn decode_v8(id: uuid::Uuid) -> (u64, u128)`
  - [ ] `pub fn is_katotonic_v8(id: uuid::Uuid) -> bool`
- [ ] Big‑endian packing:
  - [ ] Fill variable bit positions with payload bits in increasing UUID bit index order.
  - [ ] Set version and variant bits after payload insertion.
- [ ] Add unit tests:
  - [ ] Round‑trip term/seq.
  - [ ] Fixed‑bit correctness.
  - [ ] Bytewise ordering matches `(term, seq)` ordering.

**Acceptance**
- `encode_v8` produces valid v8 UUIDs for all inputs in range.
- Sorting by `as_bytes()` is monotonic.

---

### M0.3 Replace `src/idgen.rs` ULID generator
- [ ] Remove `ulid::Ulid` usage and timestamp logic.
- [ ] Introduce a temporary local monotonic generator:
  - [ ] `static PREVIOUS_SEQ: AtomicU64`
  - [ ] `pub fn generate_monotonic_id(term: u64) -> KatotonicId`
- [ ] Update `src/lib.rs`:
  - [ ] `pub mod id;`
  - [ ] `pub use id::KatotonicId;`
  - [ ] Reexport the new generator with same public name.

**Acceptance**
- Crate compiles without ULID types in public API.

---

### M0.4 Update Rust tests to UUID semantics
- [ ] Convert tests in `src/idgen.rs`:
  - [ ] Uniqueness and strict monotonicity by bytewise ordering.
  - [ ] Remove timestamp assertions.
- [ ] Ensure concurrency test still validates global uniqueness/order.

**Acceptance**
- `cargo test` passes on this module.

---

### M0.5 Update Rust examples from ULID to UUID
- [ ] Files:
  - [ ] `examples/async-client.rs`
  - [ ] `examples/sync-client.rs`
  - [ ] `examples/smol-client.rs`
  - [ ] `examples/client.rs`
- [ ] Replace ULID imports/formatting with UUIDv8.
- [ ] Ordering checks compare raw bytes.

**Acceptance**
- Examples compile and their validation output is correct.

---

### M0.6 Update Go client library to UUIDv8
- [ ] Remove ULID dependency from `go-katotonic/go.mod`.
- [ ] Switch to UUID library that supports v8 (e.g., `github.com/google/uuid`).
- [ ] Update API:
  - [ ] `NextId() (uuid.UUID, error)` in `go-katotonic/api.go`.
  - [ ] Update `go-katotonic/client.go` accordingly.
- [ ] Add helpers mirroring Rust:
  - [ ] `EncodeV8(term uint64, seqHi uint64, seqLo uint64) uuid.UUID`
  - [ ] `DecodeV8(id uuid.UUID) (term uint64, seqHi uint64, seqLo uint64)`
  - [ ] (Public API remains opaque; helpers are for debug/tests.)
- [ ] Update Go example `go-katotonic/examples/client/main.go`.

**Acceptance**
- Go example builds and validates bytewise monotonicity.

---

## Milestone 1 — Quorum Persistent State

### M1.1 Create quorum module + state struct
- [ ] Add `src/quorum/mod.rs` exporting submodules.
- [ ] Add `src/quorum/state.rs`:
  - [ ] `pub struct QuorumState { current_term, voted_term, high_watermark, config_version }`
  - [ ] `load(path)`, `persist(path)` with atomic write + fsync.
  - [ ] Helpers for bumping terms and advancing watermark.
- [ ] Enforce invariants on load:
  - [ ] `voted_term >= current_term`
  - [ ] `high_watermark` is monotonic within term.

**Acceptance**
- Unit tests cover load/persist, crash‑safe update, and rollback prevention.

---

### M1.2 Wire quorum state into startup
- [ ] Load state at server boot in:
  - [ ] `src/bin/async-server.rs`
  - [ ] `src/bin/sync-server.rs`
  - [ ] `src/bin/smol-server.rs`
- [ ] Add config entries in `src/config.rs`:
  - [ ] `state_path`

**Acceptance**
- Restarting a node never decreases term or watermark.

---

## Milestone 2 — Quorum RPC Types + Cluster Transport

### M2.1 Define RPC enums
- [ ] Add `src/quorum/rpc.rs`:
  - [ ] `QuorumRequest` and `QuorumResponse` per design.
- [ ] Derive serde traits.

**Acceptance**
- All messages are CBOR‑serializable.

---

### M2.2 Stand up quorum RPC listener on cluster port
- [ ] Reuse TLS framing (`src/transport.rs`, `src/cbor_codec.rs`).
- [ ] Prefer cluster listener (based on `cluster_addr` tags) rather than API port.
- [ ] Add a handler that routes QuorumRequest -> QuorumResponse.

**Acceptance**
- A node can send a QuorumRequest to another node and receive a response over TLS.

---

## Milestone 3 — Election + Leader State Machine

### M3.1 Add LeaderState + watchable leader state
- [ ] Add `LeaderState` enum in `src/quorum/mod.rs`.
- [ ] Replace Bully `PeerState` for serving with `WatchableValue<LeaderState>`.

**Acceptance**
- Leader state transitions are visible to API handler.

---

### M3.2 Election triggers
- [ ] Use Chitchat liveness to trigger elections:
  - [ ] Suspect/dead leader
  - [ ] Join requiring reconfig
  - [ ] Lease loss (from M4)
- [ ] Drive transitions in server runtime (`src/server/tokio.rs`, `src/server/smol.rs`, blocking server).

**Acceptance**
- Elections start reliably on failure signals.

---

### M3.3 Implement election protocol
- [ ] Add `src/quorum/election.rs`:
  - [ ] Candidate increments term, self‑votes (persist voted_term).
  - [ ] Sends `VoteRequest` to live members `M`.
  - [ ] Follower rules:
    - [ ] Reject if `proposed_term <= voted_term`.
    - [ ] Else persist voted_term and reply yes with HW.
  - [ ] Win on majority.
  - [ ] On win persist current_term and set HW to max majority HW.
- [ ] Add timeouts/backoff jitter.

**Acceptance**
- In a 3‑node integration test, exactly one leader emerges per term.

---

## Milestone 4 — Lease Renewal / Fencing

### M4.1 Implement leader lease loop
- [ ] Add `src/quorum/lease.rs`.
- [ ] Leader sends `LeaseRenew` periodically and tracks majority acks.
- [ ] Maintain `lease_valid` flag in LeaderState.

**Acceptance**
- Lease validity is only true with majority ack and unexpired lease.

---

### M4.2 Implement follower lease rules
- [ ] On LeaseRenew:
  - [ ] Nack if `term < voted_term`.
  - [ ] If `term > current_term`, persist current_term/voted_term.
  - [ ] Ensure no dual‑leader ack for same term.
- [ ] Return LeaseAck/LeaseNack.

**Acceptance**
- Minority partition leaders cannot keep a lease.

---

### M4.3 Gate serving on lease
- [ ] Update `src/server/handler.rs`:
  - [ ] Serve IDs only if `LeaderState::Leader{lease_valid:true}`.
  - [ ] Otherwise redirect/hold.

**Acceptance**
- No IDs served without a valid majority lease.

---

## Milestone 5 — Block Reservation + Hot Path

### M5.1 Add block state to idgen
- [ ] Extend `src/idgen.rs`:
  - [ ] Track current term.
  - [ ] Track reserved block `[start_seq, end_seq]`.
  - [ ] `next_seq` atomic counter.
- [ ] Issue by atomic increment within block.
- [ ] Encode using `encode_v8`.

**Acceptance**
- Per‑process monotonicity with zero allocations on hot path.

---

### M5.2 Implement ReserveBlock quorum round
- [ ] Add `src/quorum/reserve.rs`:
  - [ ] Leader selects next block from HW.
  - [ ] Followers persist HW=max(HW,end).
  - [ ] Majority ack installs block on leader.
- [ ] Add config entry:
  - [ ] `block_size`

**Acceptance**
- Blocks never overlap, and leader always starts above majority HW after failover.

---

### M5.3 Integrate reserve into serving
- [ ] In `src/server/handler.rs`:
  - [ ] Reserve new block when exhausted before returning an ID.

**Acceptance**
- Throughput matches old ULID path aside from reservation intervals.

---

## Milestone 6 — Term‑Aware Redirects

### M6.1 Extend RedirectInfo with term
- [ ] Update struct in `src/server/mod.rs`.
- [ ] Update all serializers/deserializers:
  - [ ] Rust clients `src/client/tokio.rs`, `src/client/smol.rs`
  - [ ] Go client redirect parsing.

**Acceptance**
- Redirects carry term end‑to‑end.

---

### M6.2 Client term cache
- [ ] Rust clients:
  - [ ] Track `highest_term_seen`.
  - [ ] Ignore redirects to older terms.
- [ ] Go client:
  - [ ] Same logic.

**Acceptance**
- Clients converge on newest leader without oscillation.

---

## Milestone 7 — Joint Quorum Reconfiguration

### M7.1 Surface config_version in Chitchat
- [ ] Add `config_version` tag in `src/server/mod.rs::spawn_discovery`.
- [ ] Update `src/disco.rs` to expose membership/config versions.

**Acceptance**
- All nodes report consistent config_version via discovery.

---

### M7.2 Implement joint quorum rules
- [ ] Add `src/quorum/config.rs`:
  - [ ] Compute `C_old`, `C_joint`, `C_new`.
  - [ ] For 3‑node default: require 2 acks and ≥1 from old set until catchup complete.

**Acceptance**
- No disjoint majorities during rolling replace.

---

### M7.3 Catchup RPC for joiners
- [ ] Implement CatchupRequest/Response:
  - [ ] Leader sends current term/HW/config_version.
  - [ ] Joiner persists before voting/acking.
- [ ] Wire join flow into runtime.

**Acceptance**
- Joiner never participates with stale term or HW.

---

## Milestone 8 — Remove Bully Safety + Cleanup

### M8.1 Remove Bully from serving decisions
- [ ] Bully no longer controls leader in API path.
- [ ] Keep Bully only if still useful as a bootstrap trigger; otherwise delete.

**Acceptance**
- Safety relies solely on quorum election/lease.

---

### M8.2 Delete ULID remnants
- [ ] Remove ULID deps and types from:
  - [ ] `src/config.rs`
  - [ ] `src/client/*`
  - [ ] Go lib
  - [ ] Examples
- [ ] Update any docs still referencing ULID.

**Acceptance**
- No ULID references in `rg ULID|ulid`.

---

### M8.3 Final docs + perf notes
- [ ] Update `docs/quorum-fenced-sequencer.md` to be UUIDv8‑first.
- [ ] Add block/lease tuning section.

**Acceptance**
- Docs match implementation and defaults.

---

## Cross‑Cutting Configuration Defaults
- [ ] Add to `ServerConfig` in `src/config.rs`:
  - [ ] `state_path`
  - [ ] `lease_duration_ms` (default 1000)
  - [ ] `renew_interval_ms` (default 250)
  - [ ] `election_timeout_ms` randomized (default 800–1500)
  - [ ] `block_size` (default 100_000)
- [ ] Add client retry knobs if needed.

---

## Test Plan

### Unit tests
- [ ] `src/id.rs`: encode/decode, fixed bits, ordering.
- [ ] `src/idgen.rs`: monotonicity, uniqueness, concurrency, block boundaries.
- [ ] `src/quorum/state.rs`: persistence and invariants.
- [ ] `src/quorum/election.rs`: vote rules and term monotonicity.
- [ ] `src/quorum/lease.rs`: lease ack rules.
- [ ] `src/quorum/reserve.rs`: HW advancement and overlap prevention.

### Integration tests
- [ ] Add `tests/quorum_*.rs` that spin up 3 tokio servers.
- [ ] Scenarios:
  - [ ] steady‑state global monotonic ordering across nodes.
  - [ ] minority partition (cluster port cut, API reachable) → must not serve.
  - [ ] leader stall → lease expiry → new term elected → no regression.
  - [ ] rolling replace with joint quorum/catchup.

**Global Acceptance**
- `cargo test` passes.
- Clients sort IDs correctly using raw bytewise comparisons only.
- Under partitions and failover, no duplicates or regressions occur.

