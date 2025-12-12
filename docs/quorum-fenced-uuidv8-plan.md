# Quorum‑Fenced UUIDv8 Sequencer — Implementation Task Plan

This plan is the concrete work breakdown for shipping quorum‑fenced sequencing with a single, formal UUIDv8 ID format. It is ordered for execution and progress tracking.

---

## Implementation Status (as of 2025‑12‑12)

Completed in this repo:
- Milestone 0 (Rust + Go + docs): UUIDv8 formal format, helpers, monotonic generator, tests, and examples migrated across Rust and Go.
- Milestone 1: Durable per‑node quorum summary (`current_term`, `voted_term`, `high_watermark`, `config_version`) with atomic persist + fsync, and server startup wiring.
- Milestone 2: Quorum RPC types and cluster‑port TLS transport. Quorum cluster listener runs independently.
- Milestone 3 (core election): `LeaderState` added, quorum election loop implemented, and serving gated on quorum leader state.
- Milestone 4 (lease fencing): leader lease renewal loop, follower lease rules, and serving gated on majority lease.
- Milestone 5 (block reservation): quorum block reservation round, hot‑path block issuance, and serving integrated with reservation.
- Milestone 6 (term‑aware redirects): redirects carry quorum term; Rust and Go clients ignore stale‑term redirects.
- Milestone 7 (joint quorum reconfiguration): `config_version` tags surfaced via Chitchat/Discovery, joint‑quorum rules enforced, Catchup RPC wired, and leader‑side config commit/bump implemented.
- Milestone 8 (cleanup): Bully removed from serving decisions, legacy ID remnants purged, and docs/tuning updated.

Key learnings/decisions:
- Persisted quorum state is a tiny leadership/ordering summary, written on elections/leases/block reservations; **no disk I/O on the per‑ID hot path**.
- Persistence is required for safety under crash‑recovery + partitions; diskless variants require relaxing guarantees or changing the failure model.
- Lease fencing (M4) and block reservation (M5) are implemented; IDs are served only with a valid majority lease and are issued from reserved blocks.
- Joint‑quorum gating is driven by `config_version` tags; when a higher version appears, quorum operations require majorities of both old and joint sets until catchup converges.
- `cargo test` and `go test ./...` pass as of 2025‑12‑12.

## Milestone 0 — Formal UUIDv8 ID Format + Remove Legacy IDs Everywhere

### M0.1 Write the ID spec doc
- [x] Add `docs/id-format-uuidv8.md`.
- [x] Specify:
  - [x] ID is RFC 4122 UUID, **version 8**, variant RFC4122.
  - [x] Payload ordering is `(term, seq)` big‑endian over **122 variable bits**.
  - [x] Fixed bits: version bits (48–51) = `1000`, variant (64–65) = `10`.
  - [x] `term` uses 48 bits, `seq` uses 74 bits.
  - [x] Lexicographic order on raw 16 bytes == global order.
  - [x] No wall‑clock or legacy time‑based semantics; IDs are opaque.
  - [x] Backward compatibility policy: legacy IDs removed; mixed clusters unsupported.
- [x] Link this doc from `docs/quorum-fenced-sequencer.md` (replace legacy language).

**Acceptance**
- Spec is clear enough to reimplement in Rust/Go without guessing.
- Explicitly warns about GUID field‑reordered comparisons.

---

### M0.2 Add Rust UUIDv8 helpers
- [x] Add `src/id.rs` and export it publicly.
- [x] Implement:
  - [x] `pub type KatotonicId = uuid::Uuid`
  - [x] `pub fn encode_v8(term: u64, seq: u128) -> uuid::Uuid`
  - [x] `pub fn decode_v8(id: uuid::Uuid) -> (u64, u128)`
  - [x] `pub fn is_katotonic_v8(id: uuid::Uuid) -> bool`
- [x] Big‑endian packing:
  - [x] Fill variable bit positions with payload bits in increasing UUID bit index order.
  - [x] Set version and variant bits after payload insertion.
- [x] Add unit tests:
  - [x] Round‑trip term/seq.
  - [x] Fixed‑bit correctness.
  - [x] Bytewise ordering matches `(term, seq)` ordering.

**Acceptance**
- `encode_v8` produces valid v8 UUIDs for all inputs in range.
- Sorting by `as_bytes()` is monotonic.

---

### M0.3 Replace `src/idgen.rs` legacy generator
- [x] Remove legacy ID crate usage and timestamp logic.
- [x] Introduce a temporary local monotonic generator:
  - [x] `static PREVIOUS_SEQ: AtomicU64`
  - [x] `pub fn generate_monotonic_id(term: u64) -> KatotonicId`
- [x] Update `src/lib.rs`:
  - [x] `pub mod id;`
  - [x] `pub use id::KatotonicId;`
  - [x] Reexport the new generator with same public name.

**Acceptance**
- Crate compiles without legacy ID types in public API.

---

### M0.4 Update Rust tests to UUID semantics
- [x] Convert tests in `src/idgen.rs`:
  - [x] Uniqueness and strict monotonicity by bytewise ordering.
  - [x] Remove timestamp assertions.
- [x] Ensure concurrency test still validates global uniqueness/order.

**Acceptance**
- `cargo test` passes on this module.

---

### M0.5 Update Rust examples from legacy IDs to UUID
- [x] Files:
  - [x] `examples/async-client.rs`
  - [x] `examples/sync-client.rs`
  - [x] `examples/smol-client.rs`
  - [x] `examples/client.rs`
- [x] Replace legacy imports/formatting with UUIDv8.
- [x] Ordering checks compare raw bytes.

**Acceptance**
- Examples compile and their validation output is correct.

---

### M0.6 Update Go client library to UUIDv8
- [x] Remove legacy dependency from `go-katotonic/go.mod`.
- [x] Switch to UUID library that supports v8 (e.g., `github.com/google/uuid`).
- [x] Update API:
  - [x] `NextId() (uuid.UUID, error)` in `go-katotonic/api.go`.
  - [x] Update `go-katotonic/client.go` accordingly.
- [x] Add helpers mirroring Rust:
  - [x] `EncodeV8(term uint64, seqHi uint64, seqLo uint64) uuid.UUID`
  - [x] `DecodeV8(id uuid.UUID) (term uint64, seqHi uint64, seqLo uint64)`
  - [x] (Public API remains opaque; helpers are for debug/tests.)
- [x] Update Go example `go-katotonic/examples/client/main.go`.

**Acceptance**
- Go example builds and validates bytewise monotonicity.

---

## Milestone 1 — Quorum Persistent State

### M1.1 Create quorum module + state struct
- [x] Add `src/quorum/mod.rs` exporting submodules.
- [x] Add `src/quorum/state.rs`:
  - [x] `pub struct QuorumState { current_term, voted_term, high_watermark, config_version }`
  - [x] `load(path)`, `persist(path)` with atomic write + fsync.
  - [x] Helpers for bumping terms and advancing watermark.
- [x] Enforce invariants on load:
  - [x] `voted_term >= current_term`
  - [x] `high_watermark` is monotonic within term.

**Acceptance**
- Unit tests cover load/persist, crash‑safe update, and rollback prevention.

---

### M1.2 Wire quorum state into startup
- [x] Load state at server boot in:
  - [x] `src/bin/async-server.rs`
  - [x] `src/bin/sync-server.rs`
  - [x] `src/bin/smol-server.rs`
- [x] Add config entries in `src/config.rs`:
  - [x] `state_path`

**Acceptance**
- Restarting a node never decreases term or watermark.

---

## Milestone 2 — Quorum RPC Types + Cluster Transport

### M2.1 Define RPC enums
- [x] Add `src/quorum/rpc.rs`:
  - [x] `QuorumRequest` and `QuorumResponse` per design.
- [x] Derive serde traits.

**Acceptance**
- All messages are CBOR‑serializable.

---

### M2.2 Stand up quorum RPC listener on cluster port
- [x] Reuse TLS framing (`src/transport.rs`, `src/cbor_codec.rs`).
- [x] Prefer cluster listener (based on `cluster_addr` tags) rather than API port.
- [x] Add a handler that routes QuorumRequest -> QuorumResponse.

**Acceptance**
- A node can send a QuorumRequest to another node and receive a response over TLS.

---

## Milestone 3 — Election + Leader State Machine

### M3.1 Add LeaderState + watchable leader state
- [x] Add `LeaderState` enum in `src/quorum/mod.rs`.
- [x] Replace Bully `PeerState` for serving with `WatchableValue<LeaderState>`.

**Acceptance**
- Leader state transitions are visible to API handler.

---

### M3.2 Election triggers
- [ ] Use Chitchat liveness to trigger elections:
  - [x] Suspect/dead leader (triggered when leader disappears from membership stream)
  - [ ] Join requiring reconfig
  - [x] Lease loss (from M4)
- [x] Drive transitions in server runtime (`src/server/tokio.rs`, `src/server/smol.rs`, blocking server).

**Acceptance**
- Elections start reliably on failure signals.

---

### M3.3 Implement election protocol
- [x] Add `src/quorum/election.rs`:
  - [x] Candidate increments term, self‑votes (persist voted_term).
  - [x] Sends `VoteRequest` to live members `M`.
  - [x] Follower rules:
    - [x] Reject if `proposed_term <= voted_term`.
    - [x] Else persist voted_term and reply yes with HW.
  - [x] Win on majority.
  - [x] On win persist current_term and set HW to max majority HW.
- [x] Add timeouts/backoff jitter.

**Acceptance**
- In a 3‑node integration test, exactly one leader emerges per term.
  - Status (2025‑12‑12): unit tests pass; 3‑node integration test still pending.

---

## Milestone 4 — Lease Renewal / Fencing

### M4.1 Implement leader lease loop
- [x] Add `src/quorum/lease.rs`.
- [x] Leader sends `LeaseRenew` periodically and tracks majority acks.
- [x] Maintain `lease_valid` flag in LeaderState.

**Acceptance**
- Lease validity is only true with majority ack and unexpired lease.

---

### M4.2 Implement follower lease rules
- [x] On LeaseRenew:
  - [x] Nack if `term < voted_term`.
  - [x] If `term > current_term`, persist current_term/voted_term.
  - [x] Ensure no dual‑leader ack for same term.
- [x] Return LeaseAck/LeaseNack.

**Acceptance**
- Minority partition leaders cannot keep a lease.

---

### M4.3 Gate serving on lease
- [x] Update `src/server/handler.rs`:
  - [x] Serve IDs only if `LeaderState::Leader{lease_valid:true}`.
  - [x] Otherwise redirect/hold.

**Acceptance**
- No IDs served without a valid majority lease.

---

## Milestone 5 — Block Reservation + Hot Path

### M5.1 Add block state to idgen
- [x] Extend `src/idgen.rs`:
  - [x] Track current term.
  - [x] Track reserved block `[start_seq, end_seq]`.
  - [x] `next_seq` atomic counter.
- [x] Issue by atomic increment within block.
- [x] Encode using `encode_v8`.

**Acceptance**
- Per‑process monotonicity with zero allocations on hot path.

---

### M5.2 Implement ReserveBlock quorum round
- [x] Add `src/quorum/reserve.rs`:
  - [x] Leader selects next block from HW.
  - [x] Followers persist HW=max(HW,end).
  - [x] Majority ack installs block on leader.
- [x] Add config entry:
  - [x] `block_size`

**Acceptance**
- Blocks never overlap, and leader always starts above majority HW after failover.

---

### M5.3 Integrate reserve into serving
- [x] In `src/server/handler.rs`:
  - [x] Reserve new block when exhausted before returning an ID.

**Acceptance**
- Throughput matches old legacy path aside from reservation intervals.

---

## Milestone 6 — Term‑Aware Redirects

### M6.1 Extend RedirectInfo with term
- [x] Update struct in `src/server/mod.rs`.
- [x] Update all serializers/deserializers:
  - [x] Rust clients `src/client/tokio.rs`, `src/client/smol.rs`
  - [x] Go client redirect parsing.

**Acceptance**
- Redirects carry term end‑to‑end.

---

### M6.2 Client term cache
- [x] Rust clients:
  - [x] Track `highest_term_seen`.
  - [x] Ignore redirects to older terms.
- [x] Go client:
  - [x] Same logic.

**Acceptance**
- Clients converge on newest leader without oscillation.

---

## Milestone 7 — Joint Quorum Reconfiguration

### M7.1 Surface config_version in Chitchat
- [x] Add `config_version` tag in `src/server/mod.rs::spawn_discovery`.
- [x] Update `src/disco.rs` to expose membership/config versions.

**Acceptance**
- All nodes report consistent config_version via discovery.

---

### M7.2 Implement joint quorum rules
- [x] Add `src/quorum/config.rs`:
  - [x] Compute old vs joint sets from `config_version` tags.
  - [x] Require joint‑majority and old‑majority for elections, leases, and reservations until catchup converges.

**Acceptance**
- No disjoint majorities during rolling replace.

---

### M7.3 Catchup RPC for joiners
- [x] Implement CatchupRequest/Response:
  - [x] Leader sends current term/HW/config_version.
  - [x] Joiner persists before voting/acking (`install_catchup`).
- [x] Wire catchup and joint‑quorum triggers into membership monitor.

**Acceptance**
- Joiner never participates with stale term or HW.

**Notes**
- [x] Leader‑side config commit/bump of `config_version` and re‑tagging self in Chitchat.

---

## Milestone 8 — Remove Bully Safety + Cleanup

### M8.1 Remove Bully from serving decisions
- [x] Bully no longer controls leader in API path.
- [x] Bully retained only as an optional legacy/diagnostic module (not wired into binaries).

**Acceptance**
- Safety relies solely on quorum election/lease.

---

### M8.2 Delete legacy ID remnants
- [x] Remove legacy ID deps and types from:
  - [x] `src/config.rs`
  - [x] `src/client/*`
  - [x] Go lib
  - [x] Examples
- [x] Update any docs still referencing legacy IDs.

**Acceptance**
- No legacy ID references in repo search.

---

### M8.3 Final docs + perf notes
- [x] Update `docs/quorum-fenced-sequencer.md` to be UUIDv8‑first.
- [x] Add block/lease tuning section.

**Acceptance**
- Docs match implementation and defaults.

---

## Cross‑Cutting Configuration Defaults
- [x] Add to `ServerConfig` in `src/config.rs`:
  - [x] `state_path`
  - [x] `lease_duration_ms` (default 1000)
  - [x] `renew_interval_ms` (default 250)
  - [x] `election_timeout_min_ms` / `election_timeout_max_ms` randomized (defaults 800–1500)
  - [x] `block_size` (default 100_000)
- [ ] Add client retry knobs if needed.

---

## Test Plan

### Unit tests
- [x] `src/id.rs`: encode/decode, fixed bits, ordering.
- [x] `src/idgen.rs`: monotonicity, uniqueness, concurrency, block boundaries.
- [x] `src/quorum/state.rs`: persistence and invariants.
- [x] `src/quorum/election.rs`: vote rules and term monotonicity.
- [x] `src/quorum/lease.rs`: lease ack rules.
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
