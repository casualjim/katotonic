# Katotonic: Quorum‑Fenced Monotonic Sequencer (Design)

## Summary

Katotonic today provides a high‑throughput monotonic UUIDv8 service by electing a leader (Bully) and issuing IDs only from that leader. This is fast and works well under stable conditions, but it does **not** provide datacenter‑grade safety: Bully + gossip can produce overlapping leaders during partitions or transient slowness, and leader failover can regress ordering.

This document proposes a minimal consensus layer that **does not replicate a log**, but still provides the safety properties required for Katotonic to replace a Raft “log position / term” agreement. The design keeps Chitchat for membership/reconfiguration/failure detection, while adding a quorum‑fenced `term` and `high_watermark` counter (with optional block leasing) for the ID data plane.

## Goals

1. **Global strict monotonicity and uniqueness** of issued IDs for the cluster, even under:
   - node crashes
   - transient stalls/GC pauses
   - network partitions (minority side may remain reachable by clients)
   - rolling replacement / reconfiguration
2. **High throughput** comparable to or better than current leader‑only issuance.
3. **No replicated log.** Replicate only the minimal state required to fence leadership and advance a monotonic counter.
4. **Dynamic membership** via Chitchat without requiring full cluster restarts.
5. Minimal changes to the existing hot path where possible.

## Non‑goals

- Providing linearizable reads/writes of arbitrary application data.
- Supporting large or highly dynamic clusters (> ~7 nodes) without revisiting quorum and failure‑detector tuning.
- Preserving legacy identifier compatibility or timestamp semantics if it conflicts with safety.

## Current System (v0.1.x)

- **Control plane:** Chitchat gossip provides membership and liveness (`src/server/mod.rs`, `src/disco.rs`).
- **Leader election:** Bully algorithm based on per‑node random generation/ID; leader tracked in `WatchableValue` (`src/bully.rs`).
- **Data plane:** Clients connect over TLS to any node. Followers redirect to leader; leader returns a 16‑byte UUIDv8 Katotonic ID (`src/server/handler.rs`, `src/client/*`).
- **Monotonicity:** Guaranteed only inside a single process by an atomic counter (`src/idgen.rs`).

### Observed invariants today

- **I1:** Per‑leader‑process strict monotonic IDs.
- **I2:** Per‑membership‑view single leader (eventually).
- **I3:** Clients eventually converge on some leader.

### Failure modes today

- **Split brain:** partitions or transient stalls can produce two leaders issuing concurrently.
- **Failover regression:** new leader may issue IDs smaller than previous leader’s last ID without fencing.
- **No fencing term:** stale leader keeps serving until convergence.

These modes are acceptable only under strong operational assumptions (no partitions; minority not reachable; tight clocks).

## Proposed Architecture

### Control plane (unchanged)

Chitchat remains the source of:

- membership set `M` (live nodes)
- per‑node addresses and TLS SNI metadata
- failure‑detector signals that *trigger* elections

Chitchat is not relied on for safety; it may lag or diverge temporarily.

### Data plane safety: quorum‑fenced term + counter

Introduce two cluster‑wide concepts:

1. **`term` (epoch):** a monotonically increasing integer. At most one leader can hold an *active lease* for a given term.
2. **`high_watermark` (HW):** the highest sequence number ever reserved/acknowledged by a majority for that term. IDs are issued in strictly increasing order by `(term, seq)`; `seq` is always `> HW_prev`.

Only these values must be persisted/replicated; there is no log.

### ID ordering and encoding

Primary ordering is by `(term, seq)` lexicographically. This yields a total order that does not depend on wall clocks.

Katotonic IDs are fixed to a single opaque format:

- RFC 4122 UUID **version 8**, variant RFC4122.
- The 122 variable bits store `(term, seq)` big‑endian.
- Raw 16‑byte lexicographic order equals `(term, seq)` order.

See `docs/id-format-uuidv8.md` for the formal bit layout and encoding rules.

## Protocols

### Persistent state per node

Each node stores on disk (fsync on update):

- `current_term: u64`
- `voted_term: u64` (highest term this node has voted for)
- `high_watermark: u64` (highest seq reserved for `current_term`)
- `config_version: u64` (membership version for joint configs; see below)

Crash recovery uses these values to prevent term rollback or duplicate reservations.

### Election (no log, majority vote)

Triggered by:

- Chitchat failure detector marking leader suspect/dead
- Lease renewal failure (see below)
- Join event requiring re‑election for config change

Steps:

1. Candidate reads persisted `current_term`, sets `proposed_term = current_term + 1`.
2. Candidate sends `VoteRequest { proposed_term, candidate_id }` to all `M` via direct TLS RPC (not gossip).
3. Follower receiving request:
   - If `proposed_term <= voted_term`, reply `VoteNo`.
   - Else set `voted_term = proposed_term` (persist), reply `VoteYes` with its `high_watermark`.
4. Candidate wins if it receives `VoteYes` from a **majority of M**.
5. On win:
   - `current_term = proposed_term` (persist).
   - Set local `high_watermark = max(hw_from_majority)` (persist).
   - Enter leader lease loop.
6. On loss/timeouts, backoff and retry with a higher term.

This is effectively a single‑value Paxos/Raft term election without log replication.

### Leader lease / fencing

Safety hinge: a node may issue IDs **only while it holds a valid majority lease for its term**.

Lease renewal:

1. Leader periodically (e.g., every 200–500ms) sends `LeaseRenew { term, leader_id, lease_until }` to all `M`.
2. Follower acks only if:
   - `term == voted_term == current_term` OR `term > current_term` (then update/persist term).
   - It has not acked a different leader for the same term.
3. Leader considers lease valid if it has acks from a majority for an unexpired `lease_until`.
4. If renewal fails before expiry, leader **must stop serving** and transition to `Down`, triggering election.

Effect:

- A minority partition cannot continue issuing because it cannot renew a majority lease.
- A stalled leader cannot serve indefinitely after followers elect a new term.

### High‑throughput issuance: block reservation

To avoid per‑ID coordination:

1. Leader reserves a block `[start, end]` by sending `ReserveBlock { term, start, end }` to all `M`.
2. Followers persist `high_watermark = max(high_watermark, end)` for that term and ack.
3. Once a majority acks, leader may issue IDs from that block locally with only atomic increments.
4. When exhausted, reserve the next block.

Properties:

- Blocks are non‑overlapping and strictly increasing by majority decision.
- Stale leaders cannot reserve new blocks without majority.
- Hot path is identical to today’s local atomic increment, but with no wall clock dependency.

Tuning:

- Block size `K` trades reservation overhead vs. waste on abrupt leader loss.
- For 3 nodes, `K` in the 10k–1M range is typical.

### Serving path changes (API)

On request type `1`:

1. Node checks `leader_state.is_leader()` **and** `lease_valid == true`.
2. If not, return redirect as today, but include `term` in `RedirectInfo`.
3. If yes:
   - If current block empty, reserve next block (majority RPC).
   - Issue next `(term, seq)` from block and serialize as 16 bytes.

Client changes:

- Cache `term` from redirects.
- Optionally reject redirects to leaders with older terms.

## Reconfiguration with Chitchat (joint quorum)

Replacing a node safely requires avoiding disjoint quorums.

We use a simple **joint configuration**:

1. **Join detected:** Chitchat shows new node `N+` with `config_version = v+1 (pending)`.
2. Current leader enters joint config `C_joint = C_old ∪ {N+}`.
3. During joint:
   - Elections, leases, and block reservations require:
     - majority of `C_old` **and**
     - majority of `C_joint`
4. Leader streams current `term` and `high_watermark` to `N+` (one‑shot `Catchup` RPC).
5. Once `N+` is caught up and participating, leader commits new config `C_new` by incrementing `config_version` and advertising it in Chitchat.
6. Old node `N-` is removed; leader exits joint and uses `C_new` for quorum.

For fixed size 3, a simplified rule is acceptable:

- Require 2 acks for all quorum operations, **and at least one ack from the old set** until new node has caught up.

## Expected Outcomes

- **Safety:** At most one active leader serving IDs per term; minority sides cannot serve.
- **Failover monotonicity:** New leader starts from majority `high_watermark`, not wall clock.
- **Throughput:** Hot path stays local; only occasional block reservations incur network cost.
- **Operational flexibility:** Chitchat continues to support rolling replacement without restarts.

## Implementation Sketch (repo impact)

New modules:

- `src/quorum.rs` (or similar): term persistence, vote RPC, lease RPC, block reservation.
- `src/server/handler.rs`: lease/term guard + block issuance.
- `src/client/*`: term‑aware redirects (minimal changes).

RPC transport:

- Reuse existing TLS transport patterns in `src/bully.rs` (CBOR framed messages).
- Add new request/response enums under a distinct codec to avoid mixing with Bully messages.

Persistence:

- Simple local file (e.g., `katotonic.state`), with atomic write + fsync on updates.
- Store term/HW/config_version.

## Tuning

Defaults target 3‑node clusters with realistic tail latency:

- `lease_duration_ms` (server): 1000. Keep comfortably above worst‑case RTT/GC pauses.
- `renew_interval_ms` (server): 250. Renew frequently enough to avoid expiry during spikes.
- `election_timeout_min_ms` / `election_timeout_max_ms`: 800 / 1500. Randomization prevents split votes.
- `block_size` (server): 100_000. Larger blocks reduce quorum traffic but waste more IDs on abrupt leader loss.

Adjust based on observed tail latency and failure‑detector behavior.

## Open Questions / Decisions

1. **Lease duration and renewal interval** tuning for 3 nodes under realistic tail latency.
2. **Block size default** and adaptive strategies.
3. **Chitchat membership versioning**: how to surface `config_version` cleanly in node tags.

## Next Steps

1. Ship UUIDv8 format and remove legacy IDs (Milestone 0).
2. Implement term + lease + block reservation behind a feature flag.
3. Add fault‑injection tests for:
   - minority partition serving attempt
   - transient stall false‑death
   - rolling replace with joint quorum
4. Benchmark block sizes and atomic ordering for max throughput.
