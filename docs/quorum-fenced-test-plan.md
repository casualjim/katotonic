# Quorum‑Fenced Sequencer & UUIDv8 — Comprehensive Test Plan

Status: draft (2025‑12‑12).  
Scope: the quorum‑fenced sequencer described in `docs/quorum-fenced-sequencer.md`, the shipped UUIDv8 format in `docs/id-format-uuidv8.md`, and the implementation milestones in `docs/quorum-fenced-uuidv8-plan.md`.

This plan aims to make the system “bomb‑proof” under the stated failure model by combining unit tests, property tests, deterministic simulations, integration tests, fuzzing, and long‑running chaos/soak tests.

---

## 1. Safety Invariants (what every test ultimately protects)

**IDs**
1. **Uniqueness:** no duplicate UUID bytes across the entire cluster history.
2. **Total order:** raw UUID byte lexicographic order equals `(term, seq)` lexicographic order.
3. **Strict monotonic issuance:** the *sequence of IDs returned to clients* is strictly increasing in raw byte order.
4. **Opaque format:** fixed v8 + RFC4122 variant bits always correct; payload is within range.

**Quorum state**
5. **Term monotonicity:** `current_term` and `voted_term` never decrease on any node, including after crash/restart.
6. **Vote safety:** a node grants at most one `VoteYes` per term; rejects `proposed_term <= voted_term`.
7. **High‑watermark safety:** within a term, `high_watermark` never decreases; leader starts issuance at `>= max(hw_majority)`.

**Leadership / fencing**
8. **Single active leader per term:** at most one leader may serve IDs for a given term.
9. **Lease fencing (M4):** a node may serve IDs **only while it holds a majority lease for its term**; minority partitions must not serve.
10. **Block reservation (M5):** reserved blocks do not overlap and advance `high_watermark` by majority decision.

**Reconfiguration**
11. **Joint quorum safety:** during membership change, quorum operations require overlapping majorities per joint‑config rule; disjoint quorums never both succeed.

---

## 2. Failure Model Covered

We explicitly test under:
- **Crash‑stop failures:** nodes halt; may restart later with persisted quorum state.
- **Network faults:** partitions (majority/minority), delay, loss, reordering, duplication, and transient flaps.
- **Process stalls:** GC pauses / scheduler stalls / long stop‑the‑world delays.
- **Disk faults:** partial write, torn state file, state loss, and corruption.
- **Malformed/invalid messages (bugs/version skew):** invalid CBOR, replayed old messages, unexpected request sequences.

Non‑goals for tests unless decided otherwise:
- Byzantine / intentional lying is out of scope. The cluster is a closed, mutually‑authenticated set of trusted nodes; tests assume non‑byzantine behavior. We still cover malformed inputs due to bugs or partial upgrades.

---

## 3. Test Layers

### L0. Unit tests (fast, deterministic)
Purpose: prove local invariants and guard regressions.

Targets:
- `src/id.rs`: encode/decode/fixed bits/order.
- `src/quorum/state.rs`: term/HW/config invariants and persistence behavior.
- `src/quorum/service.rs`: follower vote rules and persistence on vote.
- `src/quorum/election.rs`: election attempt logic and higher‑term observation.
- Future modules: lease and reservation handlers (M4/M5).

### L1. Property‑based tests
Purpose: explore large state spaces for algebraic/ordering properties.

Recommended tooling: `proptest` for Rust.

Targets:
- Random `(term, seq)` round‑trip and ordering monotonicity for UUIDs.
- Random sequences of `QuorumState` operations; never allow regression.
- Random vote request streams against random node states; vote safety.
- Future: lease renewals + reserve blocks; verify invariants 8–10.

### L2. Deterministic simulation / model checking‑lite
Purpose: explore worst‑case schedules with reproducible seeds.

Approach:
- Build a `sim` test harness (in `tests/sim/` or `src/quorum/sim.rs` behind `cfg(test)`):
  - In‑memory node replicas with real `QuorumService` logic.
  - Pluggable transport that can drop/delay/reorder/duplicate.
  - Fake disk implementing atomic persist + crash recovery.
  - Deterministic RNG + virtual time.

Benefits:
- Reproduces subtle races without flaky integration tests.
- Allows systematic exploration of partitions, stalls, and restarts.

### L3. Integration tests (real async tasks)
Purpose: validate end‑to‑end behavior with real `track_leader`/server wiring.

Approach:
- Spawn N nodes in‑process using loopback transports.
- Feed synthetic membership changes via a test discovery implementation.
- Run multiple clients concurrently against random nodes.

### L4. Fuzzing / negative tests
Purpose: harden request parsing and handler robustness.

Targets:
- `QuorumRequest`/`QuorumResponse` CBOR decoding.
- `QuorumService::handle` and future handlers for panics/state corruption.
- Replay attacks: old votes/leases/blocks.

### L5. Chaos / soak stress (slow, always‑on by default)
Purpose: high‑confidence “soak tests” over long randomized runs. These should run on every CI invocation to keep the “never break safety” bar high, since the system is expected to be stable with infrequent changes.

Approach:
- Run a 3–7 node test cluster for minutes/hours with:
  - Random fault injector (kill/restart nodes, partitions, delay, disk wipe).
  - Load generator producing high‑rate ID requests to random nodes.
  - Central collector tracking all IDs and leader terms.
- Assert invariants 1–11 at end and continuously.

---

## 4. Detailed Test Matrix by Component/Milestone

### 4.1 UUIDv8 format (`src/id.rs`) — M0 (shipped)

Unit:
- `round_trip(term, seq)`: decode(encode(x)) == x for boundary and random values.
- `fixed_bits`: version nibble == 8, variant == RFC4122.
- `byte_ordering`: for ordered tuples `t1 < t2` lexicographically, `encode(t1).bytes() < encode(t2).bytes()`.

Property:
- For random pairs `(a, b)`:
  - `a < b` iff `encode(a) < encode(b)` by raw bytes.
  - Inject random fixed‑bit flips and ensure `is_katotonic_v8` rejects.

Compatibility/negative:
- Compare ordering against GUID field‑reordered comparisons in tests to ensure clients must not use that ordering (doc‑anchored test).

### 4.2 Quorum persisted state (`src/quorum/state.rs`) — M1 (shipped)

Unit:
- `load_default_missing_file`.
- `persist_round_trip`.
- `validate_rejects_voted_lt_current`.
- `set_current_term_no_regression`.
- `set_voted_term_no_regression_and_ge_current`.
- `advance_high_watermark_no_regression`.
- `bump_term_resets_hw_and_increments_vote`.

Property:
- Generate random operation sequences (set/bump/advance/config bump); after each step:
  - validate passes.
  - monotonic fields never decrease.

Fault injection:
- Simulate torn writes / corrupted bytes:
  - `load` should return error and node should fail safe on boot (test expected behavior).
- State loss:
  - if file deleted between restarts, node must start at 0 and rely on higher‑term votes to catch up; test via simulation.

### 4.3 Vote handling & election (`src/quorum/service.rs`, `src/quorum/election.rs`) — M3 (shipped)

Unit:
- Follower:
  - `vote_yes_on_higher_term_persists_voted_term_and_sets_follower_state`.
  - `vote_no_on_equal_or_lower_term_does_not_persist`.
- Candidate:
  - `bump_term_self_vote_sets_voted_term = base+1`.
  - `observe_higher_voted_term_updates_persisted_vote`.
- Election attempt:
  - majority math correctness for N=1..7.
  - if any peer returns higher term/vote, candidate steps down and persists observed term.
  - on win, leader installs `(term, max_hw_from_majority)` and becomes leader.

Property:
- Random prior states for peers with random responses:
  - candidate wins only if >= majority yes at proposed term.
  - observed higher term always dominates, preventing stale leadership.

Simulation:
- **Concurrent elections:** two nodes start as Down, race to propose term.
  - Exactly one leader emerges; other observes higher term and steps down.
- **Leader crash mid‑election:** candidate crashes after self‑vote but before sending.
  - Term monotonicity holds; another node eventually wins higher term.
- **Partition during election:** split 2/1 (N=3).
  - Majority side elects; minority does not.
- **Repeated flaps:** alternating partition/heal with random delays.
  - Leadership converges; terms strictly increase.

Integration:
- Run `track_leader` on 3 nodes with synthetic membership.
  - Ensure `membership_monitor` triggers election when leader disappears.

### 4.4 Lease fencing (`QuorumRequest::LeaseRenew`) — M4 (pending)

Unit (to add with M4):
- Follower rules:
  - Ack only for `term >= voted_term`.
  - Never ack two different leaders for same term.
  - If `term > current_term`, persist new term.
- Leader rules:
  - lease_valid only when >= majority acks for unexpired lease_until.
  - on renewal failure before expiry, leader transitions to Down and stops serving.

Simulation:
- **Minority reachable partition:** 2/1 split.
  - Minority leader’s lease expires and it stops serving; majority may continue.
- **Stalled leader:** pause leader task for > lease duration.
  - Followers elect higher term; old leader cannot regain lease.
- **Clock skew on lease_until:** since lease uses local time, test with skewed leader clock.
  - Ensure safety by requiring follower acceptance based on their monotonic term/vote rules, not leader clock trust.

Chaos:
- Random stalls and partitions; assert no IDs emitted while lease_invalid.

### 4.5 Block reservation (`QuorumRequest::ReserveBlock`) — M5 (pending)

Unit (to add with M5):
- Follower:
  - Accept only for `term == current_term` (or higher term updates per design).
  - Persist `high_watermark = max(hw, end_seq)`; reject regressions.
- Leader:
  - Does not issue from a block unless majority ReserveAck.
  - Never overlaps blocks; next block starts at `prev_end+1`.

Property:
- For random sequences of ReserveBlock calls:
  - `high_watermark` equals max of all acked `end_seq` in that term.
  - Issued seq set is subset of acked ranges.

Simulation:
- **Leader crash after reserve, before issuing:**
  - new leader uses majority HW and does not overlap.
- **Leader crash mid‑block:**
  - some seq wasted but no duplicates; new leader starts above HW.
- **Out‑of‑order ReserveBlock arrivals on followers:**
  - followers accept high end, ignore lower; no regression.

Chaos:
- Vary block size K; validate max waste <= K‑1 per leader loss.

### 4.6 Reconfiguration / joint quorum — M6 (design)

Simulation:
- **Add node with joint config:**
  - elections and reservations require both majority_old and majority_joint.
  - prove overlap by attempting disjoint majority wins; should fail.
- **Remove node:**
  - old quorum still required until config commit.
- **Catchup correctness:**
  - new node after Catchup never allows term/HW regression.

Integration:
- Drive membership stream to simulate join/remove while clients issue IDs.
  - IDs stay strictly monotonic and unique.

### 4.7 Client behavior & redirects

Unit (client side):
- Redirect includes leader + term; client caches term.
- Client refuses redirect to lower term once it has seen higher term.

Integration:
- With mixed redirects during flaps:
  - clients eventually settle on highest‑term leader.
  - no client observes decreasing IDs.

---

## 5. Fault‑Injection Scenario Catalog (for L2/L5)

Each scenario should be run under multiple random seeds and with N=3,5 where applicable.

1. **Split brain attempt:** partition into two groups both with clients.
2. **Message delay skew:** one follower has 10× latency, causing repeated timeouts.
3. **Drop/dup/reorder storm:** randomized transport faults at 1–10% rates.
4. **Crash at critical points:**
   - after persisting voted_term
   - after winning election but before broadcasting lease
   - after reserving block but before serving
5. **Disk wipe on restart:** node loses state, re‑joins with term=0.
6. **State corruption:** follower loads invalid CBOR.
7. **Leader stall:** pause leader task longer than lease duration.
8. **Rapid rolling restarts:** restart every node one‑by‑one under load.

Assertions:
- Global ID uniqueness and strict monotonicity.
- No serving on minority / lease invalidity.
- Terms monotonic; no leader for older term once a higher term exists.

---

## 6. Acceptance Criteria

We consider the system “world‑class safe” when:

- All L0–L4 tests pass in CI.
- L2 simulation covers all scenarios in §5 and never violates invariants under ≥10k random schedules per scenario.
- L5 chaos runs for ≥30 minutes with ≥10^8 IDs total and 1–5% fault rates without invariant violation.
- Any found counterexample is turned into a deterministic regression test.

---

## 7. Implementation Notes for the Test Harness

- Prefer in‑process deterministic simulation for most fault‑injection cases; only keep a small integration suite to avoid flakiness.
- Keep soak/chaos tests enabled by default (no `#[ignore]`). If we ever need faster local loops, add an **opt‑out** feature like `--features quick-tests` that *skips* L5, rather than an opt‑in gate.
- Use a central `IdCollector` that records `(node, term, seq, bytes, wall_time)` for post‑hoc analysis.
- Always compare IDs using `Uuid::as_bytes()` ordering.
- Make lease/block parameters injectable so tests can sweep values.
