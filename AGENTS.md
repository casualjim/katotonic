# Repository Guidelines

## Project Structure & Module Organization

- `src/` contains the Rust crate.
  - `src/lib.rs` exports public API and feature‑gated modules.
  - `src/bin/` holds binaries:
    - `async-server.rs` (`katotonic`, default Tokio).
    - `sync-server.rs` (`catotonic`, feature `blocking`).
    - `smol-server.rs` (`kittytonic`, feature `smol`).
  - `src/server/` and `src/client/` implement TLS protocol, redirects, and pooling.
  - `src/bully.rs`, `src/disco.rs`, `src/idgen.rs` are the election, discovery, and ID core.
- `examples/` provides load/usage clients (async/sync/smol).
- `go-katotonic/` is a compatible Go client library.
- `tests/certs/` holds dev/test TLS certs (do not use in production).

## Build, Test, and Development Commands

- `cargo build` / `cargo build --release`: build crate/binaries.
- `cargo test`: run Rust unit tests.
- `cargo run --bin katotonic -- [server flags]`: run Tokio server.
- `cargo run --features blocking --bin catotonic -- ...`: run sync server.
- `cargo run --features smol --bin kittytonic -- ...`: run Smol server.
- `cargo run --example async-client -- [client flags]`: exercise protocol.
- Go client: `cd go-katotonic && go test ./...` (no tests yet; add as needed).

## Coding Style & Naming Conventions

- Rust: follow `rustfmt` (`cargo fmt`) and existing `.rustfmt.toml`. Prefer explicit names over abbreviations. Keep hot‑path code allocation‑light.
- Linting: use `cargo clippy` before large changes; fix only relevant warnings.
- Go: `gofmt` on save; exported types/functions use `CamelCase`.

## Testing Guidelines

- Tests are co‑located unit tests using `#[test]` and `#[tokio::test]`.
- Add tests near the module being changed; name by behavior (e.g., `test_election_recovers_after_timeout`).
- If adding Go tests, use `*_test.go` and standard `testing` package.

## Commit & Pull Request Guidelines

- History uses short, imperative summaries (no strict Conventional Commits). Example: `refactor go client`.
- Commits: one logical change per commit; include rationale in body when non‑obvious.
- PRs: describe behavior change, link issues/design docs, and note any protocol/feature‑flag impacts. Include benchmarks when touching `idgen`, `client`, or `server` hot paths.

## Security & Configuration Tips

- Override default cert/key/CA paths in production; defaults point to `tests/certs/`.
- Ensure clock sync if your deployment depends on wall‑clock semantics; Katotonic IDs do not.
