# CLAUDE.md

Homebase desktop file-sync client, written in Rust, targeting Windows, macOS and Linux. It
syncs a local folder tree with a Homebase drive (server: `../odin-core`; mobile client
reference: `../chat-kmp`).

Status: roadmap Layers 0–4 are built and tested against the in-memory fake remote (two-client
sync on real folders, green on all three OSes in CI). Next: Layer 5, the Homebase adapter
(shared-secret envelopes, YouAuth, V2 endpoints; see `docs/spec/homebase-api-notes.md`).

Read these first:
- `docs/foundations.md`: the pinned decisions (client-computed stored + content hashes on
  `PayloadDescriptor`, id tree with `GroupId` = parent, start-up catch-up tiers, placeholder
  strategy, and the odin-core issues #1892, #1893 and #1895).
- `docs/roadmap.md`: the layered, step-by-step build plan.
- `docs/spec/sync-semantics.md`: what the reconciler and executor must do (the contract).
- `docs/spec/node-schema-v1.md`: how a node maps onto a Homebase file.

Ground rules:
- Never lose data. The reconciler is a pure function; all OS and network work sits behind traits.
- Build against a fake remote that models the real node schema before touching Homebase.
- Homebase encryption is out of scope here for now; the focus is the three-OS side.
- When stating facts about the Homebase API, say whether they were verified in the sibling repos'
  code or inferred.
- A change to sync behavior gets a simulator property or regression test (`hbsync-sim`), not
  only an example test.

## Build and test

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
# Deeper runs (release mode):
PROPTEST_CASES=150000 cargo test --release -p hbsync-sim --test props
MILESTONE_SEEDS=3000 cargo test --release -p hbsync-engine --test milestone randomized
```

The toolchain is pinned in `rust-toolchain.toml`. CI (`.github/workflows/ci.yml`) runs fmt,
clippy and tests on Ubuntu, Windows and macOS. Crates without SQLite can also be checked for
the other OSes locally: `cargo clippy -p hbsync-platform --target x86_64-pc-windows-msvc`
(set `CARGO_FEATURE_PURE=1` so blake3 doesn't need a cross assembler).

## Crates

| Crate | Role |
|---|---|
| `hbsync-platform` | OS differences: names and comparison keys, `RelPath`, file ids, no-clobber rename, bulk directory listing, USN/FSEvents journals, fs kind |
| `hbsync-fileio` | BLAKE3/SHA-256 streaming hashes, resumable verified partial files, guarded atomic swap |
| `hbsync-namemap` | local spelling of remote names (Windows SFM escapes) |
| `hbsync-types` | node ids, node state, node-schema-v1 content JSON |
| `hbsync-state` | SQLite state DB: base, local observations, remote mirror, dirty set, parked items |
| `hbsync-local` | walker, scan merge, targeted rescans, journal catch-up, live watcher |
| `hbsync-remote` | blocking `Remote` trait and the fake server/remote with Homebase semantics |
| `hbsync-engine` | tree, pure reconciler, view builder, executor, transfer pool, adoption, sync loop |
| `hbsync-sim` | deterministic multi-client simulator with convergence/no-loss properties |
