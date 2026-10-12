# CLAUDE.md

Homebase desktop file-sync client, written in Rust, targeting Windows, macOS and Linux
(licence: `LICENSE`, the Homebase.id License = AGPL-3.0, as chat-kmp; app ID `id.homebase.filesync`). It
syncs a local folder tree with a Homebase drive (server: `../odin-core`; mobile client
reference: `../chat-kmp`).

Status: roadmap Layers 0–4 are built and tested against the in-memory fake remote (two-client
sync on real folders, green on all three OSes in CI). Layer 5 syncs with Homebase:
`hbsync-homebase` registers the Filesync app (YouAuth, its drive and circle), implements
`Remote` on the drive's V2 API (`drive::HomebaseRemote`, every node encrypted:
`docs/spec/encryption.md`) and listens on the notification
WebSocket; two folders converge through a live identity. Facts and server bugs:
`docs/spec/homebase-api-notes.md`. Large files need odin-core with PR #1921 (merged 2026-10-11: streamed,
64-bit payload ranges): a server without it holds each read in memory and can't read past 2 GiB
(**[verified]**, odin-core #1892, #1893). Layer 6 has started with filters (`hbsync-local/src/filter.rs`,
sync semantics §10): built-in junk, and per-device ignore rules in `.hbsync/ignore` (gitignore
syntax, picked up within a minute of an edit); an ignored item is kept on its device only. The
product runs as a per-user daemon (`hbsyncd`) shown and controlled by the tray app
(`hbsync-tray`, program `homebase-filesync`: status, recently synced, pause, logging in through the browser) and by `hbsync`
(`docs/spec/daemon.md`): `cargo run -p hbsync --bin hbsync -- setup <identity>`,
`cargo run -p hbsync-tray`; `HBSYNC_HOME` gives a private instance. Dev tools (`hbsync-dev`), credentials in
`~/.config/hbsync/dev/<identity>.json`:
- `cargo run -p hbsync-dev --bin hbsync-register -- <identity> [--browser firefox]`
- `cargo run --release -p hbsync-dev --bin hbsync-sync -- <identity> <folder> [--once]
  [--allow-mass-delete]`

Server dependencies: every odin-core issue the client waits on has a tracking issue here,
labelled `odin-core` (`gh issue list -R homebase-id/filesync --label odin-core`). Each one names
its upstream issue, the workaround in the code meanwhile, and what to do once it's fixed; the
upstream issue links back. Check upstream state now and then and act on what's fixed.

Read these first:
- `docs/foundations.md`: the pinned decisions (client-computed stored + content hashes on
  `PayloadDescriptor`, id tree with `GroupId` = parent, start-up catch-up tiers, placeholder
  strategy, and the odin-core issues #1892, #1893 and #1895).
- `docs/roadmap.md`: the layered, step-by-step build plan.
- `docs/spec/sync-semantics.md`: what the reconciler and executor must do (the contract).
- `docs/spec/node-schema-v1.md`: how a node maps onto a Homebase file.
- `docs/spec/daemon.md`: the daemon, its files, config, credentials, control socket, autostart.
- `docs/spec/encryption.md`: how files are encrypted (Homebase's AES-CBC format, checked
  against odin-js), and what it does and doesn't protect.

Ground rules:
- Never lose data. The reconciler is a pure function; all OS and network work sits behind traits.
- Build against a fake remote that models the real node schema before touching Homebase.
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
# Against a live identity registered with hbsync-register (the local dev host is frodo):
HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live -- --ignored
HBSYNC_LIVE=frodo.dotyou.cloud cargo test --release -p hbsync-dev --test live_e2e -- --ignored
HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync --test live_daemon -- --ignored
```

Every `Remote` must pass `hbsync_remote::contract` (the fake in CI, Homebase live).

The toolchain is pinned in `rust-toolchain.toml`. CI (`.github/workflows/ci.yml`) runs fmt,
clippy and tests on Ubuntu, Windows and macOS. Crates without SQLite can also be checked for
the other OSes locally: `cargo clippy -p hbsync-platform --target x86_64-pc-windows-msvc`
(set `CARGO_FEATURE_PURE=1` so blake3 doesn't need a cross assembler).

Releases (`RELEASING.md`, `.github/workflows/release.yml`, `docs/spec/packaging.md`): installers
made with Conveyor (`packaging/conveyor.conf`) for Intel and ARM: an MSIX for Windows, a Mac app per architecture
(also as a `.dmg`), a `.deb` with its apt repository, an `.rpm` (Fedora's `rpmbuild`), a static
Linux tarball and a Windows zip, all with `hbsync`, `hbsyncd` and the tray `homebase-filesync`
(built with the `dist` profile, smoke-tested, installed and removed on fresh runners). Build
jobs hold no secrets; a tag `vX.Y.Z` equal to the workspace version makes a draft GitHub
release from the `release-signing` environment (`v*` tags only), to publish by hand; running the
workflow by hand only builds and tests. A tag's Mac app and `.dmg` are signed with the Developer
ID and notarized, its Windows packages signed by Azure Trusted Signing (keys from DevOps).

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
| `hbsync-homebase` | YouAuth app registration (P-384 ECDH, HKDF), the AES-CBC shared-secret envelope, a V2 API client, the Filesync app definition, the drive as a `Remote`, the notification WebSocket |
| `hbsync-app` | the product shell: paths, `config.toml`, credentials in the OS keychain (a 0600 file when there is none), setting the app up on an identity, the sync runner (the daemon's loop: pause, sync now, confirming held deletes, sleep, live rule changes), the daemon, its control socket and one-daemon lock, autostart (launchd agent, systemd user unit, the Windows `Run` key) |
| `hbsync` | the daemon `hbsyncd` and the CLI `hbsync` (`setup`, `status`, `recent`, `sync`, `pause [--for]`, `resume`, `confirm-deletes`, `start`, `stop`, `autostart`, `logout`, `unlink`, `log`) |
| `hbsync-tray` | the tray app: menu from the daemon's status (`model.rs`, pure), local pages for logging in and held deletes, notifications, the folder's icon and sidebar shortcut; Linux via StatusNotifierItem (no GTK), tao's event loop elsewhere |
| `hbsync-dev` | development tools: `hbsync-register`, `hbsync-sync` (a folder synced with a live identity), the live end-to-end test |
