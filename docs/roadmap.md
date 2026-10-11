# Roadmap: layered build plan

Progress (2026-10-10): Layers 0–4 are built against the fake remote; Layer 5's adapter syncs
two folders through a live identity (frodo), including the WebSocket wake-up. Open in Layer 5:
batch deletes, files of 4 GiB and over (#1893), app sign-in by slug (#1902), and the interop
work. Files are encrypted in Homebase's own format
([spec/encryption.md](spec/encryption.md)). Layer 6 has its filters, and the
daemon with its config, keychain, control socket, CLI and autostart
([spec/daemon.md](spec/daemon.md)).
Open items in those layers:
- catch-up at background I/O and CPU priority (Layer 2) is not implemented yet;
- the performance budget isn't measured in CI yet (no 10^6-file benchmark job);
- Linux uses `getdents64` + `fstatat` through std; batching `statx` via io_uring is not done;
- resumable upload is not in the `Remote` trait (Homebase has no resumable upload yet);
- the unprivileged USN read is untested as a non-admin (CI runners are administrators).

Bottom-up. Each layer is usable and tested on all three OSes before the next one leans on it.
Decisions referenced here are in [foundations.md](foundations.md).

## Layer 0: Specs (shared with Homebase)
Written: [spec/sync-semantics.md](spec/sync-semantics.md),
[spec/node-schema-v1.md](spec/node-schema-v1.md), [spec/perf-budget.md](spec/perf-budget.md),
[spec/homebase-api-notes.md](spec/homebase-api-notes.md).

- **Sync semantics spec**:
  - node model
  - invariants: unique names under NFC + case-fold, no cycles, orphan resurrection, delete safety
  - conflict policy
- **Sync-node schema v1**: one spec that odin-core, odin-js and chat-kmp all test against. The hash golden vectors (SHA-256 + BLAKE3) come from odin-core #1895
- **Performance budget**: start-up time and RAM targets for 10^6 files, measured in CI from Layer 2 on

## Layer 1: Platform foundation
- **Cargo workspace + CI matrix for all 3 OSes** from day one
- **Platform layer**:
  - path model: NFC and case-fold keys, Windows long paths, reserved names
  - stable file id
  - atomic replace
  - share modes and locks
  - timestamp precision
- **Streaming file CRUD library**:
  - constant memory
  - write to temp then atomic swap
  - resumable partial writes
  - stored and content hashes (BLAKE3; SHA-256 when comparing with nodes that use it) computed while streaming
- **Local name mapping**: reversible escaping of names that are illegal on this OS, recorded in the DB, never pushed to the remote

## Layer 2: Local truth
- **State DB (SQLite)**. Per node it holds:
  - the base state
  - node id ↔ fileId
  - the local fingerprint (size, mtime, ctime, file id)
  - the hash
  - the content state

  It also has migrations and crash-safe writes.
- **Fast metadata walker (tier C)**:
  - bulk enumeration APIs
  - sorted streaming merge against the DB
  - hashes only when the fingerprint changed
  - runs at background I/O and CPU priority
- **Journal catch-up (tier B)**:
  - USN journal replay on Windows; FSEvents `sinceWhen` on macOS
  - detects when the journal is no longer usable and falls back to the walker
- **Live watchers**: debounced and coalesced; on overflow, run a full rescan
- **Local observation interface**: walker, journal, watchers (and later File Provider) all feed one "dirty set"

## Layer 3: Remote abstraction
- **Node-based remote trait**:
  - change cursor
  - idempotent create by node id
  - VersionTag-guarded update, move and delete
  - ranged read
  - resumable upload
- **In-memory fake remote** that models Homebase exactly:
  - tombstones without UniqueId
  - VersionTag conflicts
  - cursor semantics
  - fault and latency injection
- **Remote mirror in the state DB**: the remote side of the three-way merge

## Layer 4: Sync engine
- **Tree model**: path resolution over node ids, with invariant checks
- **Content state as a first-class concept**: online-only, local or pinned per node, from the start
- **Reconciler (pure)**:
  - three-way diff
  - renames and moves via node id, local move detection via hash
  - conflicts, cycles, orphans
  - mass-delete guard
- **Deterministic simulator + property tests**: N clients make random concurrent edits; they must converge and never lose data
- **Executor**:
  - operation journal, idempotent ops
  - retry and backoff
  - VersionTag mismatch → re-plan
  - crash recovery
- **Transfer scheduler**:
  - concurrency limits
  - priorities: user-opened file first, then small files first
  - throttling
  - resumable transfers
- **Start-up orchestrator**: remote cursor catch-up and local catch-up run in parallel; uploads of local state that is still unknown wait for it
- **First-run adoption**: match existing local files to remote nodes by hash, with no download
- **Milestone**: two-client sync through the fake remote, green on all three OSes
  (`crates/hbsync-engine/tests/milestone.rs`)

## Layer 5: Homebase (server prerequisites can start in parallel with Layer 1)
- **odin-core prerequisites**:
  - streamed payload reads (#1892, done in PR #1921)
  - 64-bit sizes and ranges (#1893: ranges done in PR #1921, the upload byte count still open)
  - payload hashes + per-drive "hashes required" setting (#1895, merged as PR #1899)
  - guarded delete: optional `versionTag` (#1903)
  - apps sign in by slug (#1902)
  - resumable upload (not filed yet)
  - "Files" drive type + reserved SyncFile / SyncFolder types (not filed yet)
- **Homebase remote adapter** (done 2026-10-09, `hbsync-homebase`):
  - app registration and auth
  - query-batch cursor feed, with the WebSocket as a wake-up hint only
  - node-schema mapping
  - VersionTag-guarded writes
  - a contract suite every `Remote` passes, run live against frodo
- **Interop**: payload hashes and the node schema in odin-js and chat-kmp, checked against the golden vectors, so web and mobile can browse and edit the same drive

## Layer 6: Product shell
- **Daemon host** (done 2026-10-09, [spec/daemon.md](spec/daemon.md)): a per-user daemon,
  started at logon by a launchd agent, a systemd user unit or the Windows `Run` key; single
  instance; sleep/resume (a wall-clock gap); network changes only through reconnect backoff
  and the poll so far
- **IPC control API + CLI** (done 2026-10-09): a local socket / named pipe only the user can
  open; `hbsync setup`, `status`, `sync`, `pause`, `resume`, `confirm-deletes`, `start`,
  `stop`, `autostart`, `logout`, `unlink`, `log`
- **Config + OS keychain** (done 2026-10-09): `config.toml`; credentials in Keychain,
  Credential Manager or the Secret Service, a user-only file when there is none
- **Filters** (done 2026-10-09, sync semantics §10): built-in junk, per-device ignore rules in
  `.hbsync/ignore`, remote nodes masked per device, local-only semantics for ignored items
- **Observability**: structured logs, per-item status, diagnostics bundle
- **Packaging**: release archives for Linux x86_64, macOS and Windows x86_64 (done
  2026-10-10, `.github/workflows/release.yml`, unsigned); still open: signing and notarization,
  auto-update, arm64 Linux and Windows

## Layer 7: Content on demand
- **Selective sync**: all OSes; this is the Linux v1 answer
- **Windows Cloud Files placeholders**: pin states, hydration through ranged reads
- **macOS File Provider extension**: the OS owns the folder and becomes a tier-A local observation source. Keep this in mind from Layer 2 on: retrofitting it is structural.
- **Tray / menubar UI**: status, conflicts, selective sync
- **Linux FUSE mount** (later)

## Continuous: hardening
- **Cross-OS hazard suite**:
  - case and Unicode collisions
  - locked files
  - disk full
  - clock skew
  - long paths
  - FAT/exFAT/SMB volumes
- **Fault injection**: crash mid-write, network drop mid-transfer
- **Performance budget in CI**: start-up catch-up time and RAM at 10^6 files
