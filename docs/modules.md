# Module sketch (first draft, step list to be rebuilt)

This is the first high-level cut. The decisions in [foundations.md](foundations.md) supersede it
wherever they conflict. The step list at the bottom will be rebuilt around those decisions.

## Guiding principles

- **Never lose data.** Every step should either keep data safe or make it possible to prove that it is safe.
- **Functional core, imperative shell.** The sync decision logic is a pure function with no I/O. All OS and network work sits behind traits.
- **Fake remote first.** Homebase is one adapter that arrives late. Until then the engine runs against an in-memory or local-folder remote that models the real node schema.
- **Journals and watchers only make sync faster.** Correctness comes from the state DB plus a catch-up scan (see foundations.md section 3).

## Modules

| Module | Purpose |
|---|---|
| Platform layer | Hides how the three OSes differ: paths (Windows long paths, reserved names, case-insensitivity, macOS Unicode normalization), stable file id, atomic replace, share modes and locks, timestamp precision |
| Streaming file I/O | Chunked read and write with constant memory, stored + content hashing (BLAKE3 / SHA-256) while streaming, write-to-temp then atomic swap, resumable partial transfers |
| Local change detection | Tiered: OS-owned (File Provider) → journal replay (USN / FSEvents) → bulk metadata walk; plus live watchers |
| State DB | SQLite holding the last-synced base state (three-way sync), node id ↔ fileId, and local name mappings |
| Remote abstraction | Trait for list, change cursor, create, update, move, delete and ranged read. Implementations: fake, then Homebase |
| Reconciler | Pure three-way diff into an operation plan: moves, renames, conflicts, cycles, orphans, mass-delete guard |
| Executor / transfer scheduler | Runs the plan with a journal, so it is idempotent and recovers after a crash; concurrency, retry and backoff, throttling |
| Conflict handling | Deterministic policy and conflict naming, surfaced to the user |
| Filters | Ignore rules, built-in junk files, and names that cannot be represented on another OS |
| Content state / placeholders | online-only, local and pinned. Windows Cloud Files API, macOS File Provider, Linux selective sync (FUSE later) |
| Daemon host | Windows service or per-user agent, launchd, systemd user unit; single instance; sleep/resume handling |
| IPC + control API | Named pipe or Unix socket used by the CLI and tray UI |
| Config + secrets | Config file plus the OS keychain |
| Observability | Structured logs, per-item status, diagnostics bundle |
| UI | CLI, then tray or menubar, then selective sync UI |
| Packaging / update | Signed installers for all three OSes, auto-update |
| Test harness | Deterministic simulator, fault injection, CI matrix covering all 3 OSes |

## First-draft steps (superseded by [roadmap.md](roadmap.md))

1. Sync semantics spec
2. Platform layer + streaming file CRUD library
3. Hashing + local scanner
4. State DB
5. Remote trait + fake remotes
6. Reconciler (pure) + simulation and property tests
7. Executor with operation journal
8. Watchers and journal catch-up
9. Cross-OS hazard pass
10. Daemon, IPC, CLI, config, keychain
11. Packaging, signing, auto-update
12. Homebase remote adapter (after the odin-core server prerequisites)
13. Tray UI + selective sync
14. Placeholders / on-demand files
