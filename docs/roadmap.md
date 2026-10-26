# Roadmap

Open work first, in order: what to fix, what to build next, then later and continuous work.
What is built is under **=== DONE ===** at the end, by layer, as it was planned bottom-up.
Decisions referenced here are in [foundations.md](foundations.md).

Status (2026-10-26): v0.9.1 is released. It syncs folders with a live Homebase on Windows,
macOS and Linux, with the tray, the daemon and the CLI, role folders, filters, Recently
deleted and signed installers that update themselves; a state DB too old or damaged to use is
set aside and the folder set up again on its own.

## Fix first (found testing 0.9.0, 2026-10-25/26)

1. **An upload whose uniqueId the server already has** (#295). The server answers with an
   error code (4105, odin-core #2019) right after the metadata, while the payload is still
   being sent. React to it at once: stop sending and treat the file as already there. If we
   don't react within about 5 seconds, the server closes the connection, so a 20 GB file isn't
   wastefully sent twice. Then, and after any upload that ended without an answer, look the file up by
   its uniqueId (`getByUniqueId`) before sending it again.

   (Found in the first sync of 309 GB: six files of 11–20 GB lost their answer, one was sent
   twice. The lost answers themselves go away on the server's side: it will answer 200 once it
   has received the file, before moving it to long-term storage.)

Emptying Recently deleted (`hbsync empty-deleted`, the tray's *Empty*) runs in the daemon's
background, in passes: all files, then the folders a level at a time. With 22,367 items, the
user's Homebase answered about 70 s per batch of 1,000 files, so it took about 30 minutes,
and nothing showed it was working:
2. **Write each batch's result as it comes**, not once per pass: Recently deleted then
   shrinks while it runs, and a crash or kill loses nothing done.
3. **Let stop and quit interrupt it between batches.** Today `hbsync stop`, quitting the
   tray and SIGTERM all wait for the whole job, and `stop` gives up after a minute with "the
   daemon is still finishing a transfer"; what isn't done stays in Recently deleted for the
   next time.
4. **Say it runs in the background**: the CLI and the tray say emptying has started and goes
   on in the background, the daemon logs when it starts (with the count) and ends, and
   `status` shows it while it runs.

One more from the same test:
5. **Quit while the tray starts the daemon leaves it running.** Quit at the moment the tray
   was starting the daemon: the start went ahead, the stop gave up ("still finishing a
   transfer"), and the tray closed with the daemon still syncing.

Also from the test:
6. **Documents holds 14,164 dirty marks for nodes with no rows** (no local, base or mirror
   row): find what leaves them, clear them, and check what they cost each sync of that folder.
7. **Auto-heal's leftovers** (a state DB set aside when too old or damaged): a DB that goes bad
   while the daemon runs is only healed at the next start; `hbsync add` heals without printing
   the note; setup after a heal doesn't refuse the empty mount point of an unplugged disk; a
   version-13 DB missing a table fails its migration instead of being set aside.

## Small fixes

- **Held deletions still show after they are confirmed.** The tray's magenta dot and
  `status`'s "deletions to review" stay until the run that carries them out ends (93 s for
  12,101 files), because they show the previous run's report.
- **The folder picker can open behind the browser** (Windows): *Change…* on the login page
  opens the OS's picker from the tray, which has no window to put it in front of, so the
  browser just shows it's loading. (Pressing Enter opened it too, the reason Enter didn't log
  in; fixed.) Bring the picker to the front, or have the page say a window opened.

## Performance (found comparing with Dropbox, 2026-10-26)

- **Start the large files alongside the small ones.** Filesync sends a folder's small files
  first and its large ones after; Dropbox starts the large ones at once and fills the link.
  With 5,204 files (4.8 GB), most of the 130 s difference.
- **Small files: find where the 0.3 s per file goes.** The server takes 80–230 ms of it (core's
  log analysis); the rest is ours or on the way (building and encrypting the upload, TLS, our
  per-file commit). Measure, then decide on more uploads at once or less work per file.
- **Idle wakeups: about 2 a second in each of the daemon and the tray,** all on one thread
  (`blocking-1`), probably the desktop's D-Bus link (the app's systemd scope, the tray's
  StatusNotifierItem). The daemon alone idled at once every 12 s on frodo: bring it back.
- **State DB writes per file** (about 70 KB per file in a first sync of many small files):
  grouping commits is parked as a data-safety rule; look for anything cheaper first.

## Next

- **Ask before starting or stopping syncing** (the user, 2026-10-25): quitting the tray asks
  whether to stop syncing too or keep it running in the background, and starting the tray
  while the daemon isn't running asks whether to start it. Today both happen without asking:
  quitting always stops the daemon, and the tray starts it whenever a folder is set up
- **Previous versions**: an overwritten file's earlier versions kept on the drive for 30
  days, as Recently deleted keeps deleted ones, with a list and *Restore* in the tray and the
  file browser. It is the protection against ransomware and broken apps: a file encrypted or
  corrupted in place reaches the drive as an ordinary edit, which no delete guard sees, and
  replaces the good version on every device. Counts against the user's storage, like Recently
  deleted; the cost per edit (bytes kept, requests) to be measured and named before building
- **Selective sync** (all OSes; the Linux v1 answer to content on demand), shown in the tray
- **Role folders, next steps**: a setup wizard offering them, then backups
- **Observability**: structured logs, per-item status, diagnostics bundle

## Ideas and things to weigh

- **`.gitignore` inside git repos** (for consideration, 2026-10-23): inside a folder with
  `.git`, honour its `.gitignore` files with git's rules (per folder, `!` included), so a repo
  can live in a synced folder without its build output and temp files; ignored items stay on
  each device, as with `.hbsync/ignore`, and the rules are the same on every device since
  `.gitignore` syncs. To weigh first: a branch switch changes the rules (newly ignored items
  leave the drive, a large swing held by the guard), turning it on for existing folders makes
  their build output leave once, and `.git` itself synced from two computers that both commit
  gives conflict copies git doesn't understand (say so, or leave `.git` out)
- **Duplicate finder** (idea, 2026-10-24): find duplicate files and folders from the BLAKE3
  content hashes already kept, with no file reads and no requests. Files: group the state DB by
  content hash and size. Folders: a hash per folder computed bottom-up from its children, in two
  kinds: one over names and hashes (the same folder, file for file), one over hashes only (the
  same contents under other names). The file browser could do the same from the drive's
  hashes, without a computer

## Later: content on demand

- **Windows Cloud Files placeholders**: pin states, hydration through ranged reads
- **macOS File Provider extension**: the OS owns the folder and becomes a tier-A local
  observation source. Retrofitting it is structural: keep it in mind in local-truth changes.
- **Linux FUSE mount**

## Open pieces of the built layers

- Catch-up at background I/O and CPU priority (Layer 2).
- Linux uses `getdents64` + `fstatat` through std; batching `statx` via io_uring is not done.
- Resumable upload: not in the `Remote` trait; Homebase has none yet (not filed).
- odin-core: apps sign in by slug (#1902); a "Files" drive type with reserved SyncFile /
  SyncFolder types (not filed).
- The server's `soft-delete-batch` took about 70 s per 1,000 files on the user's Homebase
  (2026-10-25; about 3–4 s on frodo), probably the per-file work plus S3. To examine with
  odin-core.
- odin-core, being fixed by the user (2026-10-26): an upload answered 200 once the server has
  received it, then moved to long-term storage in the background (today the answer waits for
  the S3 copy, and the load balancer cuts it after 300 s of silence); the duplicate check
  right after the metadata (#2019); and the `Authorization` header written to the logs on
  slow requests.
- Interop: payload hashes and the node schema in odin-js, checked against the golden vectors
  (chat-kmp's Filesync mini-app follows [spec/file-browser.md](spec/file-browser.md)).
- The unprivileged USN read is untested as a non-admin (CI runners are administrators).
- Packaging: confirming the Windows and Mac updates on real machines.

## Continuous: hardening

- **Cross-OS hazard suite**:
  - case and Unicode collisions
  - locked files
  - disk full
  - clock skew
  - long paths
  - FAT/exFAT/SMB volumes
- **Fault injection**: crash mid-write, network drop mid-transfer
- **Performance budget in CI**: start-up catch-up time and RAM at 10^6 files (no 10^6-file
  benchmark job yet)

## === DONE ===

Built bottom-up, each layer tested on all three OSes before the next leaned on it.

### Layer 0: Specs (shared with Homebase)
[spec/sync-semantics.md](spec/sync-semantics.md) (node model; invariants: unique names under
NFC + case-fold, no cycles, orphan resurrection, delete safety; conflict policy),
[spec/node-schema-v1.md](spec/node-schema-v1.md) (the one schema odin-core, odin-js and
chat-kmp test against; hash golden vectors from odin-core #1895),
[spec/perf-budget.md](spec/perf-budget.md), [spec/homebase-api-notes.md](spec/homebase-api-notes.md).

### Layer 1: Platform foundation
- Cargo workspace and a CI matrix for all three OSes
- Platform layer: NFC and case-fold keys, Windows long paths, reserved names; stable file
  ids; atomic replace; share modes and locks; timestamp precision
- Streaming file library: constant memory, temp file then atomic swap, resumable partial
  writes, BLAKE3 (and SHA-256) hashes computed while streaming
- Local name mapping: reversible escaping of names a given OS can't hold, never pushed

### Layer 2: Local truth
- State DB (SQLite): base state, node id ↔ fileId, local fingerprint, hash, content state;
  migrations and crash-safe writes
- Fast metadata walker (tier C): bulk enumeration, a sorted merge against the DB, hashes only
  when the fingerprint changed, and since 0.9.0 a new file is read once, as it is sent
- Journal catch-up (tier B): the USN journal on Windows, FSEvents `sinceWhen` on macOS, with a
  fallback to the walker
- Live watchers, debounced and coalesced, a full rescan on overflow; one dirty set fed by all

### Layer 3: Remote abstraction
- Node-based remote trait: change cursor, idempotent create by node id, VersionTag-guarded
  update, move and delete, ranged reads
- In-memory fake remote modelling Homebase (tombstones without UniqueId, VersionTag
  conflicts, cursor semantics, fault and latency injection)
- The remote mirror in the state DB, the remote side of the three-way merge

### Layer 4: Sync engine
- Tree model over node ids with invariant checks; content state per node
- Pure reconciler: three-way diff, moves via node id and local moves via hash, conflicts,
  cycles, orphans, the mass-delete guard
- Deterministic simulator and property tests: random concurrent edits on several clients
  converge and never lose data
- Executor: idempotent ops, retry and backoff, re-plan on a VersionTag mismatch, crash
  recovery; transfer scheduler with concurrency limits and resumable transfers
- Start-up orchestrator (remote and local catch-up in parallel) and first-run adoption by hash
- Milestone: two-client sync through the fake remote, green on all three OSes
  (`crates/hbsync-engine/tests/milestone/`)

### Layer 5: Homebase
- odin-core prerequisites: streamed payload reads (#1892, PR #1921); 64-bit sizes and ranges
  (#1893, PRs #1921 and #1932; files up to 50 GiB); payload hashes (#1895, PR #1899); guarded
  delete (#1903, PR #1931); the feed's batch endpoints, minimal events and own writes left out
  (see CLAUDE.md for versions)
- Homebase remote adapter (`hbsync-homebase`, 2026-10-09): app registration and auth, the
  query-batch feed with the WebSocket as a wake-up hint, the node-schema mapping,
  VersionTag-guarded writes, encryption in Homebase's own format
  ([spec/encryption.md](spec/encryption.md)), and a contract suite run live against frodo

### Layer 6: Product shell
- Daemon host ([spec/daemon.md](spec/daemon.md)): per user, started at logon (launchd,
  systemd, the Windows `Run` key), single instance, sleep and resume; no polling: every
  reconnect catches up from the feed's cursor
- IPC control API and the `hbsync` CLI
- Config and OS keychain
- Filters (sync semantics §10): built-in junk, per-device rules in `.hbsync/ignore`
- Role folders (2026-10-14, sync semantics §12): Documents, Pictures, Music, Videos and
  Downloads where they are, one state DB per account
- Recently deleted for 30 days, restore, held deletions to review
- The tray (2026-10-11 on): status and progress, recently synced, problems, timed pause,
  logging in, held deletes, Recently deleted, the Folders page, notifications, the folder's
  icon and shortcut
- Packaging ([spec/packaging.md](spec/packaging.md), `RELEASING.md`): Conveyor installers that
  update themselves (MSIX, Mac apps and `.dmg`, `.deb` with its apt repository), an `.rpm`, a
  static tarball and a Windows zip, signed and notarized
