# Performance budget

Targets for a sync root of **10^6 files in 10^5 folders** on a local SSD. These are estimates
until the benchmark harness measures them. CI tracks them from Layer 2 on (smaller trees in CI,
scaled up in the nightly benchmark).

| Scenario | Target |
|---|---|
| Start-up, nothing changed, tier C walk (Linux) | ≤ 10 s wall, ≤ 1 CPU core |
| Start-up, nothing changed, tier B journal (Windows/macOS) | ≤ 1 s |
| Start-up, 1,000 files changed | walk/journal time + hashing of the 1,000 files only |
| Resident memory during catch-up | ≤ 150 MB; the walk is O(depth), not O(files) |
| Resident memory when idle | ≤ 50 MB |
| State DB size | ≤ 400 bytes per node |
| Remote catch-up, nothing changed | one `query-batch` round trip |
| Hashing throughput (BLAKE3, single thread) | ≥ 1 GB/s on x86-64 with AVX2 |
| First-run adoption of an existing copy | hashing at disk speed, zero downloads for matching files |

Rules that keep these reachable (from [../foundations.md](../foundations.md) §3):
- Never re-hash at start; hash only when `(size, mtime, ctime, file id)` changed. A new file
  nothing on the drive can be compared with is hashed as it is sent, not by the scan: its
  bytes are read once (sync semantics §3, Hash on send). 2,000 new files of 1 KiB to 4 MiB,
  525 MB (fake remote, warm cache, 2026-10-23): 1,056 MB read and 0.87 s before, 532 MB
  read and 0.72 s after.
- Streaming, sorted merge of the walk against DB rows; never load the whole tree.
- The reconciler works on the dirty set plus the ancestors and siblings it needs, not the
  whole tree: in a folder, only the children whose names may clash with a dirty node's (found
  by their sibling hash, an indexed column), unless a plan finds a clash there.
- A watcher event looks at the entry it names (one `stat`, one row), not its whole folder; a
  folder, or a name that may be another spelling of one (case-insensitive volumes), rescans
  its folder. The client's own writes, named again by the watcher, write nothing.
- Catch-up runs at background I/O and CPU priority.
- A large file being written (8 MiB or more, modified in the last 5 s) is read once it is
  quiet, not at every batch: the observer looks again after 5 s, then 3, 6, 12 … 60 s apart
  while it is still written to (one never quiet is read anyway every 10 minutes). An upload looks at its file before it starts and every 16 MiB
  while it is sent, and stops at a change. A 480 MiB file written in 60 one-second steps
  (fake remote, 2026-10-22): 55 hashes, 12.3 GiB hashed, 25.6 GiB read and 55 uploads started
  before; 1 hash, 0.94 GiB read and 1 upload after, synced 5 s after the last write instead
  of at once.

## Measured: sync with a live identity (2026-10-09)
`hbsync-sync` (release) against frodo.dotyou.cloud: the local dev host on the same machine
(odin-core on SQLite, NVMe). The tree is 10,003 files of 1–8 KB in 100 folders plus three
100 MB files, 361 MB in all, on tmpfs. Loopback, so this is client and server cost with no
network latency.

| Scenario | Result |
|---|---|
| First upload: walk, hash, create 10,104 nodes | 26.6 s wall, 6.7 s CPU, peak RSS 89 MB |
| First download into an empty folder (10,130 nodes, 361 MB) | 8.3 s wall, 9.0 s CPU, peak RSS 157 MB |
| Feed read, 10,755 rows from the start | 1.1 s (500 per page; 1,000 or 2,000 aren't faster) |
| Restart, nothing changed | 55 ms to idle |
| Idle loop (watcher, WebSocket; measured with the 60 s poll, gone since 2026-10-16) | 12 MB RSS, 3 threads, CPU ≈ 0 |
| Edit in one folder until it is in the other (both running) | ≈ 0.55 s, mostly the 300 ms local debounce |
| Deleting the 10,130 nodes | 33.6 s: a header check and a delete per node, one at a time |

Follow-ups: batch deletes (done: removals through `update-archival-status-batch` since
2026-10-18, emptying through `soft-delete-batch` since 2026-10-19, 250 files a request);
find what drives peak memory during a large first download.
