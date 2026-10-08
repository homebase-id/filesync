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
- Never re-hash at start; hash only when `(size, mtime, ctime, file id)` changed.
- Streaming, sorted merge of the walk against DB rows; never load the whole tree.
- The reconciler works on the dirty set plus the ancestors and siblings it needs, not the
  whole tree.
- Catch-up runs at background I/O and CPU priority.
