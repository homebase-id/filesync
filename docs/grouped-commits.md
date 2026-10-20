# Grouped commits: parked (2026-10-20)

Not built. This is what we found, so the idea can be picked up again in the architecture pass.

## The cost today

Every placed download, and every finished op, is its own SQLite transaction
(`StateDb::write`, `synchronous=NORMAL`, WAL).
- **Per file:** about 20 `pwrite64` and 16 `fcntl` on the runner thread (strace, 2,041
  downloads, 2026-10-19).
- **Disk writes:** about 50–60 KB of WAL per placed file. B wrote 491 MB to receive 387 MB, so
  most of the ~100 MB extra is the DB.
- **CPU:** NORMAL makes each commit cheap (~0.03 ms; FULL was 2.3 ms), so this is mostly about
  the bytes written, not CPU.

## The design on the table

- After a group's barrier (the grouped download fsync, #126), commit that group's placements in
  one transaction, at most 64 per group.
- Always commit before a `write_durable` or `filling` barrier, so the durability rules in
  `hbsync-state/src/lib.rs` keep their order.
- Test it with the randomized crash and power-cut harness (`testkit` commit hook) and the deep
  runs, before and after.

Expected gain: roughly the WAL bytes per file divided by the group size, and fewer syscalls.
It hasn't been measured.

## Why it is parked

- **The failure window widens.** Today a commit survives any process crash: only a power cut
  (or an OS crash) can lose the commits since the last durable one. With grouping, an ordinary
  crash, kill or panic loses the group's uncommitted records too. Files are on the disk, but
  the DB doesn't know.
- **Recovery would run far more often.** Write-ahead `placing` records and `exec::recover`
  handle that state, but it's the path where we found the duplicate and "bytes twice" classes
  while building NORMAL (PRs #72–#74). It would go from rare (power cuts) to common (every
  crash).
- **It changes the durability contract.** That belongs in the architecture assessment, along
  with the storage API (`Durability`, one intent log), not in a speed PR.

## Picking it up

1. Measure WAL bytes and syscalls per file for 2,041 and 20,000 downloads (the
   `~/tmp/hbsync-bench-071` harness).
2. Decide whether the gain is worth recovery on every crash. If it is, write the rule into the
   crate docs of `hbsync-state` first, then build it with crash-harness coverage.
3. A cheaper variant to weigh first: group only results that touch nothing on the local disk
   (metadata writes, uploads). Their loss on a crash means a re-check, not a recovery.
