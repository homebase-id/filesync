# Sync semantics v1

The contract the reconciler implements and the simulator checks. The wire format is in
[node-schema-v1.md](node-schema-v1.md); background decisions are in
[../foundations.md](../foundations.md).

## 1. Model

**Node.** A file or a folder, identified by a **node id** (a client-generated UUID that never
changes). Attributes:

| Attribute | File | Folder |
|---|---|---|
| `parent` | node id of the containing folder (the root id for top-level nodes) | same |
| `name` | Unicode, NFC | same |
| `content` | `{ hash, size, mtime }` | none |
| `x` (executable) | bool | none |

The **root** is implicit: it has the fixed id from the node schema and is never stored as a node.

**Three trees.** For every sync root the client keeps three views keyed by node id:
- **Base**: the last state both sides agreed on. Written only after an operation succeeds.
- **Local**: what the local file system holds now, after identity matching (section 3).
- **Remote**: the client's mirror of the drive, fed by the change cursor.

**Content state** (per file node, local only, never synced): `local` (default; may be evicted),
`pinned` (always kept locally), `online-only` (no local bytes). A missing local file is a delete
only when the base says the bytes were local.

## 2. Change classification

For each node, compare each side against the base:

| Change | Condition |
|---|---|
| `created` | in the side, not in base |
| `deleted` | in base, not in the side |
| `edited` | content hash (or `x`) differs from base |
| `moved` | `parent` or `name` differs from base |

`edited` and `moved` are independent and can both be true.

## 3. Local identity matching

A local scan yields entries `(path, file id, fingerprint, hash?)`. They are matched to node ids
in this order, and the first rule that matches wins:
1. **Same path** as a base node with the same kind → that node. A changed file id with the same
   path is an edit (editors that save through a temp file and a rename do this).
2. **Same file id** as a base node whose path is no longer present → that node, moved.
3. **Same content hash and size** as exactly one base file that is missing locally → that node,
   moved. Ambiguous candidates → no match.
4. Otherwise → a **new node** with a fresh id.

Folders match by rules 1 and 2 only.

## 4. Merge rules

Per node, after classifying both sides:

| Local | Remote | Result |
|---|---|---|
| change | none | propagate the local change |
| none | change | propagate the remote change |
| deleted | deleted | drop from base |
| edited | edited | same hash → no-op. Different → **conflict copy** (section 5) |
| moved | moved | same destination → no-op. Different → **remote wins**; the local move is reverted |
| edited | moved | apply both: the content goes up, the move comes down (and vice versa) |
| deleted | edited / moved | **edit wins**: the node is restored locally from the remote |
| edited / moved | deleted | **edit wins**: the node is re-created remotely with the same node id (a tombstone frees its UniqueId) |
| created | created | impossible for one node id; same-name clashes are handled by section 6 |

A `moved` that only reverts a delete case is applied to the restored node.

## 5. Conflict copies

When both sides edited a file to different content:
- The remote version keeps the node id, name and location.
- The local version is renamed in place to `<stem> (conflict <device> <YYYY-MM-DD>)<ext>` and
  becomes a new node that is uploaded normally.
- No bytes are ever dropped: the local bytes are moved by rename, never re-written.

## 6. Invariants on the merged tree

The merged tree is checked after the per-node merge. Fixes are expressed as extra operations.

1. **Unique names per folder**, compared by `key(name) = casefold(NFC(name))`. On a clash the
   node with the lower node id (byte order) keeps the name; each other node is renamed to
   `<stem> (conflict <n>)<ext>` with the smallest `n ≥ 1` that is free. Applies equally to
   files and folders.
2. **No cycles.** If merged moves form a cycle, revert the local moves taking part in it (the
   remote side has already published its moves). Revert one at a time until no cycle remains.
3. **No orphans.** A live node whose merged parent is deleted resurrects the parent chain:
   - remote-deleted parent → re-created remotely with the same node id and its base name;
   - local-deleted parent → re-created locally.
   Any live child keeps its folder alive. Deleting clients delete bottom-up, so a completed
   delete never leaves live children, and a child that survives was added concurrently.
4. **Folder delete propagation** deletes descendants bottom-up, then the folder.

## 7. Delete safety

- **Local deletes go to trash.** Removing a local file or folder because of a remote delete
  moves it into the sync root's internal trash (`.hbsync/trash/`), never `unlink`.
- **Fingerprint guard.** Before deleting, overwriting or moving a local item, the executor
  checks that the local fingerprint still equals the one the plan was made from. On mismatch
  the operation is skipped and the node is re-planned.
- **Version guard.** Remote updates, moves and deletes carry the base `VersionTag`. A mismatch
  means the remote changed: the operation is dropped and the node is re-planned with the newer
  remote state. Homebase's delete takes no VersionTag today (verified in odin-core
  `V2DriveSingleFileWriteOperationsController.cs`), so the client reads the header right before
  deleting and compares (a small race window remains until the server accepts a VersionTag).
- **Mass-delete guard.** If one plan deletes more than `max(50, 25 % of synced files)` files on
  either side, or every file on a side, the deletes are held and the user must confirm. The
  rest of the plan proceeds.
- **Online-only files** are never deleted remotely just because they have no local bytes.

## 8. Operations

Local: `mkdir`, `download` (to temp, verify stored hash, atomic swap), `move`, `trash`,
`rename-for-conflict`. Remote: `create-folder`, `upload` (create by node id), `update-content`,
`move`, `delete`.

Ordering within one plan: folder creates top-down, then moves, then content, then deletes
bottom-up. A move whose target name is occupied by a node that is itself moving away goes
through a temporary name.

Every operation is **idempotent**: a remote create that returns `ExistingFileWithUniqueId`
(4105) is treated as already done; a local move whose source is gone and whose target holds the
expected fingerprint is treated as done.

## 8a. Execution rules (found necessary by the simulator)

- **Settling.** After each successful operation the base takes each attribute (location,
  content) that both sides now hold; never a value only one side has. A plan that stops half
  way can therefore never make the next round revert what did succeed.
- **Retry within a plan.** An operation blocked only by order (target parent not created yet,
  a folder temporarily inside its own destination) goes to the back of the queue.
- **Parking.** If a target name is held by a node the plan is about to move, trash or replace,
  that node is renamed to `.hbsync-move-<node id>` first, and the parking is recorded in the
  state DB. The local view reports a parked node at its original place, but only while it
  still sits at the parked name: a user move voids the record. Before each plan, parked items
  whose original name is free again are moved back.
- **Trash guard.** `LocalTrash` carries the nodes the plan expects in the subtree; if the
  folder holds anything else (in the DB or on disk), it is not trashed and the folder is
  rescanned.
- **Creates** run only while the local node still exists unchanged.
- **Orphans.** With a fully drained feed, a remote node whose parent is unknown everywhere (a
  child created while another client deleted its folder) is moved to the root.
- **Crash recovery** needs no operation log: every operation is idempotent or detectable
  (4105 on a re-create, fingerprint guards locally), state is re-derived from a scan and the
  feed, and parked items come back from their records.

## 9. Properties the simulator checks
1. **Convergence:** after edits stop and every client syncs twice, all local trees and the
   remote are equal (names, structure, content hashes).
2. **No data loss:** every file version that some client held and that no client replaced or
   deleted *after seeing it* exists in the converged tree (possibly as a conflict copy).
3. **Invariants** of section 6 hold in every base tree at every step.
