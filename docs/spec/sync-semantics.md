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
  that node is renamed to `.hbsync-move-<node id>` first, in its own folder, and the parking
  is recorded in the state DB. The local view reports a parked node under its original name
  in whatever folder it is in now, but only while it still carries the parked name: a rename
  voids the record. Before each plan, parked items whose original name is free again are
  moved back.
- **Trash guard.** `LocalTrash` carries the nodes the plan expects in the subtree; if the
  folder holds anything else (in the DB or on disk), it is not trashed and the folder is
  rescanned.
- **Creates** run only while the local node still exists unchanged.
- **Parents first on the server.** A remote create, upload or move waits (retry) until its
  parent folder is in the mirror; the server doesn't check parents, and a child of a folder
  whose create failed would look like an orphan to every other client.
- **Folders are deleted last.** A remote folder delete waits while the mirror still has a
  child in it (a child delete that failed or was skipped, or a child just added): the folder
  stays, and the next plan resurrects it if needed.
- **No starvation.** The dirty set is planned in pages, in node id order, swept round after
  round and across syncs, so nodes that stay dirty (held, deferred, failing) can't keep the
  rest waiting. A sync ends after a full sweep in which nothing was done.
- **Names that can't be spelled** locally (a Windows device name at the length limit) are
  skipped and reported, not retried; conflict names are shortened to fit 255 bytes.
- **Conflict copies are write-ahead.** The fork is recorded before the rename and the record
  is removed with the DB update after it; recovery runs before any scan and completes or
  forgets an interrupted fork. A copy id derives from the node, the local bytes and the base's
  remote version: stable when one conflict is replanned, new for the next conflict.
- **Case-insensitive volumes** (probed once): a name's occupant is any sibling with the same
  name key; a node may take its own name in another case.
- **Orphans.** With a fully drained feed, a remote node whose parent is unknown everywhere (a
  child created while another client deleted its folder) is moved to the root.
- **Crash recovery** needs no operation log: every operation is idempotent or detectable
  (4105 on a re-create, fingerprint guards locally), state is re-derived from a scan and the
  feed, and parked items come back from their records. A verified download that couldn't be
  placed stays in the temp folder for the retry; temp files no pending download wants are
  removed before each plan. The stored journal position never passes a change that isn't
  recorded: it is taken before waiting for events and stored only once the event queue has
  drained.

## 9. Properties the simulator checks
1. **Convergence:** after edits stop and every client syncs twice, all local trees and the
   remote are equal (names, structure, content hashes).
2. **No data loss:** every file version that some client held and that no client replaced or
   deleted *after seeing it* exists in the converged tree (possibly as a conflict copy).
3. **Invariants** of section 6 hold in every base tree at every step.
4. **Filters** (section 10): convergence and no loss hold for what each client syncs; a
   filtered local item is never changed or removed by sync, except junk going to the trash
   with its folder.

## 10. Filters

Some items are never synced. Filtering is per device and works in both directions: a filtered
item is invisible to this device's sync, locally and remotely. Ignoring never deletes anything
on its own.

**Classes** (`hbsync-local/src/filter.rs`):
- **Excluded**: the client's own `.hbsync` folder at the top of the sync root.
- **Junk**: a built-in list of names the OS or an application creates by itself, matched in any
  case at any depth: macOS `.DS_Store`, `._*`, `Icon\r`, `.Spotlight-V100`, `.Trashes`,
  `.fseventsd`, `.TemporaryItems`, `.DocumentRevisions-V100`, `.apdisk`; Windows `Thumbs.db`,
  `ehthumbs.db`, `desktop.ini`, `$RECYCLE.BIN`, `System Volume Information`; Office `~$*` and
  `~*.tmp`; LibreOffice `.~lock.*#`; vim `.*.swp`, `.*.swx`; downloads in progress
  `*.crdownload`, `*.part`. Junk is disposable.
- **Ignored**: the user's rules in `.hbsync/ignore` (gitignore syntax, per device, never
  synced), matched case-insensitively on case-insensitive volumes. Ignored items are the user's
  data, kept on this device only. A rule that doesn't parse stops the client: skipping it would
  sync what the user meant to keep here.

An item inside a filtered folder is filtered too.

**Rules:**
1. **Local.** The walk neither reports nor enters filtered entries (nothing is hashed), and the
   watcher drops their events. A synced item that becomes filtered (a rule was added, or it was
   moved into an ignored folder) looks deleted: it leaves the drive and the other devices
   (their trash), and stays here. The mass-delete guard applies.
2. **Remote.** A remote node whose path is filtered on this device is masked with its subtree:
   never downloaded, never deleted by this device. A synced node that becomes masked (another
   device moved it into a path this device ignores) is trashed here; the remote is untouched.
3. **Folders.** A remote folder with masked live children counts as having a live child: a
   local delete of it deletes what this device saw, and the folder comes back here, empty.
   Locally, junk in a folder being trashed goes to the trash with it; ignored content keeps its
   folder: the synced contents are trashed and the folder is released (the next scan sees it
   as a new folder).
4. **Rule changes.** The filter's fingerprint (rules, junk list version, case sensitivity) is
   kept in the state DB. When it changed, start-up walks the whole tree (a journal doesn't
   report items that just became visible) and re-plans every remote node. Rules are read at
   start-up.
