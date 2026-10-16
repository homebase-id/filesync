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
| `tags` | Finder tags, a set of (name, colour) | same |

The **root** is implicit: it has the fixed id from the node schema and is never stored as a node.

**Three trees.** For every sync root the client keeps three views keyed by node id:
- **Base**: the last state both sides agreed on. Written only after an operation succeeds.
- **Local**: what the local file system holds now, after identity matching (section 3).
- **Remote**: the client's mirror of the drive, fed by the change cursor.

They live in the account's state DB (`daemon.md`, Files): the mirror once per account, base and
local per sync root. Base, local and the dirty set of a root change only with that root.

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

`edited` and `moved` are independent and can both be true. Tags are compared on their own and
merged three-way whatever else changed (section 11): a tag change is never an `edited`.

## 3. Local identity matching

A local scan yields entries `(path, file id, fingerprint, hash?)`. They are matched to node ids
in this order, and the first rule that matches wins:
1. **Same path** as a base node with the same kind → that node. A changed file id with the same
   path is an edit (editors that save through a temp file and a rename do this, even when a
   scan saw the temp file; one that keeps a backup by renaming leaves the path to the
   document), unless the new file id is that of a base file that left its path (no longer
   present, itself moved onto another name, or swapped with this one): that file was moved
   onto this name (`mv c d` over `d`) and keeps its node, and the node that was here is gone,
   or moved on by its own file id (rule 2). Where file ids are recycled, only with the bytes
   it had. Files only.
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
| edited / moved | deleted | **edit wins**: the removed node is restored remotely with the local version (an update at its file id: node schema "Recently deleted"). If Recently deleted holds a version of the file this device never had (edited elsewhere, then deleted; or taken along by a folder's delete, §6.3), that one stays there and the local version goes up as a new node (detached in place) |
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
   - remote-deleted parent → restored remotely (still removed) or re-created with the same
     node id and its base name (deleted for good);
   - local-deleted parent → re-created locally.
   Any live child keeps its folder alive, but one a folder's removal took along doesn't: a
   deleting client removes the folder first, then what is in it (node schema, Recently deleted),
   so while it does, a removed folder holds live nodes. A remote node in a folder in Recently
   deleted (directly or through live folders) written no later than the folder's removal, by
   the server's times, is remotely deleted with it; one written after (an edit or a new file from
   a device that didn't know yet) keeps the folder alive: restored. A removed folder outside this
   folder's part of the drive, or one it filters (§10), isn't its to bring back: a node written
   into it waits for the folder that syncs that part, or, filtered, goes to the root as an
   orphan does.
4. **Folder delete propagation** removes the folder on the drive first, then its descendants,
   top-down; here it trashes the topmost folder (its base goes after its children's).

## 7. Delete safety

- **Deletes are removals.** A remote delete sets the node's `archivalStatus` to removed and
  keeps its bytes for `keep_deleted_days` (30): "Recently deleted" lists them, and a restore
  brings them back on every device (node schema, "Recently deleted"). The daily purge deletes
  older ones for good. The reconciler sees a removed node as deleted.
- **Local deletes go to trash.** Removing a local file or folder because of a remote delete
  moves it into the sync root's internal trash (`.hbsync/trash/`), never `unlink` (one rename:
  junk and anything that appears meanwhile go along). The daily purge empties entries older
  than `keep_deleted_days`, of those made since it began.
- **Fingerprint guard.** Before deleting, overwriting or moving a local item, the executor
  checks that the local fingerprint still equals the one the plan was made from. On mismatch
  the operation is skipped and the node is re-planned.
- **Version guard.** Remote updates, moves and deletes carry the base `VersionTag`. A mismatch
  means the remote changed: the operation is dropped and the node is re-planned with the newer
  remote state. Homebase's delete takes the VersionTag too (odin-core PR #1931, verified in
  `V2DriveControllerBase.PerformFileDelete`); a delete refused because the file is already
  deleted is a retry, and done.
- **Mass-delete guard.** If one plan deletes more than `max(50, 25 % of synced files)` files on
  either side, or every file on a side, the deletes are held and the user must confirm. The
  rest of the plan proceeds. A confirmation names the held deletes the user was shown, node
  by node: those go through, and are neither counted nor held again; it lasts while a delete
  is pending for the node (done or undone, it is forgotten). Any other delete is judged as if
  the confirmed ones weren't there (a confirmed trash of a folder with every file under it):
  one the user didn't see is never let through by a confirmation. Simulator:
  `guarded_clients_delete_only_what_was_confirmed`; the guard's count across planning batches
  is the engine's (`a_confirmed_folder_trash_does_not_hold_other_deletes`).
- **Online-only files** are never deleted remotely just because they have no local bytes.

## 8. Operations

Local: `mkdir`, `download` (to temp, verify stored hash, atomic swap), `move`, `tags`, `trash`,
`rename-for-conflict`, and for filters (section 10) `detach` (a new node id in place) and
`forget` (trash plus dropping the base). Remote: `create-folder`, `upload` (create by node id),
`update-content`, `move`, `tags`, `remove` (to Recently deleted; a create of a removed node
restores it). Outside plans: `restore` and `purge` (Recently deleted). Every op that writes a
node (create, upload, move, download, mkdir) writes its merged tags too; `tags` is for a change
of tags alone.

Ordering within one plan: folder creates top-down, then moves, then content, then deletes:
first the remote ones top-down (a folder before what is in it: node schema, Recently deleted),
then the local ones bottom-up (a folder's base is forgotten after its children's deletes, on
either side). In the other phases the local operations come before the remote ones, so that
the remote ones run together (section 8a). A move whose target
name is occupied by a node that is itself moving away goes through a temporary name.

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
  moved back; if a filter (section 10) now matches that name and something there holds it, the
  item takes the first free numbered name instead (the holder is never tracked, so it would
  never leave): one this device syncs if any of the first hundred is, else a filtered one (the
  item is then ignored, rule 1). A parked item that must leave its temporary name for another
  reason (it is detached, or released as a folder that keeps ignored items, section 10) takes
  its original name if free, else a numbered one the same way: the temporary name carries the
  old id and never reaches the remote.
- **Trash guard.** `LocalTrash` carries the nodes the plan expects in the subtree; if the
  folder holds anything else (in the DB or on disk), it is not trashed and the folder is
  rescanned.
- **Creates** run only while the local node still exists unchanged.
- **Parents first on the server.** A remote create, upload or move waits (retry) until its
  parent folder is in the mirror; the server doesn't check parents, and a child of a folder
  whose create failed would look like an orphan to every other client. A remote move whose
  destination is inside the node on the drive (a nesting turned around) waits too, for the
  move that takes the destination out: the server doesn't check cycles either.
- **Folders are removed first** (node schema, Recently deleted). A remote delete waits while
  the plan still has the removal of its folder to run (one retried goes to the back of the
  queue); one whose folder's removal didn't happen in the run waits for the next plan. A child
  whose removal failed or was skipped stays live in the removed folder until a later plan
  removes it; meanwhile §6.3 takes it as deleted with the folder by its time.
- **Together on the pool.** Transfers and remote writes (folder creates, renames, moves, tags,
  deletes) are collected and run together on the worker pool, in any order. An operation that
  depends on one collected waits until they all ran: a create or move into a folder created
  (or restored) in the batch, a move into a folder another move in the batch carries along (the two could
  make a cycle on the drive), a folder's delete while the batch moves a node into or out of
  it, a delete while the batch removes its folder, the same node twice. Anything else (local work, settling) runs on its own, after the
  batch. A remote outage starts no further writes; those failing together count as one
  failure, and the rest run again after the pause.
- **No starvation.** The dirty set is planned in pages, in node id order, swept round after
  round and across syncs, so nodes that stay dirty (held, deferred, failing) can't keep the
  rest waiting. A sync ends after a full sweep in which nothing was done.
- **Names that can't be spelled** locally (a Windows device name at the length limit) are
  skipped and reported, not retried; conflict names are shortened to fit 255 bytes.
- **Conflict copies are write-ahead.** The fork is recorded before the rename and the record
  is removed with the DB update after it; recovery runs before any scan and completes or
  forgets an interrupted fork. A copy id derives from the node, the local bytes and the base's
  remote version: stable when one conflict is replanned, new for the next conflict. Two devices
  with the same bytes make the same copy, so it may exist remotely already, named to fit its
  folder there; if that name is held in the local folder by something that stays, the fork
  takes the first free numbered name, and a later plan moves the copy where it belongs.
- **Case-insensitive volumes** (probed once): a name's occupant is any sibling with the same
  name key; a node may take its own name in another case.
- **Orphans.** With a fully drained feed, a remote node whose parent is unknown everywhere (a
  child created while another client deleted its folder) is moved to the root.
- **Crash recovery** needs no operation log: every operation is idempotent or detectable
  (4105 on a re-create, fingerprint guards locally), state is re-derived from a scan and the
  feed, and parked items come back from their records. A download placed but not recorded (a
  crash just before its commit) is found again: each batch of downloads is recorded before it
  runs, with the base each commit writes (the node as the drive has it at that version) and its
  target; on the next sync the file at a record's target holding its bytes, new to the scan or
  the node's own file replaced, is that node with that base, and later changes on the drive are
  plain changes against it. The records go then. A verified download that couldn't be
  placed stays in the temp folder for the retry; temp files no pending download wants are
  removed before each plan. The stored journal position never passes a change that isn't
  recorded: it is taken before waiting for events and stored only once the event queue has
  drained.
- **Power cuts.** The state DB syncs its commits to the disk only now and then
  (`synchronous=NORMAL`): an OS crash or a power cut can lose the last ones, in order, while the
  folder and the drive keep what was done. So a sync starts and ends with the DB durable (what
  earlier syncs did is on the disk before parked items are put back), a run starts from a durable
  DB (the ids the scan gave new items go out with them), and so does an upload after a detach
  (its new id); write-ahead records (parking, conflict copies, downloads, a folder this device
  synced before made again) and a folder's registration are durable; before something is put
  where an item was moved or trashed from since the last durable commit, the commits are made
  durable (with that record lost, the scan would take the newcomer for the old item); and the
  folders whose entries were changed are synced before any durable commit to the account's DB,
  whichever folder's engine makes it (macOS: a full sync per volume), and so are they before
  the DB copies its log into its file (which makes every commit durable), so a durable commit
  never holds a change the disk lost. The state DB keeps this order itself, and a new process
  takes the DB as not durable (one before it may have died). Not ordered: the system writing
  the log out on its own between syncs, and tags or times written on an item; a commit kept
  without its change on disk reads as a local change. What the lost commits did is then found
  again as after a crash. The engine's randomized crash test cuts the power too, keeping any commits
  since the last durable one.

## 9. Properties the simulator checks
1. **Convergence:** after edits stop and every client syncs twice, all local trees and the
   remote are equal (names, structure, content hashes).
2. **No data loss:** every file version that some client held and that no client replaced or
   deleted *after seeing it* exists in the converged tree (possibly as a conflict copy).
3. **Invariants** of section 6 hold in every base tree at every step.
4. **Filters** (section 10): convergence and no loss hold for what each client syncs; a
   filtered local item is never changed or removed by sync, except junk going to the trash
   with its folder.
5. **Tags** (section 11): convergence includes the tags as each client sees them (clients tag
   like macOS, Linux and Windows); a tag some user added and no user removed is on its node in
   the end, unless the node was deleted, or the item it was added to went to the trash or got
   a new id on that device (then it went along).

## 10. Filters

Some items are never synced. Filtering is per device and works in both directions: a filtered
item is invisible to this device's sync, locally and remotely. Ignoring never deletes anything
on its own.

**Classes** (`hbsync-local/src/filter.rs`):
- **Excluded**: the client's own `.hbsync` folder at the top of the sync root.
- **Junk**: a built-in list of names the OS or an application creates by itself, matched in any
  case at any depth: macOS `.DS_Store`, `._*`, `Icon\r`, `.Spotlight-V100`, `.Trashes`,
  `.fseventsd`, `.TemporaryItems`, `.DocumentRevisions-V100`, `.apdisk`; Windows `Thumbs.db`,
  `ehthumbs.db`, `desktop.ini`, `$RECYCLE.BIN`, `System Volume Information`; KDE `.directory`;
  Office `~$*` and
  `~*.tmp`; LibreOffice `.~lock.*#`; vim `.*.swp`, `.*.swx`; downloads in progress
  `*.crdownload`, `*.part`, `*.partial`, Safari's `*.download`; other sync apps' own files, left
  in a folder moved over from them: Dropbox `.dropbox`, `.dropbox.cache`, OneDrive
  `.849C9593-D756-4E56-8D6E-42412F2A707B`, Google Drive `.tmp.drivedownload`,
  `.tmp.driveupload`. Junk is disposable.
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
   never downloaded, and never deleted by this device, with one exception: a node this device
   synced at a place it now filters (a rule was added) is leaving per rule 1, and is deleted
   remotely unless the remote changed it since: its place, bytes or tags (a folder: unless it holds anything that isn't
   leaving, such as another device's new file; then only its base is dropped, once the bases
   below it are gone or go in the same run). If the user brings the local item back into view first (moves it out of
   the ignored place), it syncs as usual, and its local place wins over a remote move (the
   remote place is filtered too, and following it would take the item out of view again). Names held by masked nodes stay taken: a local item with such a name gets a numbered
   one (a name clash).
   A masked node that comes into view (a rule was removed, or another device moved it out of
   the ignored place) is downloaded with everything in it.
   A synced node that becomes masked because another device moved it into a path this device
   ignores leaves this device, the remote untouched:
   - an unchanged local copy goes to the trash, and its base goes in the same step
     (`Forget`): a base left behind would read as a local delete once the node is back in view
     (another rule, another move);
   - a local copy with changes the remote doesn't have (edited, or never synced) is
     **detached** (`LocalDetach`): it becomes a new node where it is and is created remotely
     like any new item. The old id is still live remotely, so it can't be created again. The
     masked folders it sits in are detached with it, as are masked folders holding anything
     else this device syncs; their unchanged contents go to the trash.
3. **Folders.** A remote folder with masked live children (that aren't leaving) counts as
   having a live child: a local delete of it deletes what this device saw, and the folder comes
   back here, empty.
   Locally, junk in a folder being trashed goes to the trash with it; ignored content keeps its
   folder: the synced contents are trashed and the folder is released (the next scan sees it
   as a new folder); junk directly in it stays with it.
4. **Rule changes.** The filter's fingerprint (rules, junk list version, case sensitivity) is
   kept in the state DB. When it changed, start-up walks the whole tree (a journal doesn't
   report items that just became visible) and re-plans every synced node; the new fingerprint
   is stored in the same transaction. Items that come into view are new items. The sync loop
   checks the ignore file for an edit once a minute and on a sync request, and
   starts over with the new rules when it changed.

## 11. Tags

Finder tags: a name and optionally one of Finder's seven colours, on files and folders (node
schema `tags`). They are a sync dimension of their own: adding or removing a tag, or changing
its colour, syncs even when nothing else changed.

**Where they live** (`hbsync-platform/src/tags.rs`): macOS `com.apple.metadata:_kMDItemUserTags`
(a binary plist array of `"Name"` or `"Name\nColour"`, what Finder and Spotlight use); Linux
`user.xdg.tags` (comma-separated names, what KDE uses); Windows nowhere. A file system without
extended attributes counts as "nowhere".

**What a device can't hold is the drive's.** The local side of a node's tags is what the device
read, completed from the drive's tags (the base's when the drive has none): a tag the system
can't hold (any on Windows, colours and names with a comma on Linux, names with a newline on
macOS) is taken as it is on the drive, and a tag read as the system shows a drive tag is that
tag (Linux's `Red` is the drive's `Red`, orange). So a device never removes or recolours a tag
it can't see.

**Merge**, per name, three-way: a tag either side added is in, a tag either side removed is out;
a colour comes from the side that changed it, the remote's if both did. Never a conflict copy.

**A tag change is not an edit.** Against a delete on the other side the delete wins (the item
is in Recently deleted, with its tags); it never makes a conflict copy. A conflict copy keeps
the local file's tags, and an edit that restores a removed node merges the removed version's
tags in (it may hold tags this device never saw).

**Local changes** are noticed by the change time a tag write moves (scan, watcher); the scan
then reads the tags. Tags that couldn't be read are tried again at the next scan.
Downloads write the tags onto the temp file before it is put in place: a file without them
would read as the user removing them. An item with more than about 2,000 bytes of tags
isn't tag-synced: it reads as "can't tell", and keeps what the drive has; so do tags that can't
be read or written for a moment (a failed tag write never fails the operation). A node with no
base gets one when place and content agree, with the tags both sides have: the merge then
brings each side the other's (a copy proven equal only across hash algorithms can't be proven
again later).

**Limits.** Tag changes are seen through the change time: on a volume that keeps attributes
elsewhere (exFAT's `._` files) or with one-second times (HFS+), a change can go unnoticed until
the item changes again. A tag change makes the scan read the file again (the change time is
part of the fingerprint, to catch writes that keep the modification time). An item that becomes
a new node on a device that can't hold all its tags (detached on Windows, §10) goes up with
the tags that device has.

## 12. Scopes
A device syncs the main folder (`~/Homebase`) and, if the user chooses, **role folders** where
they are: Documents, Pictures, Music, Videos, Downloads (node schema, "Role folders"). Each
local folder has an engine of its own, scoped to a part of the drive; they share the account's
state DB (one mirror, one feed) and keep their own base and local rows:
- the main folder's scope is the drive's top **without the role folders**, on every device,
  whether it syncs them or not: a role folder appears only where it is synced in place, never
  inside `~/Homebase`;
- a role folder's scope is what lies in that folder. Its engine sees the folder as its root
  and the rest of the drive as one folder at its top that it leaves out; what it writes at its
  root goes into the role folder. A node whose place isn't known (its folder gone from the
  drive, a cycle) isn't a role folder's: the main folder repairs it (§6); one that sits in a
  role folder in Recently deleted (another device put it there as it was deleted) is that role
  folder's.

Before every sync of a role folder, after reading the drive, its top must be on the drive: one
in Recently deleted comes back with what was deleted with it, one not there at all (never made,
or deleted for good) is made at the drive's top with the role's id. So the first device to add
a role makes its folder, the others join it, and a role folder deleted on the drive while a
device syncs it comes back at that device's next sync, nothing trashed there and nothing
uploaded into Recently deleted. To delete a role folder for good, stop syncing it everywhere
first.

What lies outside an engine's scope is masked as filtered nodes are (§10): never downloaded,
never changed or deleted remotely. It is never *leaving* (§10, rule 1): another engine or device
syncs it. A role folder at the top that the engine holds nothing of isn't planned at all; no
version ever synced one into the main folder, so a device holds one only through a bug, and
then lets go of it as of anything masked. A role folder moved into a folder (against the node
schema's rules) is masked there and keeps that folder alive (§6.3).

A role folder holds no name for the scope: a plain folder of the same name at the top keeps it
(two at the top, told apart by their ids). A new scope plans the folders it, or the last one,
left out again, from their tops; it needs no walk, as only the remote view changed. Recently
deleted is the account's, like the mirror: one list, each item naming the folder of this
device it comes back to (none for a role folder this device doesn't sync); restoring, emptying
and the purge (by age, with every folder's trash) are the account's too. A change of the drive
is planned in the folders it concerns: those holding the node, and the one whose part of the
drive its place is in (every folder when that place isn't known).

A detached item's new id is fixed for the node and the device, so a replan picks the same one,
but never one in use here: the same node detached again while its first copy lives gets the
next.
