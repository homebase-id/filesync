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
  Tags count one by one: one both sides agree on (both hold it, or both lack it) takes that
  value, one they differ on keeps the base's. A local write settles against the drive's version
  it acted on (the one the plan saw; a download runs only while the mirror still holds that
  version, or the one the run's own remote write made of it with the same bytes (a tag write, a
  move) while the base is there, which it acts on then; it is planned again otherwise), not a
  later one, even where the mirror has moved on
  or lost the node since; the base records that version. Its tags record exactly what it wrote,
  tag by tag, never a recomputation: each tag the write changed on the item takes the written
  value where that version holds it, and every other tag keeps the base's. A write that writes
  no tags (a move, a folder made with none over none) leaves them as they are; a download or a
  folder made sets the item's whole set, changing each tag the item held otherwise before (none
  where there was no item; where what it wrote isn't known, the base's tags start afresh, each
  one the item holds and the version holds too). This holds whatever moved since: the next plan
  compares the drive's newer value with the written one, and the scan reports what the user
  changed after the write. A tag this device can't hold at all the item never held: what a
  write leaves of it is the drive's value, which the base takes. The commit, recovery and the
  simulator settle tags alike.
  Its content, place and version settle only if the base is still at the version its plan saw,
  or, for a write that doesn't put a node in place from a record, where the run's own earlier
  operations on the node settled it (compare and set): where anything else moved it since
  (another folder, the feed, recovery), they stay as they are and the node dirty, for the next
  plan. Where the run's own remote write moved the node past the version a local write acted
  on, its content, place and version settle against the remote write's version, while the
  mirror holds it. A download or a folder made from its record settles by the record alone, so
  its commit and its recovery settle it the same way. Where the drive moved on from the version
  a write settles against, the node stays dirty too (an operation can come for a node that
  wasn't). Without a base, a write that put the item in place
  (made, fetched or moved it) makes that version the base where the bytes agree; a tag change
  alone doesn't, nor a move to where the item already is.
- **Local**: what the local file system holds now, after identity matching (section 3).
- **Remote**: the client's mirror of the drive, fed by the change cursor; after 29 days away
  it reads the whole feed again (node schema, "The purge", 4.).

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
   present, a folder in its place, itself moved onto another name, or swapped with this
   one): that file was moved onto this name (`mv c d` over `d`) and keeps its node, and the
   node that was here is gone, or moved on by its own file id (rule 2). Where file ids are
   recycled, only with the bytes it had. Files only.
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
- The copy's node id comes from the node, the local bytes (hash and executable bit), the base
  version and the computer (its state DB's own id for the account): planned again on that
  computer, it is the same copy. Each computer makes its own copy: two computers with the same
  bytes against the same version give two copies, each uploaded (rare, and accepted). A state
  DB copied to a second computer (a backup restored there) gives both the same id: accepted.
- A copy that can't be placed yet (its folder waits for its own parent, or the node, or a
  folder it or its copy is in, waits for a name what waits for a hash holds, §6.3) isn't made:
  the node waits with it, its conflicts (the edits, a move) aren't reported yet, and the
  folder's notes say that a conflict copy is waiting.

## 6. Invariants on the merged tree

The merged tree is checked after the per-node merge. Fixes are expressed as extra operations.

1. **Unique names per folder**, compared by `key(name) = casefold(NFC(name))`. On a clash the
   node with the lower node id (byte order) keeps the name; each other node is renamed to
   `<stem> (conflict <n>)<ext>` with the smallest `n ≥ 1` that is free. Applies equally to
   files and folders. Recovered files, and an orphan moving into it, lose a clash whatever the
   ids (rule 3). A name held by what waits for a hash (rule 3) isn't renamed or taken: the
   other node waits.
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
   a device that didn't know yet) keeps the folder alive: restored. Made here before its
   restore on the drive, the folder has the removed version as its base until the restore
   settles (another device may restore it first, with changes of its own: a tag it removed
   stays removed). A removed folder outside this
   folder's part of the drive, or one it filters (§10), isn't its to bring back: a node written
   into it waits for the folder that syncs that part, or, filtered, goes where an orphan does.
   A file not hashed yet keeps its folders as they are, neither deleted nor restored, till it
   is hashed (below); then it keeps a folder alive as any live child does.

   **What waits for a hash.** A file here not hashed yet (being written, locked, unreadable, a
   placement a crash left unread) can't be judged: until it is hashed it, and every folder it
   is in here, on the drive or in the base, and every folder those are in on any side, stay
   as they are on both sides, whatever the drive did meanwhile. Nothing is done to the file;
   such a folder is not deleted, restored or moved on either side (made where it isn't yet,
   and its tags, may go): the delete or move waits, it isn't undone. A node that would take
   the file's name or such a folder's, here or on the drive, waits too, never renamed, where
   it is, holding its own folders the same way. Once hashed, all of it merges as ever (an
   edit keeps a deleted folder alive as above, an unchanged file follows the drive). The
   round that finds something waiting reads those of the files that haven't changed since
   the scan couldn't (locked, unreadable; one still being written is read by the scan its
   change brings, or, a large one, by the observer's look once it has been quiet a few
   seconds: below) and plans again; one that still can't be read is told, by name, and read
   again at the next sync (no poll: on Windows an app letting go of a file it didn't change
   tells no one, and the wait lasts till the next sync for another reason). A file that can
   never be read holds what waits for it for as long. A large file being written (8 MiB or
   more, modified in the last 5 s, or changed while it was read) isn't read at every batch
   while the live watcher runs: it is left unhashed, and the observer looks again once it may
   have been quiet 5 s, then 3, 6, 12 … 60 s apart while it is still written to; once quiet it
   is read and syncs as any edit (one never quiet is read anyway every 10 minutes; the
   start-up catch-up reads every changed file at once). Only where nothing in that scan depends on its hash: a new
   file of the size of one that vanished is read at once (a move by hash, §3). At start the
   observer looks again at every file left unhashed (one being written when the client
   stopped, a locked one): a journal's catch-up names only what changed since.

   **Orphans go into Recovered files.** A live node whose folder is nowhere on the drive
   (deleted for good: a stale plan's create racing a delete and an emptied Recently deleted,
   a restore of something whose folder went for good, another app, a hard delete), or a folder in Recently deleted that such a node brings back,
   is moved on the drive into **Recovered files** (one metadata update at its version; a
   mismatch plans again), a folder at the top with a fixed node id (node schema, "Recovered
   files"). Nowhere means on a whole picture of the drive: the feed read to its end (it reads on
   past the rows the server holds back) or kept current by the notifications, so a folder not
   in the mirror isn't on the drive. A folder merely not read yet never counts: a node whose
   folder the picture lacks waits while the picture isn't whole. An orphan in Recently deleted
   stays there, an item of its own; only what is live, or brought back, moves. A live node
   whose folder is a file (another writer set its `groupId` to a file's node id) is moved the
   same way, whether the picture is whole or not; a folder this device can't read, or a file
   whose newer version it can't read, is no such place: what is in it waits. A file here not
   hashed yet is never an orphan to adopt (it waits for its hash, above): adopted from the
   drive's state, its item took the drive's tags over its own.
   - The main folder adopts: a node whose place isn't known is never a role folder's (§12).
     A folder in Recently deleted has a place where its own folder is: a live one, or one in
     Recently deleted in turn, up to the top; what is in it is neither an orphan nor the main
     folder's when that chain reaches a role folder.
   - Recovered files is made when an orphan first needs it, with its fixed id, so devices
     adopting at once make one folder (a create that finds it made joins it). One in Recently
     deleted comes back, alone, as any folder a live node keeps; one synced here before and
     gone for good is made again. It is an ordinary folder otherwise: the user may rename it,
     move things out or delete it, and a deleted one comes back only for a new orphan. Nothing
     removes it when empty (that would only fill Recently deleted).
   - Names: an orphan moving in loses a clash to what is there (`<stem> (conflict <n>)`); the
     folder itself loses a clash at the top too, so a plain folder of the same name keeps its
     name.
   - A device that filters Recovered files (§10), and a role folder (a node written into a
     removed folder it filters), puts the orphan at its top instead; so does every device where
     another writer gave Recovered files' id to a file (a row no device reads, §11 of the
     schema's checks): no device can make it on the drive, and a move into it would wait for
     good, tried again at every sync.
4. **Folder delete propagation** removes the folder on the drive first, then its descendants,
   top-down; here it trashes the topmost folder, its bases and its children's in one step.

## 7. Delete safety

- **Deletes are removals.** A remote delete sets the node's `archivalStatus` to removed and
  keeps its bytes for `keep_deleted_days` (30, never less: the purge is the whole account's, so
  a device may only keep longer): "Recently deleted" lists them, and a restore
  brings them back on every device (node schema, "Recently deleted"). The daily purge deletes
  older ones for good (node schema, "The purge"). The reconciler sees a removed node as
  deleted.
- **Local deletes go to trash.** Removing a local file or folder because of a remote delete
  moves it into the sync root's internal trash (`.hbsync/trash/`), never `unlink` (one rename:
  junk and anything that appears meanwhile go along). The daily purge empties entries older
  than `keep_deleted_days`, of those made since it began. A download that moves a file to
  another folder puts the old version there; where the new one then can't be put in place, the
  old one comes back at once (gone, it would read as the user deleting it, against the drive's
  edit: a conflict, and the download again), and its emptied entry goes. Where that fails, or
  a crash came between, it stays in the trash.
- **Downloads look in the trash first.** A download (a delete taken back, say: a restore from
  Recently deleted) first looks for the file in the trash: in the entry of the node, or of a
  folder above it, under the names the download's place has. It takes one only once it hashed
  it there and it holds exactly the bytes wanted (the drive's content hash and size; one of
  another size or mtime isn't read): its bytes synced, it is put in place from there as any
  download is (the placement record first, the same durability), in one rename. Anything
  else stays in the trash, untouched, and the file is downloaded; so does one whose
  placement doesn't go through (it is still in the trash), and one whose tags couldn't be
  written over the ones it had there (downloaded instead). Nothing is read where no entry is
  there.
- **Fingerprint guard.** Before deleting, overwriting or moving a local item, the executor
  checks that the local fingerprint still equals the one the plan was made from. On mismatch
  the operation is skipped and the node is re-planned.
- **Version guard.** Remote updates, moves and deletes carry the base `VersionTag`. A mismatch
  means the remote changed: the operation is dropped and the node is re-planned with the newer
  remote state. Homebase's delete takes the VersionTag too (odin-core PR #1931, verified in
  `V2DriveControllerBase.PerformFileDelete`); a delete refused because the file is already
  deleted is a retry, and done.
- **Mass-delete guard.** It holds this device's own removals from the drive: when one sync
  would remove from the drive more than `max(50, 25 % of synced files)` files deleted here (by
  the user, or newly ignored, §10 rule 1), or every synced file, those removals are held, sent
  nowhere: the drive and every other device keep them. The rest of the plan proceeds. The user
  answers, about exactly what was shown (a token names the held items; an answer to a set that
  changed since is refused, the new one shown; one taken goes with the next sync, for the
  items it named, and any held since stay held):
  - **Delete**: they go to the drive's Recently deleted, as any delete does.
  - **Put them back**: the delete here is undone, as the drive has them now: each node's base
    goes (with what is below it in the base), so the drive's live node comes down again
    (downloads look in `.hbsync/trash` first); what below it was moved elsewhere here keeps
    its base, and the move goes on. What the drive removed or changed since wins: it is never
    brought back. A newly ignored item stays on the drive and is synced here again where the
    drive has it (masking goes by the drive's path), beside the ignored copy, which stays as
    it is: that folder's disk use twice, until the user removes one.
  While they are held, their names on the drive stay taken: a new item here of such a name
  syncs under a numbered one (`Projects (conflict 1)`, the dialog says so). Nothing held
  expires: the status and the tray show it until answered. Meanwhile what is held isn't planned
  again at every sync: a sync whose last sweep did nothing takes their nodes out of the dirty
  set (the state DB's `held_deletes`), and the guard still counts their files
  with every plan, so a later delete is judged with them. They are planned again, all of them,
  when a plan's view reaches one of them (a change in, above or beside them), on an answer, at
  each start, and once the guard would let this device's own removals through (more files
  synced since). What else stays dirty (refused, failing, waiting for a hash) is planned alone
  once more at the end of that sync: if its view reaches them, or it is more than 1,000
  nodes, they stay dirty, planned every sync as before. Cost: taking them out writes a row and clears a
  mark per node, putting them back writes a mark and deletes a row per node (a change that
  keeps reaching them, a file of their name saved again and again, pays both each time, on top
  of planning them, as before); a sync with
  nothing new while they are out reads the dirty set, the base's file count and the held count
  (5,051 held: about 1 ms, against 80 ms when they were planned each time). Simulator:
  `held_deletes_out_of_the_dirty_set_plan_alike` (a plan without them plans as one with them,
  unless its view reaches them). The guard's counts (synced files, what is pending) are read
  when a sync starts planning. Each page reads the connection's `data_version` with its marks:
  when another connection committed since (an announcement, another folder's pull, this
  sync's own catch-up), what is pending is counted again there, with what that marked, never
  less than the sweep's earlier pages were judged by (what the sync did meanwhile, or a restore
  elsewhere, doesn't let through the rest of what they held); the synced files stay as counted
  for the sweep, but where nothing was dirty at the start they are counted there, so a change
  marked meanwhile is never judged against no synced files. What is held as every synced file
  earlier in the sync stays held with the new counts (engine:
  `a_wipe_marked_right_after_the_counts_is_still_held`,
  `a_wipe_marked_after_the_counts_beside_a_dirty_file_is_still_held`,
  `a_wipe_held_in_one_pass_stays_held_after_the_counts_are_read_again`,
  `a_wipe_over_pages_stays_held_after_a_restore_elsewhere`). Not covered: a mark
  committed between a page's read and its plan's own reads, a pull whose pages are only partly
  committed, and marks this engine writes itself mid-sync (rescans) are judged against counts
  read before them. Cost: a pragma per page; the pending counts again only when something
  else wrote.

  Removals made elsewhere (another computer, the phone, the file browser, another app) are
  followed here without asking: into `.hbsync/trash`, and the drive keeps them in Recently
  deleted for `keep_deleted_days`. One exception: removals that would take every synced file
  here at once, and more than the same `max(50, …)` (a broken device, a wiped drive), are held
  (a move elsewhere out of this device's view isn't a removal: the drive keeps it live). Once
  held so, they stay held until answered, whatever the count says since (one of them back on
  the drive, a put back half done), across restarts (`guard.wiped`, kept as soon as planned):
  a held item stays held while a removal elsewhere is still to be followed in it, or below it
  once it is back on the drive, and a trash of anything held or taking held items along is
  held. **Delete**, or **Put them back**: kept here, and each synced item in them the drive
  no longer has is restored from Recently deleted (each removal as an item: what was removed
  with its folder comes back with it; what was removed apart, before, on its own), or, gone
  for good there, uploaded again from here as it is; a folder already back has what is in it
  looked at. Every device then has them back. Cost: a feed read, then one restore of all the
  removals together: one read, and per wave (what comes back into folders already back) a
  request per 1,000 rows and one write; held file by file (plans in pages, the folders kept
  on the drive meanwhile), that is still one restore (300 loose files: 2 requests, against 301
  when each removal was restored on its own). A row whose name is taken is a request of its
  own, as in any restore. What a failure leaves is still held, and shown again, with a note.

  A confirmation lasts while its removal is pending, or waits for a file in it not hashed yet
  (§6.3: what was in such a folder, sent apart meanwhile, goes with it); done or undone, it is
  forgotten. A removal the user didn't see is never let through by a confirmation. A folder
  without its marker (moved, deleted, a disk not mounted) syncs nothing, so nothing is held,
  sent or put back there: an answer then is refused. Simulator:
  `guarded_clients_delete_only_what_was_confirmed` (with `Event::PutBack`); engine:
  `a_delete_here_is_held_and_put_back_brings_it_back`,
  `every_file_removed_elsewhere_is_held_and_put_back_restores_it`,
  `a_confirmed_folder_trash_does_not_hold_other_deletes`.
- **Online-only files** are never deleted remotely just because they have no local bytes.

## 8. Operations

Local: `mkdir`, `download` (to temp, verify stored hash, atomic swap), `move`, `tags`, `trash`
(what goes takes its bases along, whatever the drive holds by then: a base left behind would read
as a local delete once the node is back in view, and undo a restore made while it ran),
`rename-for-conflict`, and for filters (section 10) `detach` (a new node id in place) and
`forget` (trash plus dropping the base, also where nothing of it is here). Remote: `create-folder`, `upload` (create by node id),
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
(4105) is treated as already done, with the bytes the file had then: they count as agreed only
where the drive's hash is the local one, so an edit since goes up; a local move whose source is gone and whose target holds the
expected fingerprint is treated as done.

## 8a. Execution rules (found necessary by the simulator)

- **Settling.** After each successful operation the base takes each attribute (location,
  content) that both sides now hold; never a value only one side has. A plan that stops half
  way can therefore never make the next round revert what did succeed.
- **Answers.** A remote write (create, upload, rename, move, tags, removal; and outside plans
  a restore, the purge's removals and put-backs, the role folder made again) is recorded from
  its answer, over the version of the file it replaced. Another engine or an announcement may
  pull a newer state of the file after the write landed and before its answer is recorded (a
  file's versions follow each other: anything but the replaced version or the answer's came
  after the write, as a header's write is conditional on its version tag, verified in
  odin-core; a delete for good, or a version this client can't read, count as newer too). The
  answer then stays out of the picture of the drive, which keeps the
  newer state, and the node settles against the answer, as if that pull came after it: the
  next plan reads the newer state as any device's change (another device's removal of a file
  just renamed here takes it to the trash here too; a restore after a removal here brings it
  back here). The check and the write of the answer, and the settle, are one transaction,
  which no pull's commit comes into: writes to the state DB take turns (its process's gate,
  and SQLite's write lock across processes). The account reads the feed again at the next
  sync then (a page fetched before the write landed may be what came first).
- **Refused.** An operation the drive refuses for good (a request it won't take as it is: a
  bad request, a content it won't hold, a hash it won't have; a file whose bytes don't match
  what the drive says they are, twice at one version) is recorded, keyed by its node, its kind
  and everything it would send or fetch (its parent, name, tags, the version it acts on, the
  local bytes it uploads). The plan leaves out an operation whose key matches, and what waits
  for it (what goes into a folder whose create is refused, the removals in a folder whose
  removal is, what goes to the local place the refused item doesn't leave, and the trash or
  forgetting of a folder it is in): it stays dirty, but isn't sent again, nor counted as
  unfinished work. Something else blocked behind it is tried and skipped, a local cost, told
  when it keeps coming back. Any change
  of the node, here or on the drive, is another key, and is sent. A refusal lasts an hour at
  most, and until Sync now; the operation done forgets it. A download whose bytes didn't match
  once reads the drive again first: it may have been written meanwhile.
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
  removes it (or, where no device does, the purge: node schema, "The purge", 2.); meanwhile
  §6.3 takes it as deleted with the folder by its time.
- **Together on the pool.** Transfers and remote writes (folder creates, renames, moves, tags,
  deletes) are collected and run together on the worker pool, in any order. An operation that
  depends on one collected waits until they all ran: a create or move into a folder created
  (or restored) in the batch, a move into a folder another move in the batch carries along (the two could
  make a cycle on the drive), a folder's delete while the batch moves a node into or out of
  it, a delete while the batch removes its folder, the same node twice. Anything else (local work, settling) runs on its own, after the
  batch. A remote outage starts no further writes; those failing together count as one
  failure, and the rest run again after the pause.
- **No starvation.** The dirty set is planned in pages, in node id order, swept round after
  round and across syncs, so nodes that stay dirty (deferred, failing) can't keep the rest
  waiting; held deletes leave it till answered (§7). A sync ends after a full sweep in which nothing was done. A round clears only
  the marks it read with its page (a change of the drive's picture moves a mark's version on):
  a node another engine's pull or an announcement marks again meanwhile stays dirty, though it
  was dirty already, and the change it brought is planned at the next sweep.
- **Names that can't be spelled** locally (a Windows device name at the length limit) are
  skipped and reported, not retried; conflict names are shortened to fit 255 bytes.
- **Conflict copies are write-ahead.** The fork is recorded before the rename and the record
  is removed with the DB update after it; recovery runs before any scan and completes or
  forgets an interrupted fork. A copy id derives from the node, the local bytes, the base's
  remote version and the computer (section 5): stable when one conflict is replanned there,
  new for the next conflict, and another computer's copy is another node. If the copy's name is
  held in the local folder by something that stays, the fork takes the first free numbered
  name, and a later plan moves the copy where it belongs.
- **Case-insensitive volumes** (probed once): a name's occupant is any sibling with the same
  name key; a node may take its own name in another case.
- **Orphans.** With a fully drained feed, a remote node whose parent is unknown everywhere (a
  child created while another client deleted its folder for good) is moved into Recovered
  files (§6.3), made first where it isn't there. So is one whose parent is a file.
- **Rows that aren't nodes.** A version that doesn't decrypt, that uses a fixed id out of place
  (the root's, or a role folder's or Recovered files' on a file), or that changes a node's kind
  is left out, reported, and recorded as there all the same (node schema, Identity and
  tombstones):
  - What is in a folder this device can't read stays in it, waiting: it is never an orphan.
  - A node whose newer version it can't read is held as it had it: a local edit goes up as a
    new node (a conflict copy, under a free name); a rename, tags, a delete or a download of it
    is refused and not tried again while it is the same; the drive's version stands.
  - A later version that reads, or the file's delete, ends it.
- **Crash recovery** needs no operation log: every operation is idempotent or detectable
  (4105 on a re-create, fingerprint guards locally), state is re-derived from a scan and the
  feed, and parked items come back from their records. A download (or a folder synced before,
  made again) placed but not recorded (a crash just before its commit) is found again: each
  batch of downloads is recorded before it runs, with the base each commit writes (the node as
  the drive has it at that version: for a folder, the version its plan saw, else the mirror's;
  one nowhere live, made again from Recently deleted, records the version there for recovery
  only, its commit keeping its base), its target, what the item holds of tags and what the item
  there held before (none: not known), and the base's version its plan saw (recovery checks it as the commit does; a record
  from before records said so is settled against the base as it is). A download claims the tags it writes on the file before moving it into place, the
  empty set too, as they read back (not known where the volume keeps none, learned when a tag
  write finds it unsupported); where the write turns out otherwise, the record says so durably before the move,
  so only a failure costs a commit. A folder is made without tags: it claims the empty set if it
  is to hold none, and otherwise claims what was written once written, durably before its
  commit. On the next sync the item at a record's target holding its bytes, new to the scan or
  the node's own file replaced, is that node, settled as its commit settles it (one function for
  both, against the same version), its tags as the record says the write changed them, a tag
  the user changed since being their change; where it claims none, the base's tags start
  afresh, each one the item holds and the drive holds too, so a tag only the drive has is
  written on the item and one only the item has goes up. Where the mirror no longer has the
  node as the plan reads it (deleted, in Recently deleted, or taken along by a folder there in
  this folder's part, §6.3), the commit and
  recovery alike settle the item at the record's version whatever the plan saw (its recorded
  spelling and the rest of the base as any settling keeps them), so the removal reaches it:
  never read as an edit, which would bring the node back; one whose item isn't the drive's to
  take (its name) is held as a file not hashed yet is, then goes. A folder's recovery, like its commit, marks what is in it on
  the drive dirty. Later changes on the drive are plain changes against it. A record goes once
  its base is written, by the commit or by recovery, or when the operation that wrote it, in
  that run, fails before its own item is in place, or its batch stops before it starts (so no
  item the user puts at that place is ever taken for it); one whose item is in place stays
  through a failed commit: where the state DB fails, the run stops and the next sync's
  recovery settles it; where reading the item back fails, the operation ends there and
  recovery settles it right away by that item alone, no other item read again (the one way an
  item put in place settles then; a folder new here, without a record, is taken over by its
  retry). One whose item can't be told yet (its file not hashed, say: locked; its folder
  unreadable) stays for ten minutes from when recovery first found it so, then goes. Recovery
  runs at the start of a sync and of each of its rounds, and first rescans the folders its
  records go into and those their nodes' items are in (each record's once a sync); both it and
  recovery in the run read the disk at a record's target alike: the item there with the
  record's bytes is settled, a record whose target verifiably holds nothing (not found, a file
  where a folder would be, a loop of links) goes, and one whose target holds another node is
  left to adoption. A record never goes only because no scan has seen its item yet. An item
  at its target whose bytes are neither the record's nor the base's (the user edited it before
  the next sync) can't be told from the file that was there before (the crash may have come
  during the transfer, before the move): the record goes, and the item reads against the old
  base, a conflict copy where the drive changed too; the tags the download wrote read as this
  device adding them (a known limit, §11). No operation writes over a
  record it didn't write: it is left for the next plan, the node dirty, until recovery settles
  the record. A verified download that couldn't be
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

## 8b. Gone for good

A file deleted for good is forgotten here (out of the mirror and Recently deleted) and its id
kept 30 days, so no stale row brings it back; what this device holds of it is then deleted
here (or put up anew, if edited). That takes proof (`hbsync-engine/src/gone.rs`):
- **Proof on its own:** a tombstone (a 4002 answer, a header that says deleted, a tombstone in
  the feed), and a hard delete the feed or the notifications announced.
- **The identity's "not there"** (its own 404 for the file: a header read after a write, a
  restore, a delete for good or a re-read found it not there) is a suspicion, kept in the state
  DB (`gone_suspects`, so a restart keeps it) with the version this device knew and the time
  (this device's clock). The file counts as gone only once the identity says so again, a minute
  to an hour on, at that version; one later than that, or one by a clock set back, is a first
  again. Meanwhile the operation is skipped (stale) and the file stays; a restore of it fails,
  to try again (what was to come back into such a folder is asked about too, a header read
  each, so that a minute on all of it is gone at once); a put back (§7) that fails is run once
  more a minute on; emptying Recently deleted leaves it listed until emptied again a minute on;
  and what the purge finds so is asked again a minute on, a header read each (a delete for good
  this device missed: it leaves nothing in the feed). A new row of the file (the feed, a write's answer) or a header that says
  it is there clears the suspicion. One wrong 404 for a moment (a server's hiccup) then never
  deletes a live file here, nor hides it for 30 days. Costs, per file deleted for good without
  a trace that this device finds by a request: a header read more, a minute's delay, and the
  syncs within that minute asking again (each the write, a header read and a feed read); and
  one indexed delete per new feed row (none for a row the mirror has).
- **Only from the identity:** a 404, a 410 or a "not there" or "tombstone" error code counts only
  when the identity itself sent it (`X-Odin-Version` and a sealed or ProblemDetails body of its
  status, homebase-api-notes): a proxy's or a captive portal's is a failed request, asked again.

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
6. **Gone for good** (section 8b): with the drive saying now and then that a live file isn't
   there, once of each, no client takes a file the drive holds as gone for good, nor deletes
   it here; the clients converge without loss.

## 10. Filters

Some items are never synced. Filtering is per device and works in both directions: a filtered
item is invisible to this device's sync, locally and remotely. Ignoring never deletes anything
on its own.

**Classes** (`hbsync-local/src/filter.rs`):
- **Excluded**: the client's own `.hbsync` folder at the top of the sync root, in any case or
  spelling a volume that ignores case takes for it (`.HBSYNC`): another writer's folder so named
  would be this client's own there, and what is in it would land in it.
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
   (their trash), and stays here. The mass-delete guard applies (§7): held over its limit;
   put back, it stays on the drive and is synced here again where the drive has it.
2. **Remote.** A remote node whose path is filtered on this device is masked with its subtree:
   never downloaded, and never deleted by this device, with one exception: a node this device
   synced at a place it now filters (a rule was added) is leaving per rule 1, and is deleted
   remotely unless the remote changed it since: its place, bytes or tags (a folder, or a file
   another writer put something in: unless it holds anything that isn't
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
   checks the ignore file for an edit when the watcher sees it change (once a minute without
   a watcher) and on a sync request, and starts over with the new rules when it changed.

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
the local file's tags. Every write over a drive version merges that version's tags in (it may
hold tags this device never saw): a restore from Recently deleted, bringing back a node a
folder's removal took along (§6.3), any update; a version detached as a new item writes over
none, and keeps the local tags. What the device can't hold it writes as that version has it:
its plan took those from the drive it saw, which may be older (a restore planned while the
node was deleted, sent after another device brought it back with a tag and deleted it again).

**Local changes** are noticed by the change time a tag write moves (scan, watcher); the scan
then reads the tags. Tags that couldn't be read are tried again at the next scan.
Downloads write the tags onto the temp file before it is put in place: a file without them
would read as the user removing them. An item with more than about 2,000 bytes of tags
isn't tag-synced: it reads as "can't tell", and keeps what the drive has; so do tags that can't
be read or written for a moment (a failed tag write never fails the operation). An item whose
tag write fails (a tag change, a download, a folder made) holds what it holds, as read, and its
row says so: never "can't tell", which reads as the drive's tags, so the next scan took their
absence for the user removing them, and a tag only the item had never went up. Where it can't
be read back either, it is taken to hold what it held before the write (a failed write
changes nothing): a file or folder made here none, a folder taken over what was read of it
(none where that couldn't be read either: claiming less than an item holds only sends the
rest up); a tag change's row stays as the scan read it, its change time too, so the scan reads
whatever changed since. Recovery still reads an item whose tags it can't read as not known: it
can't tell that from tags too big to sync, which keep the drive's. A placement whose tag write
failed claims nothing in its record (not known), and the next plan writes the drive's tags
again. A folder synced here before and made back from a removal (for a child written into it,
its restore to follow) has no record, and keeps its base: but for a tag the failed write didn't
put on it that the drive's last version holds (in Recently deleted; the base's own where that
is not known), which would read as the user's removal. One that version lacks stays in the base:
its absence is the drive's removal, which the restore merges in. A download whose failed write
would lose a tag the file it replaces holds and the drive doesn't (the user's, not sent up yet)
isn't put in place: it is tried again, the plan sending the tag up meanwhile, unless the drive refused that
for good in the run (access, read-only): then it is placed. A node with no
base gets one when place and content agree, with the tags both sides have: the merge then
brings each side the other's (a copy proven equal only across hash algorithms can't be proven
again later). A tag write needs a base to record what it wrote: the item's own, or one it
founds (place and content agree with the drive's version it acted on). Without one it waits,
the node left for the next plan: the drive's tags it wrote would read as this device adding
them, and a tag removed on the drive meanwhile came back. The plan's move or remote write
founds the base; where it runs first, the tags come down in the same run. A folder made here
from Recently deleted (never synced here, its restore to follow) founds its base on the version
there, as found before the write (a pull landing before its commit doesn't change what it settles
against); where that no longer holds the tags it is to write (restored, changed or purged since
the plan), it waits for the next plan.

**Limits.** Tag changes are seen through the change time: on a volume that keeps attributes
elsewhere (exFAT's `._` files) or with one-second times (HFS+), a change can go unnoticed until
the item changes again. A tag change makes the scan read the file again (the change time is
part of the fingerprint, to catch writes that keep the modification time). An item that becomes
a new node on a device that can't hold all its tags (detached on Windows, §10) goes up with
the tags that device has. A download put in place whose commit a crash lost, its file edited
before the next sync, is read against the old base (§8a, crash recovery): a tag the download
wrote reads as this device adding it, so a tag another device removed meanwhile can come back.
No tag is lost, only the removal; the simulator allows exactly this.

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
  drive, a cycle) isn't a role folder's: the main folder repairs it (§6; into Recovered files,
  §6.3); one that sits in a
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

A detached item's new id is fixed for the node and the computer (the state DB's own id, as
for conflict copies, section 5), so a replan picks the same one,
but never one in use here: the same node detached again while its first copy lives gets the
next.
