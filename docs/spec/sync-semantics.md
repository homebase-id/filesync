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

**The executable bit.** Where a device doesn't hold a file's `x` (Windows never does), it
follows the drive's, as with tags (§11): it never reads the bit back from disk, an edit sends
the drive's `x` on (the base's if the drive has no row, else false), and a change of `x` alone
on the drive changes nothing there. On Linux a launcher (a name ending in `.desktop`, any case)
is such a file: file managers start an executable launcher on a double-click, under whatever
name and icon it gives itself, so its bit is the user's trust in it, given on that computer. A
launcher arrives not executable, and the user's own `chmod +x` stays on that computer: a
launcher made here goes up with `x` false, and the drive's `x` stays whatever another device
set. A move that changes whether the bit is held gives the file what it holds now: a file
renamed into a launcher loses its bit, a launcher renamed into another name takes the drive's.
Launchers that an earlier version put in place executable stay so.

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

**Words.** In these rules a *held* delete, removal or item is one the mass-delete guard keeps
back until the user answers (§7); nothing else is held so. The app shows them as *deletions
to review* (the tray, the CLI); its "waiting" means items not synced yet. What a plan leaves as it is,
nothing done, until something else happens *waits*: for a hash (a file here not hashed yet,
that is being written, locked or can't be read; the folders it is in; what would take a name
it holds: §6.3), for another page (§6.5), or for a version this device can read (§8a). An
operation the drive refused, and what waits for it, is left out (§8a, Refused). A file whose
newer version on the drive this device can't read is *unreadable* (§8a). An item, a folder or
a row of Recently deleted *holds* what is in it, and a node holds its name (a name is held by
the node that has it).

**Content state** (per file node, local only, never synced): `local` (default; may be evicted),
`pinned` (always kept locally), `online-only` (no local bytes), `wanted` (no item here yet: its
item went into a conflict copy, §5; the drive's version comes down). A missing local file is a
delete only when the base says the bytes were local.

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

**Hash on send.** A new file (rule 4) that rule 3 can't match (no vanished file of its size)
isn't read by the scan: it is hashed as it is sent, so its bytes are read once. While it is
*free*, with nothing its bytes could be compared with (no base, its node not on the drive,
nothing on the drive in its folder of its name, not parked), the planner takes it as a new
file and uploads it unread; the upload checks its fingerprint before, every 16 MiB and after
the last byte (as `hash_file` checks it), and the hash of the bytes sent settles the base and
the local row in one commit (the row only if it still shows that fingerprint, else the next
scan hashes it). One that isn't free (the drive got its node, a write's answer lost, or
something of its name) is read before it is planned, as a file something waits for (§6.3),
and so is one a first run may adopt. A base claims what it always does: both sides hold the
bytes of hash H, here the hash of what the drive got. One being written when found is left
unhashed and read once quiet, as ever (§6.3).

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
| edited / moved | deleted | **edit wins**: the removed node is restored remotely with the local version (an update at its file id: node schema "Recently deleted"). If Recently deleted holds a version of the file this device never had (edited elsewhere, then deleted; or taken along by a folder's delete, §6.3): other bytes than agreed, or an executable bit changed elsewhere that this device doesn't send (§1), that one stays there and the local version goes up as a new node (detached in place) |
| created | created | impossible for one node id; same-name clashes are handled by section 6 |

A `moved` that only reverts a delete case is applied to the restored node.

## 5. Conflict copies

When both sides edited a file to different content:
- The remote version keeps the node id, name and location.
- The local version is renamed in place to `<stem> (conflict <device> <YYYY-MM-DD>)<ext>` and
  becomes a new node that is uploaded normally.
- No bytes are ever dropped: the local bytes are moved by rename, never re-written.
- The fork marks the node's base *wanted* (in its commit, and in its crash recovery): the node
  has no item here, and the drive's version comes down.
  - *Absence*: its missing item never reads as a delete here, not even with its folder: a
    folder deleted here keeps it (and is made again with it), as it would a change of the
    drive's it hasn't seen (§6.3): the drive's version was never here to delete.
  - *Settling*: the base stays for the run's other writes of the node (a move, its tags), which
    merge against it, and follows the drive's versions while no item is here. A write that puts
    an item here settles against the version it acted on however the base moved, its tags from
    the drive's (the item that held the base's went into the copy); from then on it is an
    ordinary base.
  - *Elsewhere*: a wanted node may be adopted (the drive's bytes put back here by hand), is
    masked at a place this device filters (never leaving, §10), and isn't counted by the
    mass-delete guard (§7).
  - *Recovery*: one that finds neither the item nor the copy (the user deleted one of them
    before the run) can't tell whether the rename happened: it takes the node as forked, and
    the drive's version comes back.
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
   `<stem> (conflict <n>)<ext>` with the smallest `n ≥ 1` that is free and that this device
   syncs (§10: one it ignores would leave the drive at the next scan), of the first hundred,
   as the executor's numbered names; else the smallest free one (simulator:
   `regression_a_clash_name_is_one_this_device_syncs`). Applies equally to
   files and folders. Recovered files and Desktops, and an orphan moving into Recovered
   files, lose a clash whatever the
   ids (rule 3). A name that what waits for a hash holds (rule 3) isn't renamed or taken:
   the other node waits.
2. **No cycles.** If merged moves form a cycle, revert the local moves taking part in it (the
   remote side has already published its moves). Revert one at a time until no cycle remains.
   With no local move in it (the drive took two devices' moves, each folder into the other),
   the lowest id goes back to its place on the drive, else in the base, and to the top where
   that would close the cycle again, under its name (reconcile
   `a_cycle_with_no_local_move_puts_the_lowest_at_the_top`). What is in a cycle the drive
   itself holds (another writer's) is filtered as anywhere once its folder is connected again
   (§10: no mask judges what the drive doesn't connect to the top): an item at a place this
   device doesn't sync stays out of the plan, as below an orphan recovered. Brought in, it
   read as an item the device stopped syncing and left the drive (engine and simulator
   `what_a_cycle_on_the_drive_holds_is_filtered_as_anywhere`, milestone seed 829).
3. **No orphans.** A live node whose merged parent is deleted resurrects the parent chain:
   - remote-deleted parent → restored remotely (still removed) or re-created with the same
     node id and its base name (deleted for good);
   - local-deleted parent → re-created locally.
   Any live child keeps its folder alive, but one a folder's removal took along doesn't: a
   deleting client removes the folder first, then what is in it (node schema, Recently deleted),
   so while it does, a removed folder holds live nodes. A remote node in a folder in Recently
   deleted (directly or through live folders) written no later than the folder's removal, by
   the server's times, is remotely deleted with it; one written after (an edit or a new file from
   a device that didn't know yet) keeps the folder alive: restored. A folder a plan keeps so
   (or by its own change here) keeps what its removal took along, still live on the drive and
   unchanged here, as restoring a folder restores what was deleted with it (node schema,
   Recently deleted), whether the plan holds those nodes or not; one changed here stays taken
   along (§4: the drive's version stays with the removal, this device's goes up as a new
   item). A folder below it that the same delete already removed isn't brought back by this
   (a restore by a write restores the folder only), nor what that one took along. Made here before its
   restore on the drive, the folder has the removed version as its base until the restore
   settles (another device may restore it first, with changes of its own: a tag it removed
   stays removed). A removed folder outside this
   folder's part of the drive, or one it filters (§10), isn't its to bring back: a node written
   into it waits for the folder that syncs that part, or, filtered, is left to the devices
   that sync that place (the orphans' rule below, "Nor is what is in a folder in Recently
   deleted …", says when).
   A file not hashed yet keeps its folders as they are, neither deleted nor restored, till it
   is hashed (below); then it keeps a folder alive as any live child does. A new file free to
   be hashed on send (§3) is no file not hashed yet: it is a new file, nothing compares its
   bytes, and it keeps its folders alive (restored with it) as any new file does.

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
   never be read keeps what waits for it waiting for as long. A large file being written (8 MiB or
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
   - A device that filters Recovered files (§10), and a role folder (a node in a file, or one
     it holds in a removed folder it filters), puts the orphan at its top instead; so does
     every device where another writer gave Recovered files' id to a file (a row no device
     reads, §11 of the schema's checks): no device can make it on the drive, and a move into
     it would wait for good, tried again at every sync.
   - Only into a place the device syncs: an orphan isn't connected to the top, so the filter
     (§10) never masked it. One whose place (in Recovered files, or at the top) the device
     doesn't sync is left as it is, out of the plan, for a device that syncs it; so is what is
     below a recovered orphan at a place it doesn't sync (an ignored name), as the filter masks
     it once its folder is connected again. Brought in, it would read as an item the device
     stopped syncing, and leave the drive: another device's files, deleted by one that never
     synced them.
   - Nor is what is in a folder in Recently deleted at a place the device filters (masked,
     §10) its orphan: something written into it (a live child, which keeps it) that the device
     holds nothing of is left as it is, the business of the devices that sync that place, which
     bring the folder back with it. Taken for an orphan, it was moved into Recovered files on
     the drive, and the folder stayed deleted for the others. One the device holds goes as
     before. Where no device syncs that place, such a node stays inside its removed folder,
     ignored everywhere and never deleted: the purge deletes removed nodes only, so it keeps
     the folder, restorable, while it holds a live node.
   - Nor is a folder made again (rule 3's "re-created") at a place the device filters since
     it synced the folder there (its rules changed, §10, and before its next sync the folder
     was deleted for good with a node live in it): it knows the folder from before only,
     which no mask covers. It forgets the folder, what it keeps there staying its own, and
     the node is an orphan like any other, in the same plan: as on a device that never knew
     the folder. Made again, the folder took over the item the device keeps to itself, the
     node came down into it, and left the drive as one the device stopped syncing.
     So too for a node the device synced there before the rule (it holds a base of it; its
     item there is its own since, §10 rule 1) that another device changed in that window,
     which keeps it on the drive: an orphan, into Recovered files in the drive's version,
     the item the device keeps left as it is. Its folder made again, the node couldn't come
     down onto the name of that item, and was tried again at every sync, for good, where no
     other device repaired the drive (#427). A device that still syncs the folder makes it again for
     the node (rule 3): whichever writes first decides where the node is, and the other
     follows (the folder back on the drive is masked on the device that filters it). And
     below a folder that is live on the drive and masked, the folder and the node wait, as
     before.
   - A folder that leaves the picture for good (deleted outright, announced) while nodes are
     live in it plans those again: they are orphans now, whichever of them changed.
4. **Folder delete propagation** removes the folder on the drive first, then its descendants,
   top-down; here it deletes the topmost folder, its bases and its children's in one step (§7).
   What the plan moves out of that folder first (a move, a download to another folder) is left
   out of that step, with what is below it: a delete inside a folder moved out is a step of its
   own there, after the move. Where such a move doesn't run, the folder holds what its delete
   doesn't expect: the delete goes stale and takes nothing, for the next plan.
5. **Planned in pages.** The engine plans its dirty set in pages of 20,000 nodes, in node id
   order (§8a). A node dirty in another page, or a held delete (§7), is left as it is: its
   change isn't merged in this plan, and no fix here moves, renames or adopts it.

   A folder's delete is planned as one. A folder that may go (synced before or taken along,
   and deleted or moved on a side) waits, left as it is with the folders above it and with
   everything of the page below it (and the folders each of those is in now), when something
   below it on any side is dirty in another page, or when it is itself planned in another page
   and is above a node of this page (then only it waits: its page covers the page's folders
   below it). A held delete out of the dirty set below it doesn't count: as in one page, it is
   planned again only when a view reaches it. Planned apart, what is below read alive as
   context and kept a deleted folder alive; or the folder's delete took along what changed
   there (an edit lost); or its items went one by one before it, each an
   item of its own in Recently deleted. A node that waits so is left as it is on every side,
   nothing done to it (not even made where it isn't yet), and never brought back here as an
   orphan, nor its folder: its own page decides.

   What waits is planned next as a page of its own: the folder with its whole dirty subtree,
   on every side. A subtree too big for a page goes as its folders' subtrees first (several
   together while they fit a page), then the folder once more; there they don't wait for the
   folder above them, but what its removal on the drive took along (§6.3), unchanged here,
   does: left as it is, it waits for the folder's plan, which decides whether the folder
   stays and so whether that does (a folder kept keeps what its removal took along). Planned
   first, it went with the removal while the folder came back. Such a node, dirty (a tag
   here, say), is planned in the folder's last page: about 1 KB more there, and a plan more
   in its piece, a node (simulator:
   `regression_what_a_removal_took_along_waits_for_its_folder_in_pages`; engine:
   `what_a_held_removal_took_along_waits_without_a_spin`). So does one deleted here in a
   piece, at the same cost a node: removed there, it went to Recently deleted apart from the
   folders it is in (those not dirty aren't in the piece), which the folder's last page
   removes. That page has no size limit: a large folder deleted here while a removal of it
   on the drive took its files along is planned there whole, about 1 KB a node (rare: another
   device's removal of it stopped part way, and the folder deleted here too) (engine:
   `a_file_deleted_here_in_a_piece_goes_with_its_folders_to_recently_deleted`,
   `a_file_deleted_here_that_a_held_removal_took_along_waits_without_a_spin`; simulator:
   `regression_a_file_deleted_here_in_a_piece_waits_for_the_folder_that_took_it_along`). What is left then (more than a
   page directly in the folder, and what so waits) is planned whole, as one larger page. So the drive removes the
   folder first, then what is in it, and Recently deleted lists it as one item, or as its
   folders' items and then its own: never a file of it apart. What waits is planned before
   the sweep ends (a sync that stops, at its round limit say, goes on with it), each tried once
   a sweep (engine: `a_sync_plans_what_its_pages_wait_for_before_it_ends`; simulator:
   `regression_a_chain_of_waits_across_pages_is_planned_before_the_sweep_ends`); what a page of its own planned isn't
   planned again in the sweep's own pages (its plan stands), and a page so passed over costs
   no round. Cost: a plan takes about 1 KB a
   node at its peak, so 20 MB for a page; a flat folder of 100,000 items is one page of about
   100 MB, for that page only. Measured: 20,000 nodes peak at 24 MB (0.3 s), 100,000 at
   100 MB (2 s), 200,000 at 200 MB (5 s). Where the page isn't all there is to plan (a
   count), which of the view's nodes outside it are still to plan costs a query per 500 of
   them (100,000: 90 ms), and a page's topmost folder that may go a walk of its subtree.

   Clashes across pages are planned the same way. A node whose name a node of another page
   holds, one going into a folder another page makes or that waits (for another page, or for
   such a wait), or onto a name such a node holds here now (a swap), or one whose folder
   another page decides, waits, left as it is (nothing sent to fail meanwhile). One going
   into a folder that waits so waits with every folder above it that does too, at once: a new
   file deep in new folders isn't found a folder a page. So does one in
   a cycle with a node another page plans (a folder and the folder in it swapped here), left
   where the drive has it: no move of either is undone for the cycle, which one page plans
   whole (engine: `a_swap_of_a_folder_and_the_one_in_it_planned_in_pages`, also beside
   deletes the guard holds; simulator:
   `regression_a_swap_of_a_folder_and_the_one_in_it_planned_in_pages`). It is planned
   with the other node, as a page of its own; one that still waits there is planned again
   with what it waits for, one larger page. A folder such a wait keeps (a node in it moves
   out into such a place, or into a folder that waits for its own page), that the plan would
   delete, goes with that page too: what else
   the plan deletes in it waits with it, so the folder goes as one, as in one page. Only
   while something waits with a node outside the page: each such page is larger, so it ends;
   what still keeps the folder then isn't another page, and what is deleted in it goes as
   before (engine:
   `a_folder_kept_for_another_page_goes_as_one_to_recently_deleted`; simulator:
   `regression_what_a_folder_waiting_for_another_page_deletes_waits_with_it`). Deleted first,
   it was listed in Recently deleted apart from its folder. A removed folder that waits so
   (it comes back into a folder another page makes, say, or is in a cycle with a node another
   page plans) decides whether what its removal took along stays: what of that, unchanged
   here, the page plans waits with it, planned together (engine:
   `what_a_removal_took_along_waits_with_its_folder_placed_with_another_page`,
   `what_a_removal_took_along_waits_with_its_folder_in_a_cycle_across_pages`; simulator:
   `regression_what_a_waiting_folders_removal_took_along_waits_with_it`). Planned first, it
   went with the removal (a tag added here lost) while the folder came back. A group another
   holds is planned in that one. A folder whose delete waits for a
   hash still goes item by item (§6.3). Simulator:
   `clients_planning_in_small_pages_converge_without_loss` and
   `planning_in_small_pages_ends_as_planning_whole` (pages of 1 to 4 against one page: the
   drive's folders and files, and the files Recently deleted lists apart from their folders);
   engine: `a_delete_here_in_pages_goes_as_one_to_recently_deleted`,
   `a_change_deep_in_a_folder_removed_elsewhere_keeps_it_in_pages`,
   `what_was_moved_into_a_deleted_folder_goes_with_it_in_pages`,
   `a_node_moved_out_of_a_waiting_folder_keeps_its_new_one`,
   `what_goes_into_a_folder_waiting_for_another_page_waits_too`,
   `a_held_delete_below_a_removed_folder_doesnt_hold_it_for_good`.

## 7. Delete safety

- **Deletes are removals.** A remote delete sets the node's `archivalStatus` to removed and
  keeps its bytes for 30 days (not a setting: the purge is the whole account's, so every client
  keeps 30): "Recently deleted" lists them, and a restore
  brings them back on every device (node schema, "Recently deleted"). The daily purge deletes
  older ones for good (node schema, "The purge"). The reconciler sees a removed node as
  deleted.
- **Deletes from elsewhere are checked, then final.** On the computer where the user deletes,
  Filesync touches nothing: it sees the item gone and removes it on the drive. On every other
  computer, an item removed on the drive (or moved where this device doesn't sync it, §10) is
  deleted here outright, and nothing is kept: it is renamed into `.hbsync/aside/` (one rename,
  on its volume), checked there to be exactly what the plan saw, then unlinked, and its rows and
  bases go in the commit after. A file is checked by its size and mtime and, where ids are
  stable, its id (its ctime moved with the rename), and also by its hash where those can't prove
  its bytes: a volume whose mtimes are coarse or whose ids aren't stable (FAT, exFAT, HFS+, ext2
  and ext3, a network or FUSE volume; probed once per folder). A folder is first looked at where
  it is (its items' metadata, one walk): a change, or something that stays here, is found
  without moving it, and it stays in place while that walk runs. Aside, it is checked again in
  one walk that judges ignore rules at its place: every item in it must be one the plan saw,
  unchanged. Then it is unlinked deepest first, each file only while it is still the one checked
  (read again, as the walk read it), each folder only once empty. Anything else found (an edit
  through a handle an app had open, a new item, an ignored one) puts it back where it was (under
  a numbered name if that is taken, at the folder's top if its folder is gone), and the next
  plan decides: an edit wins (§4), ignored content stays (§10). Junk files go along; a folder on
  the junk list (it may hold what was never synced) stays, as ignored content does, and as a
  state folder below the top does (`.hbsync`, another sync folder's: §10). A parked
  item goes back under its own name, never its temporary one, and its parking record goes. While
  an item is aside, the scan keeps its rows, and the folders it is in, for it to come back to.
  What the drive removed stays in Recently deleted for 30 days; a restore downloads
  it again. Cost per file: a rename and an unlink, a stat more than the guard's, and on such
  volumes a read of its bytes; per folder, one walk more (two in all: in place, then aside).
  Accepted: an app that opened a file before the rename and writes to it in the microseconds
  between the last check and the unlink loses that write (any delete has this window; Windows
  refuses the rename while a file is open without delete sharing). Simulator:
  `remote_deletes_never_lose_an_edit`, `a_crash_anywhere_in_a_delete_loses_nothing` (with apps
  writing to what is aside), and every property's check that nothing is unlinked that isn't the
  version its base records, and nothing stays aside.
- **Old versions.** A download that moves a file to another folder moves the old version
  aside first (`.hbsync/aside/`), puts the new one in place and, once that is committed,
  unlinks the old one if it is still what was there (checked as a delete checks a file: on a
  volume whose metadata can't prove the bytes, a read of them); anything else leaves it aside
  for the next sync's recovery (§8a). Where the new one can't be put in place, the old one
  comes back at once (gone, it would read as the user deleting it, against the drive's edit: a
  conflict, and the download again). Nothing is kept, and a download never looks for its bytes
  on this device: a restore from Recently deleted downloads them again.
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
  the user, or newly ignored, §10 rule 1), or every synced file (two or more), those removals
  are held, sent nowhere: the drive and every other device keep them. 50 and 25 % are the
  defaults (`[mass_delete]` `min_files` and `fraction` in `config.toml`, daemon.md). In pages
  (§6.5) the synced files gone here are counted over the whole sync, and with them the files
  left live on the drive in a folder another device removed (taken along, §6.3) that this
  device removes from the drive: those synced here, and those not synced here but marked here
  (by the feed, or held while this device's delete of that folder took their bases). A delete
  from the drive of a file not synced and not marked here is weighed within its own plan, not
  across pages. Taken along is read by the drive's times alone: where the plan keeps the
  removed folder (a change here in it, §6.3), the count is higher than what the plan removes,
  and holds sooner. A removal elsewhere goes with what it took along: a folder's delete here
  plans the removals from the drive of what in it is still live there (taken along), which run
  first; the delete here waits for them, and one that doesn't land leaves it for the next plan
  (a run cut between leaves a removal elsewhere done on the drive, followed as any: engine
  `a_removal_elsewhere_cut_before_its_delete_here_is_followed_after`). A delete here that goes
  stale after them (an app saving into the folder meanwhile) keeps the folder for what is new
  there; what it took along is removed by then, as the removal elsewhere meant, and goes here
  too, kept in Recently deleted (engine
  `a_folder_kept_by_a_save_during_its_delete_keeps_only_whats_new`). Where those removals
  are held, the folder's delete here is held with them, and a held delete here holds them: one
  item, shown as removed on another device with a removal not finished on the drive (its own
  sentence, not that of a removal of every synced file), answered as one (the folder's answer
  is theirs, and a confirmed folder's delete covers them in the counts). Held so, it is judged again with
  every plan, not kept till answered as a removal of every synced file is: once the removal
  elsewhere is finished on the drive, nothing is left to hold. While the drive refuses one of
  those removals (§8a, Refused), the folder stays here: a removal elsewhere is followed only
  once all of it can be. Cost: a held removal is an operation of its own per file while held
  (about 100 bytes in memory, and a copy in each sync's report), where the folder's delete
  alone was a node id per file.
  **Put them back** restores the folder on the drive with what it took along; **Delete** lets
  all of it through (engine
  `a_removal_elsewhere_is_held_with_what_it_took_along_and_put_back`,
  `a_removal_elsewhere_held_with_what_it_took_along_goes_on_delete`). The rest of the plan
  proceeds. The user answers, about exactly what was shown (a token names the
  held items; an answer to a set that changed since is refused, the new one shown; one taken
  goes with the next sync, for the items it named, and any held since stay held):
  - **Delete**: they go to the drive's Recently deleted, as any delete does.
  - **Put them back**: the delete here is undone, as the drive has them now: each node's base
    goes, with the bases below it of what is gone here and live on the drive, so the drive's
    live nodes come down again; what below it was moved elsewhere here keeps its base, and the
    move goes on. What the drive removed or changed since wins: it is never brought back. A
    newly ignored item stays on the drive and is synced here again where the drive has it
    (masking goes by the drive's path), beside the ignored copy, which stays as it is: that
    folder's disk use twice, until the user removes one.
  While they are held, their names on the drive stay taken: a new item here of such a name
  syncs under a numbered one (`Projects (conflict 1)`, the dialog says so). Nothing held
  expires: the status and the tray show it until answered. Meanwhile what is held isn't planned
  again at every sync: a sync whose last sweep did nothing takes their nodes out of the dirty
  set (the state DB's `held_deletes`), and the guard still counts their files
  with every plan, so a later delete is judged with them. They are planned again, all of them,
  when a plan's view reaches one of them (a change in, above or beside them), on an answer, at
  each start, and once the guard would let this device's own removals through (more files
  synced since). Above them counts through a folder's removal too: a held removal from the
  drive has no item here, so no view holds it unless it is dirty, yet written after the removal
  of a folder above it, it keeps the live folders between them (§6.3). A view with a folder in
  Recently deleted therefore reaches a held delete that is live below it and was written after
  its removal, or is in a folder outside the view that was (read from the mirror, up from each
  held delete to the view; cost: a read per held delete and per folder above it outside the
  view, for a plan whose view has a folder in Recently deleted, none for any other). What else
  stays dirty (refused, failing, waiting for a hash) is planned alone
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
  marked meanwhile is never judged against no synced files. The synced files are a count
  of the base (a scan, 2 ms at 20k files) that each connection keeps in memory while nothing
  can have changed it: no other connection committed since (`data_version`) and it wrote no
  base row. A sync while a file waiting for its hash keeps the dirty set busy reads it for
  nothing; one after its own downloads or uploads counts again, once per sweep. (A row kept on
  disk by triggers cost a page, 4 KB, more in the WAL per file a commit added: 8.5 MB on a
  create of 2,067 files.)
  What is held as every synced file earlier in the sync stays held with the new counts (engine:
  `a_wipe_marked_right_after_the_counts_is_still_held`,
  `a_wipe_marked_after_the_counts_beside_a_dirty_file_is_still_held`,
  `a_wipe_held_in_one_pass_stays_held_after_the_counts_are_read_again`,
  `a_wipe_over_pages_stays_held_after_a_restore_elsewhere`). Not covered: a mark
  committed between a page's read and its plan's own reads, a pull whose pages are only partly
  committed, and marks this engine writes itself mid-sync (rescans) are judged against counts
  read before them. Cost: a pragma per page; the pending counts again only when something
  else wrote.

  Removals made elsewhere (another computer, the phone, the file browser, another app) are
  followed here without asking: deleted here (checked, above), and the drive keeps them in Recently
  deleted for 30 days. One exception: removals that would take every synced file
  here at once, and more than the same `max(50, …)` (a broken device, a wiped drive), are held
  (a move elsewhere out of this device's view isn't a removal: the drive keeps it live; nor
  is a move out of a folder removed elsewhere: the plan moves it out first, and the folder's
  delete expects only what goes, so a move that doesn't run leaves that delete stale, taking
  nothing; engine: `files_moved_out_of_a_folder_removed_elsewhere_dont_count_with_it`,
  `a_move_out_that_fails_leaves_the_folders_delete_stale`). A folder's delete counts every
  file it takes here, synced or not: never fewer than the synced ones. The count is the whole sync's,
  not a page's (§6.5): with the page's removals it takes what other
  pages still have to remove, the dirty ones and the files in folders the drive removed that
  are still live here (taken along, by §6.3's rule; read while anything is dirty: a walk of
  Recently deleted's folders, 2 ms for 50,000 items), less those a confirmed delete here takes. So a wipe planned in pages
  is held as a whole (engine: `a_stopped_wipe_planned_in_pages_is_held`). Once
  held so, they stay held until answered, whatever the count says since (one of them back on
  the drive, a put back half done), across restarts (`guard.wiped`, kept as soon as planned):
  a held item stays held while a removal elsewhere is still to be followed in it, or below it
  once it is back on the drive, and a delete here of anything held or taking held items along is
  held. **Delete** lets the removals through: deleted here (checked, above). **Put them
  back**: kept here, and each synced item in them the drive no longer has is restored from
  Recently deleted (each removal as an item: what was removed with its folder comes back with it; what was removed apart, before, on its own), or, gone
  for good there, uploaded again from here as it is, also what the restore itself finds gone
  for good (looked at again after it: two reads a node restored); a folder gone for good only
  once nothing synced below it waits in Recently deleted, till then held with what waits
  (engine `a_put_back_uploads_what_its_restore_finds_deleted_for_good`,
  `a_put_back_keeps_a_folder_gone_for_good_while_a_file_in_it_waits`); a folder already back
  has what is in it looked at. Every device then has them back. Cost: a feed read, then one restore of all the
  removals together: one read, and per wave (what comes back into folders already back) a
  request per 1,000 rows and one write; held file by file (plans in pages, the folders kept
  on the drive meanwhile), that is still one restore (300 loose files: 2 requests, against 301
  when each removal was restored on its own). A row whose name is taken is a request of its
  own, as in any restore. What a failure leaves is still held, and shown again, with a note.

  A confirmation lasts while its removal is pending, or waits for a file in it not hashed yet
  (§6.3: what was in such a folder, sent apart meanwhile, goes with it) or for another page's
  plan (§6.5); done or undone, it is
  forgotten. It is kept in memory only: after a restart, or when the folder's engine is set
  up again (new ignore rules, the folder set up again), what was confirmed and isn't done yet
  is judged again by the guard's rules, and asked about again where they hold it. A removal
  the user didn't see is never let through by a confirmation. A folder without its marker
  (moved, deleted, a disk not mounted) syncs nothing, so nothing is held, sent or put back
  there: an answer then is refused. Simulator:
  `guarded_clients_delete_only_what_was_confirmed` (with `Event::PutBack`), and
  `put_backs_that_go_wrong_lose_nothing` (a file deleted for good mid put back, a restore the
  drive stops after a wave: nothing put back is deleted or held again but what still waits;
  the simulator keeps the hold and puts back by the engine's own rules,
  `hbsync_engine::held`); engine:
  `a_delete_here_is_held_and_put_back_brings_it_back`,
  `every_file_removed_elsewhere_is_held_and_put_back_restores_it`,
  `a_confirmed_folder_delete_does_not_hold_other_deletes`.
- **Online-only files** are never deleted remotely just because they have no local bytes.

## 8. Operations

Local: `mkdir`, `download` (to temp, verify stored hash, atomic swap), `move`, `tags`, `delete`
(what goes takes its bases along, whatever the drive holds by then: a base left behind would read
as a local delete once the node is back in view, and undo a restore made while it ran),
`rename-for-conflict`, for filters (section 10) `detach` (a new node id in place) and
`forget` (a delete plus dropping the base, also where nothing of it is here), and for read-only
folders (section 13) `move-out` (out of the folder, the base kept). Remote: `create-folder`, `upload` (create by node id),
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
  just renamed here deletes it here too; a restore after a removal here brings it
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
  removal is, what goes to the local place the refused item doesn't leave, and the delete or
  forgetting of a folder it is in): it stays dirty, but isn't sent again, nor counted as
  unfinished work. Something else blocked behind it is tried and skipped, a local cost, told
  when it keeps coming back. Any change
  of the node, here or on the drive, is another key, and is sent. A refusal lasts an hour at
  most, and until Sync now; the operation done forgets it. A download whose bytes didn't match
  once reads the drive again first: it may have been written meanwhile.
- **Retry within a plan.** An operation blocked only by order (target parent not created yet,
  a folder temporarily inside its own destination) goes to the back of the queue.
- **Parking.** If a target name is held by a node the plan is about to move, delete or replace,
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
- **Delete guard.** `LocalDelete` carries the nodes the plan expects in the subtree: if the
  DB holds anything else there, it is not deleted, and is planned again; if the item itself
  changed, it is not deleted, and its place is rescanned. What is in it is checked once it is
  aside (§7).
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
  waiting; held deletes leave it till answered (§7). What a page leaves for another page, a
  folder that may go with its subtree or a clash, is planned next as a page of its own
  (§6.5), before the sweep ends. A sync ends after a full sweep in which nothing was done. A round clears only
  the marks it read with its page (a change of the drive's picture moves a mark's version on):
  a node another engine's pull or an announcement marks again meanwhile stays dirty, though it
  was dirty already, and the change it brought is planned at the next sweep. A name clash's
  loser the page didn't hold (planned for the winner's sake) is marked with the clearing when
  the run is sent to rename it: a run stopped before the rename (a pause, a crash) leaves it
  dirty for the next, though the round read the winner clean. The answer to a
  write of the folder's own run marks its node as the folder's own marks do, its version as
  it was: no round of the folder plans while its run writes (and a mark rewritten per write
  cost a page in the log each).
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
  (the root's, or a role folder's or Recovered files' on a file), that changes a node's kind,
  or of a newer schema (`v` above 1: read-only, node schema; so a write of this client's never
  loses its newer fields) is left out, reported, and recorded as there all the same (node
  schema, Identity and tombstones):
  - What is in a folder this device can't read stays in it, waiting: it is never an orphan.
  - A node whose newer version it can't read (unreadable) is kept as it had it: a local edit
    goes up as a new node (a conflict copy, under a free name); a rename, tags, a delete or a
    download of it is refused and not tried again while it is the same; the drive's version
    stands.
  - A later version that reads, or the file's delete, ends it.
- **Crash recovery** needs no operation log: every operation is idempotent or detectable (4105
  on a re-create, fingerprint guards locally), state is re-derived from a scan and the feed, and
  parked items come back from their records. What a delete left aside (a crash between its move
  aside and its commit, or a put back that failed) is decided at the start of the next sync,
  before the scan's records are read, by what it is, as the delete's own check decides (§7;
  recovery finishes verified deletes, 2026-10-22): still its node's synced version (each item as
  its row has it, each row as its base has it), it is unlinked with its rows and bases, whatever
  became of its folder meanwhile (the drive's delete stands); anything else goes back to its
  place under its own name, else under a numbered one, else at the folder's top (§7), and is
  planned again (an edit wins). An old version a download
  replaced, its node holding the new bytes by then, goes once its bytes read as the old
  version's; written to, it comes back as a new item beside the new one, never onto it (its
  node parked: beside the name it gets back, numbered, never the temporary one); with no name
  to come back under (every numbered one taken), it waits, told; unreadable now, it waits. One
  whose download's commit a crash took (its placement record still there) waits for that
  placement's recovery after the scan (§7 Old versions, write-ahead records): the placed file
  is its node's (the old row, kept while it is aside, is no other place of it), settled
  against the base the download acted on, and the old version decided there as the commit
  would have: unlinked if it still holds its bytes, else back beside the new one as a new
  item; the folder its old row was in is read again. Decided first, its row and base would go
  with it, and the placed file read as new. A record dropped (the new version gone from its
  place, or changed) or kept (its file not hashed yet, or the aside folder unreadable) leaves
  it for the next sync, decided then as any entry is. Where its rows went (an item took its place meanwhile), its base
  decides: a file holding the bytes its base records goes; anything else goes back where it was
  last synced, read as a new item there. One whose node this folder doesn't know (its state DB
  lost or replaced since), or one to go back whose place it doesn't, is left aside, and told
  (one note naming a few); junk the OS leaves there (`.DS_Store`) is passed over. A
  crash after an unlink and before its commit leaves the bases: the item reads as deleted here,
  which the drive's removal settles; a restore made in between is removed again, to Recently
  deleted (a known limit: a write-ahead record of a run's deletes, a durable commit more, would
  close it). A download (or a folder synced before, made again) placed but not recorded (a crash
  just before its commit) is found again: each batch of downloads is recorded before it runs,
  with the base each commit writes (the node as the drive has it at that version: for a folder,
  the version its plan saw, else the mirror's; one nowhere live, made again from Recently
  deleted, records the version there for recovery only, its commit keeping its base), its
  target, what the item holds of tags and what the item there held before (none: not known), and
  the base's version its plan saw (recovery checks it as the commit does; a record from before
  records said so is settled against the base as it is). A download claims the tags it writes on
  the file before moving it into place, the empty set too, as they read back (not known where
  the volume keeps none, learned when a tag write finds it unsupported); where the write turns
  out otherwise, the record says so durably before the move, so only a failure costs a commit. A
  folder is made without tags: it claims the empty set if it is to hold none, and otherwise
  claims what was written once written, durably before its commit. On the next sync the item at
  a record's target holding its bytes, new to the scan or the node's own file replaced, is that
  node, settled as its commit settles it (one function for both, against the same version), its
  tags as the record says the write changed them, a tag the user changed since being their
  change; where it claims none, the base's tags start afresh, each one the item holds and the
  drive holds too, so a tag only the drive has is written on the item and one only the item has
  goes up. Where the mirror no longer has the node as the plan reads it (deleted, in Recently
  deleted, or taken along by a folder there in this folder's part, §6.3), the commit and
  recovery alike settle the item at the record's version whatever the plan saw (its recorded
  spelling and the rest of the base as any settling keeps them), so the removal reaches it:
  never read as an edit, which would bring the node back; one whose item isn't the drive's to
  take (its name) waits as a file not hashed yet does, then goes. A folder's recovery, like its
  commit, marks what is in it on the drive dirty. Later changes on the drive are plain changes
  against it. A record goes once its base is written, by the commit or by recovery, or when the
  operation that wrote it, in that run, fails before its own item is in place, or its batch
  stops before it starts (so no item the user puts at that place is ever taken for it); one
  whose item is in place stays through a failed commit: where the state DB fails, the run stops
  and the next sync's recovery settles it; where reading the item back fails, the operation ends
  there and recovery settles it right away by that item alone, no other item read again (the one
  way an item put in place settles then; a folder new here, without a record, is taken over by
  its retry). One whose item can't be told yet (its file not hashed, say: locked; its folder
  unreadable) stays for ten minutes from when recovery first found it so, then goes. Recovery
  runs at the start of a sync and of each of its rounds, and first rescans the folders its
  records go into and those their nodes' items are in (each record's once a sync); both it and
  recovery in the run read the disk at a record's target alike: the item there with the record's
  bytes is settled, a record whose target verifiably holds nothing (not found, a file where a
  folder would be, a loop of links) goes, and one whose target holds another node is left to
  adoption. A record never goes only because no scan has seen its item yet. An item at its
  target whose bytes are neither the record's nor the base's (the user edited it before the next
  sync) can't be told from the file that was there before (the crash may have come during the
  transfer, before the move): the record goes, and the item reads against the old base, a
  conflict copy where the drive changed too; the tags the download wrote read as this device
  adding them (a known limit, §11). No operation writes over a record it didn't write: it is
  left for the next plan, the node dirty, until recovery settles the record. A verified download
  that couldn't be placed stays in the temp folder for the retry; temp files no pending download
  wants are removed before each plan. The stored journal position never passes a change that
  isn't recorded: it is taken before waiting for events and stored only once the event queue has
  drained.
- **Power cuts.** The state DB syncs its commits to the disk only now and then
  (`synchronous=NORMAL`): an OS crash or a power cut can lose the last ones, in order, while the
  folder and the drive keep what was done. So a sync starts and ends with the DB durable (what
  earlier syncs did is on the disk before parked items are put back), a run starts from a durable
  DB (the ids the scan gave new items go out with them), and so does an upload after a detach
  (its new id); write-ahead records (parking, conflict copies, downloads, a folder this device
  synced before made again) and a folder's registration are durable; before something is put
  where an item was moved or deleted from since the last durable commit, the commits are made
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
  each, so that a minute on all of it is gone at once): the user's restore from Recently
  deleted is run once more a minute on by the daemon, unasked, and says so ("Homebase says
  one item isn't there; asking again in a minute", never a network fault), then what was
  deleted for good meanwhile and can't come back; a restore that leaves rows in Recently
  deleted with no error (no free name, a folder that didn't come back) says which, once
  (keeper `a_restore_said_not_there_once_runs_once_more`,
  `a_restore_that_leaves_a_row_says_which`; engine
  `a_restore_said_not_there_once_hands_back_what_stayed`); a put back (§7) that fails is run once
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
   filtered local item is never changed or removed by sync, except junk deleted with its
   folder.
5. **Tags** (section 11): convergence includes the tags as each client sees them (clients tag
   like macOS, Linux and Windows); but for §11's known limits, a tag some user added and no
   user removed is on its node in the end, unless the node was deleted, or the item it was added to was deleted or got a new
   id on that device (then it went along).
6. **Gone for good** (section 8b): with the drive saying now and then that a live file isn't
   there, once of each, no client takes a file the drive holds as gone for good, nor deletes
   it here; the clients converge without loss.
7. **Deletes are checked:** no client unlinks an item that isn't the version its base
   records (junk aside), and nothing stays aside once the clients settle (§7).
8. **Read-only folders** (section 13): a read-only client never writes the drive, converges to
   it like any other, and loses nothing: what changed there is where it moved out to.
9. **Another identity's drive** (section 13): a member, as the drive's items are shared with
   it and withdrawn one by one, converges to what is shared with it (an item without its
   folder at the top, one of a name kept where names meet there), never writes, and plans
   the same on the engine's path over the state DB, also in pages and through crashes.

## 10. Filters

Some items are never synced. Filtering is per device and works in both directions: a filtered
item is invisible to this device's sync, locally and remotely. Ignoring never deletes anything
on its own.

**Classes** (`hbsync-local/src/filter.rs`):
- **Excluded**: the client's own `.hbsync` folder at the top of the sync root, in any case or
  spelling a volume that ignores case takes for it (`.HBSYNC`): another writer's folder so named
  would be this client's own there, and what is in it would land in it. And an item of that
  name **at any depth** (a file too): below the top it is another sync folder's state, moved
  or copied in with its folder (a folder of shares that lost its marker holds one per sharer).
  Its DB and half-done downloads make no sense on another device, and its marker there would
  be taken for a set-up sync folder's. It is the user's as ignored content is: kept on this
  device only, and it keeps its folder (rule 3). No rule of the user's brings it into view.
  One a version before this rule synced leaves the drive and the other devices by rule 1 and
  stays where it is; one another writer puts on the drive is masked (rule 2), listed nowhere
  on the computers (engine `a_state_folder_below_the_top_is_left_alone`, sim
  `a_state_folder_stays_on_its_device_at_any_depth`). A restore brings nothing back under
  that name, in any folder (one gone for good too): it takes a numbered one
  (`.hbsync (restored)`), where the computers see it (engine
  `a_restore_into_a_plain_folder_never_takes_the_state_folder_s_name`, sim
  `a_restore_below_the_top_numbers_around_the_state_folder_name`). And a **folder of
  shares** at the top: the folder that holds what other identities share with this one
  ("The folder of shares" below), found by the client's record of it and its marker, never
  by its name.
- **Junk**: a built-in list of names the OS or an application creates by itself, matched in any
  case at any depth: macOS `.DS_Store`, `._*`, `Icon\r`, `.Spotlight-V100`, `.Trashes`,
  `.fseventsd`, `.TemporaryItems`, `.DocumentRevisions-V100`, `.apdisk`; Windows `Thumbs.db`,
  `ehthumbs.db`, `desktop.ini`, `$RECYCLE.BIN`, `System Volume Information`; KDE `.directory`;
  Office `~$*` and
  `~*.tmp`; LibreOffice `.~lock.*#`; vim `.*.swp`, `.*.swx`; downloads in progress
  `*.crdownload`, `*.part`, `*.partial`, Safari's `*.download`; other sync apps' own files, left
  in a folder moved over from them: Dropbox `.dropbox`, `.dropbox.cache`, OneDrive
  `.849C9593-D756-4E56-8D6E-42412F2A707B`, Google Drive `.tmp.drivedownload`,
  `.tmp.driveupload`; and this client's `.hbsync-shared`, the marker of a folder of shares
  (never synced, so a folder of shares copied elsewhere marks nothing on another device).
  Junk is disposable.
- **Ignored**: the user's rules in `.hbsync/ignore` (gitignore syntax, per device, never
  synced), matched case-insensitively on case-insensitive volumes. Ignored items are the user's
  data, kept on this device only. A line that isn't a valid rule (in the glob syntax read here,
  stricter than Git's in places: an unclosed `{` is no rule) is skipped and matches nothing,
  as Git skips a bad pattern: the other rules apply and syncing goes on, and the folder's
  notes say which lines, once a version of the rules, not at every sync (the note goes once
  they are fixed). What such a line meant to keep here syncs; a working rule edited into a
  bad one is a rule removed, and its items come into view and sync as new items (rule 4). But
  a bad line that starts with `!` (a negation, or meant as one: indented too) isn't skipped:
  without it, what it kept synced would be ignored, and by rule 1 leave the drive and the
  other devices. It stops the folder's sync, failing, with one note naming every such line
  (".hbsync/ignore line 2 isn't a valid rule (…), and keeps things synced: syncing stops till
  it is fixed (fix it to resume)"); fixed, syncing resumes (within a minute: the watcher is
  stopped with the folder) and the note goes (runner
  `a_bad_negation_stops_the_folder_till_it_is_fixed`). An escaped `\!` is an ordinary
  pattern, skipped like any other; a negation valid in Git but not in the glob syntax read
  here (an unclosed `{`) stops the folder too. A rules file that can't be read (not a typo:
  its rules are unknown) stops the folder's sync till it can.

An item inside a filtered folder is filtered too.

**The folder of shares.** What other identities share with this one is synced into folders of
their own (§13, "Another identity's drive"), kept together in one folder at the top of the
account's sync folder. That folder isn't the account's: nothing in it is on the account's
drive, and this folder's sync leaves it alone, as its own `.hbsync`. It is never known by
its name, which the user may change, but by the client's record of it, `.hbsync/shares`: the
folder's file id (which a rename or a move within the volume keeps) and a token, which the
folder's marker, a file `.hbsync-shared` directly in it, holds too. The folder (not a link)
at the top with the id on record, holding a marker (a file) with the record's token, is the
folder of shares, read when the folder is opened and again as rule 4 says
(`shares::folders`, `Filter::load`). Neither says anything alone: the marker is a file
anyone can copy (on Windows it has the hidden attribute, as a leading dot hides it
elsewhere), and where ids are reused (ext4, XFS) a new folder may get the id of one deleted.
The rule is the filter's, so it holds at the top of every folder this client syncs, not the
account's alone.

Where the volume's file ids aren't stable (FAT, exFAT, a network or FUSE volume:
`has_stable_file_ids`), no id is recorded and the token alone tells: there a marker copied
into another folder at the top makes that one a folder of shares too (a known limit; it
leaves the drive and the other devices by rule 1, and stays here).

The client makes it so that it is never at the top without its marker, nor before it is on
record (`shares::make`): prepared in `.hbsync/tmp` with a marker holding a new token,
recorded, then renamed into place, never over anything (the marker is on disk before the
record names it). A crash in between leaves a prepared folder: one that is on record is put
into place as it is by the next try (never taken apart first: stopped there, the record
would name nothing), any other is made anew.

- Its name is excluded at the top, as the volume compares names (on macOS in any
  normalization, whatever the volume's case), and by rules 1 and 2: it is
  neither walked nor watched for changes, nothing in it is sent, and an item of its name at
  the top of the drive (a folder or a file, another device's) is masked on this device while
  the folder holds the name: not downloaded, not deleted from here, and nothing on the drive
  changes. It comes once the folder of shares has another name (sim
  `a_folder_of_the_same_name_on_the_drive_comes_once_the_folder_of_shares_yields`), which
  the daemon gives it: the folder of shares gives way to the drive's item, renamed to its
  next free number (`Shared-2`; `docs/spec/daemon.md`, runner
  `the_folder_of_shares_is_made_and_gives_way_to_the_drives_item_of_its_name`). Where the
  volume ignores case, names compare as everywhere in the client (`name_key`: `ß` as `ss`),
  which is stricter than the volume: an item of the user's own beside it that the volume
  tells apart and the client doesn't would be left out with it, and by rule 1 leave the
  drive. So the folder of shares gives way to such an item too, as the folder opens and
  before a filter that holds its name is read (the folder is opened again at once when the
  folder of shares is renamed, or such a folder is made or an item renamed to it; a file
  made under such a name is found at the next look, left out till then and never on the
  drive before): the order matters (sim
  `a_folder_of_shares_yields_before_a_plan_holds_a_name_a_synced_item_has`, runner
  `the_folder_of_shares_gives_way_to_a_folder_the_client_cant_tell_from_it`). The names
  compared are the drive's top as the DB has it and what is at the folder's top on disk: an
  item that is neither yet (made on another device and not read here) is found by the sync
  that reads it, which plans nothing with it masked: the folder of shares gives way first
  (runner
  `the_folder_of_shares_gives_way_before_a_synced_folder_that_took_its_name_is_planned`).
  Masked it is only where the folder of shares can't give way (its rename fails), and
  where nothing gives way at all (the engine alone). It doesn't give way to an item this
  device synced and is taking off the drive (deleted here; or the very folder named as the
  folder of shares on the user's word, `docs/spec/daemon.md`): that item is leaving by
  rules 1 and 2 (deleted on the drive though its name is filtered), and its name is free
  once it is gone (account
  `a_folder_named_as_the_folder_of_shares_leaves_the_drive_and_takes_the_shares`, runner
  `the_folder_of_shares_keeps_a_name_this_device_is_taking_off_the_drive`). That is told
  before the plan, by what a lookup can tell (the drive has the item as it was synced
  here, and it isn't here any more); where the plan then keeps the item (another device
  put something into it meanwhile: only its base goes) the sync says there is work left,
  and the next one has the folder of shares give way (runner
  `the_folder_of_shares_gives_way_to_an_item_that_stays_on_the_drive_after_all`).
- One the filter doesn't know yet (it came, or took another name, while a sync was running;
  the filter is read again before the next sync) is left alone as well, by the record and
  its marker on disk: the walk reads the record once when it meets a folder at the top,
  compares each folder's file id there with it (the listing has the id: nothing more is
  read), and reads the marker of the one that matches, before it reports or enters it;
  and a folder of the drive is never made out of the folder of shares (the drive's waits,
  and is masked once the filter is read). Nothing in it is sent, and the drive's folder of
  its name isn't put into it (engine
  `a_folder_of_shares_the_filter_doesnt_know_yet_is_neither_sent_nor_merged`).
  Nor is anything put into it where it took the place of a synced folder (that one deleted,
  the folder of shares renamed onto its name) and a sync runs before a scan has seen either
  (a watcher that tells late, as macOS's does): before an item is placed in a folder, or
  moved into it, the folder on disk is looked at (once a run for each folder when a
  transfer is prepared, and again as a download is placed, however long its transfer took,
  as a folder is made in it, and as an item is moved to it), and where it is no longer the one
  the DB has there (nothing there, no folder, another file id) the item isn't placed: the
  next plan decides, once a scan has seen what is there. No scan of that place alone is
  asked for: it would take a folder moved elsewhere for deleted, where the watcher's own
  scan sees both ends of the move. That holds for any folder, not only this one: what the
  drive has for a folder goes into that folder, never into another that has its path now;
  an item put back from aside goes to the folder's top instead (and stays aside for the next
  sync where its folder can't be looked at). Older paths of the executor still ask for a
  scan of an item's old place when it changed on disk, which takes a folder moved before a
  scan for deleted (filesync #421). Told by the file id where
  ids tell an item (not on FAT, exFAT, a network or FUSE volume: a folder there is taken
  for it). (Engine `a_folder_of_shares_at_a_synced_folders_place_gets_nothing_of_the_drive`,
  `a_folder_moved_before_a_scan_isnt_taken_for_deleted`; a stat for each folder something
  goes into, a run, and a row's read and a stat for each download placed in a folder.)
- Moved into another folder (a slip of the hand in the file manager; the same volume, so
  its file id stays) it is left alone there too: a walk knows it by the file id on record,
  which each listing has, and looks for its marker before it reports or enters it (one
  whose marker can't be looked for just then is neither reported nor entered that time, and
  what the DB has of it stays: a folder of the user's own that was the folder of shares
  once has such an id too; a rescan asked for in the folder of shares or below it starts in
  the folder it is in; and a marker made or removed below the top leads to such a rescan,
  where an id is on record, while the watcher runs: a marker removed there while nothing
  ran is found by the next whole walk, or when the folder it is in changes, as the
  journal of Windows names a folder by an id the DB has no row for). A folder of the drive is never made
  out of it there either: nothing gives way below the top, so that folder can't be held
  here until the folder of shares is moved back, said in a note. Nothing of it is
  sent; back at the top it is the folder of shares as before, and nothing of it was on the
  drive to take away (scan `the_folder_of_shares_moved_into_another_folder_never_reaches_the_db`,
  engine `the_folder_of_shares_moved_into_a_synced_folder_is_neither_sent_nor_merged`).
  Where the volume's file ids aren't stable no id is on record: there it is known at the
  top only, and moved into a folder it syncs as the user's own (a known limit). So it does
  where it gets another file id while it is below the top (the folder it is in moved to
  another volume and back, or restored from a backup): only at the top is one looked for
  by its marker (filesync #417).
- Dragged into another folder the account syncs (a role folder, this computer's Desktop)
  it is left alone there as well: such a folder's filter knows it by the main folder's
  record (`Filter::load_beside`), by the file ids on record wherever it is in that folder
  (where no id is on record it isn't known there: a marker alone tells nothing but at the
  main folder's top). It has no place there: nothing is made, renamed or taken, and a
  folder of the drive at its path can't be held until it is moved back (a note). Such a
  folder reads the record again when the main folder opens (its record may say something
  else then), and at its looks. While the main folder can't be asked (gone, not mounted,
  its record unreadable) no folder beside it stops: a filter in use keeps the ids it
  knows, and a walk that meets a folder with such an id leaves it as it is, neither
  entered nor taken away; a folder opened anew meanwhile knows of none (a known limit). The main folder meanwhile has none at its top: shares
  are off (`docs/spec/daemon.md`). Moved to a folder on another volume it is a copy with
  another file id, and isn't known there (the limit above, filesync #417) (scan
  `the_folder_of_shares_dragged_into_another_synced_folder_never_reaches_its_db`).
- Back as a copy (moved to another volume and back, restored from a backup: another file
  id, its marker in it) it is taken as the folder of shares again: as the sync folder
  opens, and while it is synced as soon as a folder at the top holds the record's marker
  (the folder is opened again for it, also when one such folder goes and another comes).
  The record keeps the ids it had before beside the new one (the last few): the folder a
  copy is taken for may still be in the sync folder, moved into another folder, and is the
  folder of shares where it is as well. Back at the top beside the one on record
  now, both are left out, and what is shared is synced into the one on record now. Taken is the only folder there that holds the marker
  and isn't synced: one the state DB has a base for (agreed with the drive), under its name
  as names compare or by its file id (a synced folder a marker was copied into stays the
  user's own, renamed or not). One only scanned so far (it came whole, a rename into
  place) isn't synced: taken, it leaves the DB with the next scan and never reaches the
  drive (runner `a_folder_of_shares_back_as_a_copy_is_taken_again_while_syncing`). A known
  limit: copied back piece by piece, the folder is sent as the user's own from the moment
  its first items are (a sync may begin before the marker is copied), and isn't taken
  afterwards; nor is one renamed into the top whole in the middle of a running sync that
  lists the top (a placement there, a rescan the sync asks for), which that sync may send
  before the folder is opened again. The log says that a folder holding the marker wasn't
  taken. The user names it as the folder of shares then
  (`hbsync shared-folder --take`, `docs/spec/daemon.md`): what was sent of it leaves the
  drive, and it stays on disk.
- A marker anywhere else does nothing (junk): copied into a synced folder at the top, that
  folder stays synced, with what is in it and what comes (scan
  `the_folder_of_shares_never_reaches_the_db_and_a_copied_marker_does_nothing`, engine
  `a_marker_copied_into_a_synced_folder_leaves_it_synced`). A copy of the folder of shares
  holds the marker too, and isn't the one on record: it syncs as the user's own, at the top
  or below it.
- The folder of shares that loses its marker is a plain folder from then on, synced with
  what is in it; where the drive has an item of its name at the top, the two meet as a name
  clash (§6), and one gets a conflict name. With its own marker back it is the folder of
  shares again, and leaves the drive and the other devices by rule 1 (sim
  `a_marker_lost_makes_the_folder_plain_and_its_own_back_takes_it_off_the_drive`), unless
  the client made another meanwhile: that one is on record then, with another token.
- A copy of the whole sync folder (a restore from a backup) holds new folders, which the
  record doesn't find. As the folder opens, where the folder on record isn't at the top and
  exactly one folder there that isn't synced holds a marker with the record's token, that
  one goes on record (`shares::adopt`, runner
  `a_restored_folder_of_shares_is_taken_as_the_folder_opens`). Not synced: the state DB has
  no item of its name at the top, and none with its file id anywhere (a synced folder
  renamed while nothing ran; an id the DB has for an item that is at its place with another
  id tells nothing: after a restore every id in the DB is from before). Never a synced folder, and with two to choose from neither:
  those sync as the user's own. With nothing on record nothing is taken, and nothing looked
  at.
- A folder at the top this user may not look into counts as holding no marker (the walk
  can't read it either). A top that can't be listed, a record that can't be read or isn't
  one, or a marker that can't be looked for (an I/O error: it may be there, and a folder of
  shares taken for a plain one would be sent), stops the folder's sync, failing, till it
  can, as a rules file that can't be read.

**Rules:**
1. **Local.** The walk neither reports nor enters filtered entries (nothing is hashed), and the
   watcher drops their events. A synced item that becomes filtered (a rule was added, or it was
   moved into an ignored folder) looks deleted: it leaves the drive and the other devices
   (deleted there), and stays here. The mass-delete guard applies (§7): held over its limit;
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
   one (a name clash); not a folder of shares, which isn't synced and keeps its name.
   A masked node that comes into view (a rule was removed, or another device moved it out of
   the ignored place) is downloaded with everything in it.
   A folder in Recently deleted whose place is filtered is masked; what is live in it is left
   as §6.3 says (orphans: "Nor is what is in a folder in Recently deleted …").
   A synced node that becomes masked because another device moved it into a path this device
   ignores leaves this device, the remote untouched:
   - an unchanged local copy is deleted (§7), and its base goes in the same step
     (`Forget`): a base left behind would read as a local delete once the node is back in view
     (another rule, another move);
   - a local copy with changes the remote doesn't have (edited, or never synced) is
     **detached** (`LocalDetach`): it becomes a new node where it is and is created remotely
     like any new item. The old id is still live remotely, so it can't be created again. The
     masked folders it sits in are detached with it, as are masked folders holding anything
     else this device syncs; their unchanged contents are deleted. What the plan moves out
     of a folder the filter masks, to where the drive has it, doesn't keep that folder nor
     the masked ones above it: a folder held for nothing else is deleted like an unchanged
     copy, never made again on the drive, empty. One whose move waits, or that leaves a
     conflict copy, keeps its folder detached; so does what lies outside the scope (§12).
3. **Folders.** A remote folder with masked live children (that aren't leaving) counts as
   having a live child: a local delete of it deletes what this device saw, and the folder comes
   back here, empty.
   Locally, junk files in a folder being deleted go with it; ignored content, a state folder
   (`.hbsync`), and a folder on the junk list, keep their folder: the synced contents are deleted, item by item, and the
   folder is released (the next scan sees it
   as a new folder); junk directly in it stays with it.
4. **Rule changes.** The filter's fingerprint (rules, the built-in names' version, case sensitivity,
   the names of the folders of shares) is kept in the state DB. When it changed, start-up walks the whole tree (a journal doesn't
   report items that just became visible) and re-plans every synced node; the new fingerprint
   is stored in the same transaction. Items that come into view are new items. The sync loop
   checks the ignore file for an edit when the watcher sees it change (once a minute without
   a watcher) and on a sync request, and starts over with the new rules when it changed.
   The folder of shares is looked for with it (where one is on record: one listing of the
   top and one marker's read; nothing but the record's read where none is), and alone, at
   once, before the next sync, when the watcher
   sees an entry of the top made, removed or renamed (not a file, where the OS says it is
   one), or a marker in a folder there (nothing half written to wait for: a sync in between
   would send what is in a folder that just became one). Without a watcher a change to it
   is seen within the minute; the walk leaves it alone meanwhile, by the record.

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
none, and keeps the local tags. What the device can't hold it writes as that version has it,
Linux's colours too: its plan took those from the drive it saw, which may be older (a restore
planned while the node was deleted, sent after another device brought it back with a tag and
deleted it again).

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
isn't put in place: it is tried again, the plan sending the tag up meanwhile. Nor is one that
would forget a tag removed here (the drive holds it, the file it replaces doesn't, and the
download wasn't to write it): its base's tags starting afresh, the drive's tag would read as
added there and be put back on the file. Either waits for a tag write that lands, whether or not
the drive refused the change for good (access, read-only): the file keeps the change meanwhile,
and the fetched bytes stay for the retry. A node with no
base gets one when place and content agree, with the tags both sides have: the merge then
brings each side the other's (a copy proven equal only across hash algorithms can't be proven
again later). A tag write needs a base to record what it wrote: the item's own, or one it
founds (place and content agree with the drive's version it acted on). Without one it waits,
the node left for the next plan: the drive's tags it wrote would read as this device adding
them, and a tag removed on the drive meanwhile came back. The plan's move or remote write
founds the base; where it runs first, the tags come down in the same run. A folder made here
from Recently deleted (never synced here, its restore to follow) founds its base on the version
there, as found before the write (a pull landing before its commit doesn't change what it settles
against); where that no longer holds each tag it is to write that this device can hold, as it
holds it (on macOS in its colour; Linux, which holds none, settles against that version's colour
and its restore keeps it), as restored, changed or purged since the plan, it waits for the next
plan.

**Limits.** Tag changes are seen through the change time: on a volume that keeps attributes
elsewhere (exFAT's `._` files) or with one-second times (HFS+), a change can go unnoticed until
the item changes again. A tag change makes the scan read the file again (the change time is
part of the fingerprint, to catch writes that keep the modification time). An item that becomes
a new node on a device that can't hold all its tags (detached on Windows, §10) goes up with
the tags that device has. A download put in place whose commit a crash lost, its file edited
before the next sync, is read against the old base (§8a, crash recovery): a tag the download
wrote reads as this device adding it, so a tag another device removed meanwhile can come back.
That loses a removal, not a tag.

**Known limits** (the user's call, 2026-10-21: not now). The simulator names each one
(`hbsync-sim` `tagbook::Limit`) and excuses only that case. Three lose an add:
- *Own removal in flight*: a tag added back while this device's removal of it is still on its
  way. The removal's write settles the base on what both sides hold after it, so the add then
  reads as the drive's removal, and goes.
- *A new item keeps the base's tags*: an item put in place by a download where the node's base
  outlived its own item keeps in its base the old item's tags the write didn't touch. A tag
  neither side holds any more then reads as this device's removal, and is sent over a later
  add. (A conflict copy no longer leads here: its wanted base settles from the drive's tags,
  §5.)
- *Added back to the base*: a tag removed and added back here matches the base, so nothing goes
  up; where the drive dropped it meanwhile (another device's delete, the node made again
  without it), the drive's state comes down over the add.

Two lose a removal (the tag comes back):
- *Own tag write in flight*: a tag removed while this device's plan still writes it (onto the
  item, or up): the write, planned before, lands over the removal. A colour given then is lost
  the same way: both sides changed it since the base, and the merge takes the drive's. So is
  one given after the write landed and before the next plan settles it: the base still has
  the colour from before the write.
- *Answer superseded*: a write carrying a tag landed, but another device changed the node
  before its answer was recorded (§8a, Answers): the base never records the tag as on the
  drive, so it goes up again, over another device's later removal.

A write up is on its way until its answer is recorded. The engine reads the folder between
plans only, so its own write up lands over nothing it has seen; what leaves a node in this
state is a write that landed with its answer lost (a timeout after the server wrote): the next
plan reads it as another device's change, and a tag put back before that plan goes (seen in
the engine), a colour given before it likewise. The simulator reads the item at each step, and
reaches the same state without a lost answer.

Apart from these and the crash case above, no tag add or removal is lost, nor a colour given
to a tag the device's base holds, unless another device gave the tag a colour it hadn't seen
(both changed it: the drive's wins). A colour given before the base holds the tag (a new item
whose base isn't founded yet) is not promised: with no base to tell who changed it, the merge
takes the drive's.

## 12. Scopes
A device syncs the main folder (`~/Homebase`) and, if the user chooses, **role folders** where
they are: Documents, Pictures, Music, Videos, Downloads (node schema, "Role folders"), and its
Desktop (below). Each
local folder has an engine of its own, scoped to a part of the drive; they share the account's
state DB (one mirror, one feed) and keep their own base and local rows:
- the main folder's scope is the drive's top **without the role folders**, on every device,
  whether it syncs them or not: a role folder appears only where it is synced in place, never
  inside `~/Homebase`; nor this device's own Desktop's folder (below);
- a role folder's scope is what lies in that folder. Its engine sees the folder as its root
  and the rest of the drive as one folder at its top that it leaves out; what it writes at its
  root goes into the role folder. A node whose place isn't known (its folder gone from the
  drive, a cycle) isn't a role folder's: the main folder repairs it (§6; into Recovered files,
  §6.3); one that sits in a
  role folder in Recently deleted (another device put it there as it was deleted) is that role
  folder's.

Before every sync of a role folder, after reading the drive, its top must be on the drive: one
in Recently deleted comes back with what was deleted with it (one that can't come back where it
was, its folders there in a cycle each waiting for the other, comes back at the drive's top
first, then what was deleted with it), one not there at all (never made,
or deleted for good) is made at the drive's top with the role's id. So the first device to add
a role makes its folder, the others join it, and a role folder deleted on the drive while a
device syncs it comes back at that device's next sync, nothing deleted there and nothing
uploaded into Recently deleted. To delete a role folder for good, stop syncing it everywhere
first.

What lies outside an engine's scope is masked as filtered nodes are (§10): never downloaded,
never changed or deleted remotely. It is never *leaving* (§10, rule 1): another engine or device
syncs it. What a delete took along is judged on the drive as the filter leaves it (§10), what
the scope leaves out included: a node outside the folder's scope (its computer's own Desktop
in Desktops) written after the delete keeps the folders above it there too. A role folder at the top that the engine holds nothing of isn't planned at all; no
version ever synced one into the main folder, so a device holds one only through a bug, and
then lets go of it as of anything masked. A role folder moved into a folder (against the node
schema's rules) is masked there and keeps that folder alive (§6.3).

**Desktops.** A computer's Desktop is synced where it is too, but into a folder of its own,
never mixed with another computer's: each computer that adds its Desktop has a folder in
**Desktops** (a folder at the drive's top with a fixed id, node schema "Desktops"), with an id
of its own, made when the computer first adds it (or one of those there, if the user says the
computer is that one again). Its engine's scope is what lies in that folder, as a role
folder's. Each folder of a device is kept with its kind (main, role, Desktop); one of a kind a
newer version set up isn't synced. The main folder's scope leaves this computer's own Desktop
folder out too, wherever it is on the drive, and nothing else of Desktops: every main folder
shows the *other* computers' Desktops in `~/Homebase/Desktops/<computer>`, synced both ways,
and a computer's own one is masked there, holding its name (another item there takes another)
and keeping Desktops alive on it (also where the drive lacks it). The main folder reads the
device's folders at every round of its syncs (one small read): a Desktop added leaves it from
its next round (one not running takes it first thing when it starts), and it lets go of what it
held of it (masked, as above: a change made there meanwhile is kept, detached, in a folder of
its own beside it); one removed comes back into it. The Desktop syncs at once. Before every
sync of a Desktop, after reading the drive, Desktops comes back
from Recently deleted (with what was deleted with it; at the top where its folders there are in
a cycle, as a role folder's top) or is made at the top whenever it isn't
live, even with the computer's folder live in it (a delete stopped half way); then the
computer's folder must be live: it comes back, or is made in Desktops under the computer's
name (the first free one: `PC (conflict 1)`); nothing syncs into one that isn't. Made beside a
plain folder of its name, Desktops takes the first free one, and it loses a name clash at the
top whatever the ids, as Recovered files (§6, rule 1). The computer's name is the one the app
gives (made one the drive takes: NFC, not empty, `.`, `..` or `.hbsync…`, no `/` or NUL, 255
bytes at most; else `Computer`); with none given yet, the folder is made under the local
folder's own name, and a folder there already keeps its name (a computer joining a folder in
Desktops takes the name it has there, as given). The folder takes a name when one is given (a
rename of its node where it is, under the first free name), again at a later sync if the
rename didn't land; a rename made elsewhere stands until a name is given again, the same name
as before too (the user asked for it). The purge
keeps a folder in Desktops (a computer's Desktop) that Desktops' delete took along, as it keeps
a role folder: its computer brings Desktops back with it; and a restore into it never takes
`.hbsync`, as into any folder (§10). No device knows the other
computers' Desktops by id, so the purge knows them by place: one moved out of Desktops isn't
kept so.

A role folder holds no name for the scope: a plain folder of the same name at the top keeps it
(two at the top, told apart by their ids). A new scope plans the folders it, or the last one,
left out again, from their tops; it needs no walk, as only the remote view changed. Recently
deleted is the account's, like the mirror: one list, each item naming the folder of this
device it comes back to (none for a role folder this device doesn't sync); restoring, emptying
and the purge (by age) are the account's too. A change of the drive
is planned in the folders it concerns: those holding the node, and the one whose part of the
drive its place is in (every folder when that place isn't known). A folder in Recently deleted
has its place where its row there says it was (§6.3): what a folder's delete takes along is
planned in that folder's part only, not in every folder of the account. A folder deleted for
good has none: what is still live in it is planned in every folder.

A detached item's new id is fixed for the node and the computer (the state DB's own id, as
for conflict copies, section 5), so a replan picks the same one,
but never one in use here: the same node detached again while its first copy lives gets the
next.

## 13. Read-only folders

A read-only folder syncs one way: the drive decides, and nothing is ever written to it (what
another identity shares with this one; the engine's `FolderCtx::read_only`, a folder outside
it given to move things out to, on the same volume). Every node's target is the drive's state,
whatever changed here, and the plan holds no operation on the drive:

- **Changed here, kept by moving it out.** A file whose bytes changed here (to other than the
  drive's), and an item made here that doesn't hold the drive's bytes (a new folder as one, with what is in it),
  move out of the folder first, in one rename, under their own name in the folder they go to,
  else the first free of `name (from <folder>)`, `name (from <folder> 2)` … (a hundred). Their
  rows go, their bases stay: the next plan finds the drive's version missing here and brings
  it back. A synced item moved into a folder made here moves back first, a download of it to
  another folder too: the move-out runs after them, before the deletes here (a folder the drive
  deleted, holding a new file here, keeps it out of its delete), and expects what it takes as a
  delete does (§7); a synced item there it doesn't expect makes it wait for the next plan. A
  crash after the rename and before the commit leaves rows of an item that isn't there: the
  next scan reads it as deleted here, which comes back the same way. A delete here of what is
  in a folder that moves out isn't planned: it goes out with it, its base kept, planned again.
  What is new here in a folder made here that waits (for a file in it still being written,
  §6.3, or for another page, §6.5) waits with it, and moves out with it as one. A known limit,
  where the plan is split in pages (a dirty set of over 20,000): a synced item moved into a
  folder made here that another page plans goes out with the folder and comes down again (a
  download, not a move back). Nothing is lost.
  Another known limit: a cycle of folders on the drive (two writers' moves at once) shows
  here at the folder's top, as a two-way folder places it, till a two-way folder's repair of
  it on the drive comes down.
  The cost: each save of a file there is a move out and a download of the drive's version
  (a file kept open with autosave, one per save), and a hundred moved out under one name
  leave the next where it is, told, till one goes. Files there set read-only on disk (the
  app's, later) keep most saves from happening at all.
- **A name-clash loser that changed here** still moves out (only its syncing waits).
- **Undone.** A move or rename here goes back to the drive's place; a delete here comes back
  (downloaded again); tags here become the drive's. Nothing of these moves out: the drive holds
  what they changed. An executable bit changed here stays as it is while the drive's is the one
  synced (the bytes are the drive's; undone, it came back sync after sync on a volume that
  doesn't keep it); one the drive changes comes down. A file's bytes count as changed here
  against the ones it synced with (its local hash then), not against the drive's: an edit
  there of a file whose bit only changed here comes down, nothing moved out.
- **The same item on both sides** (adopted, the drive's bytes) stays.
- **Filters** (section 10). An item ignored here since it synced stays on the drive (it never
  leaves it): masked, as one never synced here, its item here kept here only. One moved out of
  an ignored place here isn't brought back: the drive's place wins, and this device filters
  it, so it stays masked, and what is here of it is let go of as of any masked node (detached
  and so moved out, where it changed here).
- **Repairs** (section 6) that would write the drive don't happen. An orphan isn't moved into
  Recovered files, nor its folder made again: it has no place here, so what is here of it is
  let go of with what is below it (deleted as synced, moved out where it changed here or never
  synced), once nothing of that waits or is planned in another page; it comes back once the
  drive gives it a place. (On another identity's drive only a node inside a file that is
  shared, or in a cycle, is one: an item whose folder isn't shared has a place, the top,
  below.) A name clash among the drive's items (case, normalization, or a name
  this device's ignored items hold) can't be renamed away: the item synced here keeps the name
  (else the folder that the item on that name here is in on the drive now, see "Names that
  meet at the top"; else the lowest id), and the others aren't synced here, each told so (by its place on the
  drive), as long as the clash lasts; each stays dirty, planned again at every sync (a page's
  work a clash, rare: what another writer made). What a folder's removal there took along is
  left there as it is, and a node no longer synced here with nothing of it here has its base
  forgotten.
- The mass-delete guard (§7) judges the deletes here as everywhere (a removal elsewhere of
  every synced file is held); what it holds is confirmed or stays held: putting it back would
  restore it on the drive, and is refused.
- The folder things move out to is a full path outside the read-only one (else its scan would find them
  as new items there) and made where it is missing, on the folder's volume (a move out is one
  rename; a rename across devices, as into a bind mount of the same file system, tells it too):
  one that isn't is refused for the run, told, each move out staying where it is. An item parked
  under a temporary name for what took its name (§8) moves out under its original name; one
  that doesn't go (refused, changed since, holding what the plan didn't expect) is given a
  name of its own again (its original if free, else a numbered one), never left hidden; what is
  parked below one that moves out gets its name back first.
- The executor refuses any write to the drive from a read-only folder, whatever planned it;
  so does the read-only remote (what another identity shares: `PeerRemote`).

### Another identity's drive

What another identity shares with this one is its drive, read as a member (`Remote::shared`,
kept in the state DB: `ReadTx::shared`). Its feed lists only the files shared with this
identity, each on its own: a file can be shared without the folder it is in. A folder synced
from such a drive is read-only (above; one set to sync both ways isn't opened), and these
rules hold too:

- **An item whose folder isn't shared is at the top.** A node whose folder the picture
  doesn't have (not shared with this identity, or no more) is read as in the top folder. Its
  row keeps the drive's folder: once that folder is shared, the item is in it, each item of
  it planned again (a move here, nothing downloaded), and it is back at the top once the
  folder's share is withdrawn while its own lasts. A folder in the sharer's Recently deleted
  that the picture has still holds what is in it (taken along, §7). A folder shared in a
  version this client can't read isn't in the picture either: what it holds is at the top
  till a version it reads comes.
- **Names that meet at the top** (two items of one name, each there for want of its folder)
  follow the name-clash rule above: the one here under the name keeps it (the one on disk
  under it before one only recorded there; a parked item, §8, isn't on its name on disk, though
  the local view reports it there), and the other isn't synced here, told so, planned
  again at every sync until its folder is shared, its name changes or the first one's share
  ends. One left out that was here already under an older name keeps that copy as it is
  meanwhile, and the older name stays taken here: a new item of that name waits for it in
  turn. A loser inside a folder whose share is withdrawn leaves with the folder.
  Where none of them is here under the name and another item is (one synced here that the
  drive has in a folder now), and that folder is one of them, the folder takes the name: it
  is made, the item here moves into it, and the others wait. By the ids alone the name
  could go to an item whose download waits for the name the item here holds, which waits
  for its folder, which waited for the name: none of the three ever came (filesync #419;
  plan `read_only_the_folder_the_holder_of_a_name_moves_into_takes_it`, and the simulator's
  saved case of `a_member_in_pages_that_crash_has_what_is_shared_with_it`).
- **A share withdrawn isn't a delete.** The drive tells it as a tombstone without a version
  (all zeros). The file leaves the picture and this device (deleted here as synced, moved out
  where it changed here), and nothing records it as gone: the share given again brings it
  back, at the version it has then. An announced withdrawal of a file the picture holds (or
  this device knows in a version it can't read) is
  left to the feed (one read), as the share may have been given again since. What was noted
  of the file goes with it (a version this client couldn't read, a doubt whether it was
  there). The sharer's own delete keeps its version, and is final as on any drive.
- **Nothing there is deleted for good from here.** The sharer's Recently deleted and their
  tombstones are their own devices' to purge (§8b; node schema, "The purge"): this device's daily purge only forgets
  the files it knew gone, each a tombstone's keeping time after its delete, and keeps no
  tombstone to delete later. Restoring from it and emptying it are the sharer's too: asked of
  such a drive here, both are refused before anything is asked of it (`Drive::restore`,
  `Drive::empty`).
- **A DB is one kind of drive for good.** Whose drive it is of is recorded before the DB first
  reads its drive, and a DB that read an identity's own drive is refused for a share (and the
  other way): read by the other's rules, a withdrawal would be a delete, and an orphan an
  item at the top.
- **Known limits.** A hard delete on the sharer's drive is told to no member (API notes,
  "Shared with you"): only a whole re-read of the feed finds it, which a device does after
  about 29 days away, so a device that stays connected keeps such a file. Every item of the
  sharer's Recently deleted ends that way (their purge deletes it for good after 30 days): its
  row stays in this device's Recently deleted until that re-read, and a file here whose
  folder is such a row stays hidden in it. A file shared
  without its folder stays here while that folder is in the sharer's Recently deleted (the
  picture never had the folder to take it along). A copy parked for a download that took its
  name (§8), its own delete planned, whose file is shared again before the delete ran (the
  share given back while the download ran, or a crash between) waits under its parked name,
  hidden, while the name is taken: the file on the name keeps it and follows the drive.
- **The cost.** Where an item is, is worked out as its row is read, and nothing is stored,
  so nothing goes stale when a folder comes or goes. Measured with 100,000 mirror rows (a
  release build, the DB on disk), against this identity's own drive, which pays one read of
  the drive's kind per transaction that reads the mirror, and nothing per row:
  - a mirror row read: one indexed probe more (3.0 µs for 1.9 µs);
  - which folders the picture lacks is worked out once per transaction that asks about the
    top, and again after a write to the picture in it: a step along the index for each
    folder holding something shared, 1.3 ms with 500 such folders, 6.5 ms with 5,000, 41 ms
    with 50,000, whatever the number of files;
  - a name looked up at the top after that: 2 to 5 µs (1.5 µs on an own drive) where each
    of those folders is shared, and one probe more for each folder that isn't (1 µs
    each: 2.4 ms a name with 2,500 of them). A share given by folder has none, or the one
    its top is in; someone sharing loose files out of thousands of folders pays this, and a
    stored mark with its upkeep on every write would be the cure;
  - a listing of the top: the rows of those folders are read with it (15 ms for 5,000
    folders at the top against 10 ms on an own drive; 132 ms where all 100,000 rows are at
    the top for want of their folders);
  - a folder arriving marks what the mirror holds in it dirty (one indexed read).
