# Sync-node schema v1

How one sync node is stored as one Homebase drive file. Every client that reads or writes a
"Files" drive (this client, odin-js, chat-kmp) follows this schema. Fixtures live in
[fixtures/](fixtures/).

This is the living schema: every change to what a node carries lands here, in the same PR, with
a line under [Changes](#changes). Other clients follow the copy on `main`:
https://github.com/homebase-id/filesync/blob/main/docs/spec/node-schema-v1.md

Markers: **[verified]** = checked in `../odin-core` code; **[provisional]** = our choice, not yet
reserved in odin-core.

## Drive
- One Homebase drive per sync root, of the "Files" drive type.
- The Filesync app's drive: slug `filesync`, type slug `filesync`, drive type GUID
  `5da48122-e946-487a-8b9d-589642f3c91d` (`crates/hbsync-homebase/src/app.rs`). The GUIDs are
  **[provisional]** until odin-core reserves a drive kind (foundations §5 item 5).
- Every client, other apps' too, finds it by slug (`GET /api/v2/apps/filesync/drives/filesync`):
  the route names the app that owns the drive, not the caller, and returns it to any app or owner
  allowed to see it **[verified]** `V2AppDriveController.GetAppDrive`,
  `DriveManager.GetDriveBySlugAsync`. The alias `cfceb937-b868-496b-91cb-3402f18320f8`, fixed in
  the Filesync app's drive request, also works; if it ever changes, a line under
  [Changes](#changes) says so.
- The drive should have `requirePayloadHashes = true` (owner setting; apps can't set it
  **[verified]** `OwnerDriveManagementController.cs`).

## Root
- Root node id: `2a1c0d55-8ad3-417e-a5d7-42c3d0baba0e`, the same on every drive.
- The root is never stored as a file. A node with `groupId == root id` is top level.

## Role folders
Folders at the top a device can sync where they are, from the folder the OS has for the role
(Windows Known Folders, the macOS home folders, Linux's XDG user dirs). Each has a fixed node id,
the same on every drive: devices that add one join it.

| Role | `uniqueId` | Name when created | Suggested icon (Fluent UI System Icons, MIT) |
|---|---|---|---|
| Documents | `0274575b-c2cf-4ac7-b39b-d56b2c121b75` | `Documents` | Folder Document |
| Pictures | `c2c9af9b-4873-48d8-a8ff-4779802f038f` | `Pictures` | Image |
| Music | `0f4cd8f9-a914-476d-86d0-b1d1a5215a85` | `Music` | Music Note 2 |
| Videos | `b133291b-9309-46c5-9bfd-ac6c81669049` | `Videos` | Video Clip |
| Downloads | `5290e709-bc6d-4e12-827e-96d6dd006403` | `Downloads` | Arrow Download |

- Recognise a role folder by its `uniqueId`, never by its name: a plain folder may be called
  `Documents`, and a role folder may be renamed.
- They are ordinary folders otherwise (content `v`, `name`). On a computer, a role folder is
  synced only where the user chose it, and never inside the main folder (sync semantics §12).
- A plain folder may share a role folder's name at the top: both are shown.
- A role folder in Recently deleted still holds its `uniqueId` (Homebase refuses a second live
  one, inferred): restore it rather than create it.

## Recovered files
A folder at the top holding what lost its folder for good: a live node whose `groupId` is no
node on the drive (deleted for good while something was written into it, or by another app; or
a restore of something whose folder went for good).
Computers syncing the main folder move such a node into it (sync semantics §6.3), where they
sync its place: one at a place every computer ignores stays as it is.

| `uniqueId` | Name when created |
|---|---|
| `fa4646cb-2df0-4f18-a3c6-1fc2b2f6ac4b` | `Recovered files` |

- The id is the same on every drive, so devices adopting at once share one folder. Recognise
  it by its `uniqueId`, never by its name: the user may rename it.
- It is made when an orphan first needs one (one in Recently deleted is restored instead,
  alone), and is an ordinary folder otherwise (content `v`, `name`): open, rename, move out of,
  delete. Nothing removes it when it is empty; a deleted one comes back only for a new orphan.
- The adopting device moves an orphan in with one metadata update of `groupId` at its
  `versionTag` (a removed folder a live node brings back: restored and moved in one update). A
  name taken in it: the orphan gets `<stem> (conflict <n>)`.
- An orphan only removed (in Recently deleted) isn't moved: it stays an item of its own there.
- In a name clash at the top it is the one renamed (`Recovered files (conflict 1)`): a plain
  folder called `Recovered files` keeps its name, whatever the ids.

## File header fields

| Field | Folder | File |
|---|---|---|
| `appData.fileType` | `7101` **[provisional]** | `7102` **[provisional]** |
| `appData.dataType` | `0` | `0` |
| `appData.uniqueId` | node id | node id |
| `appData.groupId` | parent node id | parent node id |
| `appData.content` | JSON, see below | JSON, see below |
| `appData.archivalStatus` | `0` live, `2` removed | `0` live, `2` removed |
| `appData.tags` | none (Finder tags are `content.tags`, encrypted) | none |
| `isEncrypted` | `true` (`encryption.md`) | `true` (`encryption.md`) |
| payloads | none | exactly one, key `sync_data` |

The file types must stay outside the server's reserved range `2100000000..int.MaxValue`, which
app uploads may not use **[verified]** `ReservedFileTypes.cs`, `StandardFileStreamWriter.cs:35`.

These are all the header fields sync uses. A write replaces the whole header (**[verified]**
`StandardFileUpdateWriter.cs:48-85`, homebase-api-notes), and every write of this client sends
the fields above with an owner-only `accessControlList`: what another client sets elsewhere in
the header (an ACL that shares the node, `appData.tags`, `userDate`, a preview, an
`archivalStatus` other than `0` and `2`) is reset by the next rename, move, tag change, upload or
restore of the node on a computer. Other clients must not rely on them. (Keeping them would cost
every node its raw header in each computer's store, for fields no client uses; a later version
of this schema may define some of them.)

## `appData.content`

```json
{ "v": 1, "name": "Report Q3.pdf", "mtime": 1759920000123, "size": 52311, "x": true, "ct": 1451606400123,
  "tags": [{ "n": "Red", "c": 6 }, { "n": "Work" }] }
```

| Key | Type | Required | Meaning |
|---|---|---|---|
| `v` | int | yes | schema version, `1` |
| `name` | string | yes | original name, Unicode NFC. Never contains `/` or NUL, never `.` or `..`, not empty |
| `mtime` | int | files | local modification time, Unix ms. Readers round a fractional value |
| `size` | int | files | plaintext size in bytes |
| `x` | bool | no | executable bit; omitted when false. Readers take `1` and `0` as `true` and `false` |
| `rm` | object | no longer written | from before 2026-10-15: `{"at": <Unix ms>, "root": "<node id>"}` on a removed node. Readers ignore it (the header says what is removed and when, see Recently deleted); the next write of the content drops it (a removal or restore through the archival-status batch keeps the content, and so it) |
| `ct` | int | no | creation time, Unix ms (files and folders): the item's own when the node is created, then never changed. Shown and applied (Windows, macOS; Linux can't set one), never a reason to sync, like `mtime`. Readers round a fractional value and take anything else as none (and keep it) |
| `tags` | array | no | Finder tags (files and folders): `[{"n": <name>, "c": <colour>}, …]`. `n` is the tag's name (NFC, not empty, one entry per name); `c` is Finder's colour, `1` grey, `2` green, `3` purple, `4` blue, `5` yellow, `6` red, `7` orange, left out when none. Order doesn't matter (this client sorts by name). No key means no tags. Synced: adding or removing a tag, or changing its colour, reaches every device (sync-semantics §11). Readers skip an entry without a string `n` and read any other `c` as none |

- "Required" is for writers. A reader skips a file without `size` (an encrypted payload's stored
  size is padded, `encryption.md`) but keeps one without `mtime`, as a file with no date (this
  client reads it as 0, writes it back without one, and gives the file its own date when it
  downloads it). A folder's `mtime`, `size` or `x` from another writer is kept as an unknown key
  is (a `false` or `null` one as none, which reads the same).
- Readers ignore unknown keys. Writers keep unknown keys they read (forward compatibility).
- `ct` is kept the same way through every rewrite (new bytes, a rename, a move): editors that
  save by writing a new file and renaming it over the old one would otherwise reset it. A
  download gives it to the file once in place (after the mtime). A node without one (from
  before `ct`, or a client that sends none) gets its file's on its next update or rename.
- `tags` is kept the same way through every write that isn't about tags, as it was written (its
  entries' other keys, an entry a reader skips) until the set of tags changes (or past about
  2,000 bytes, then written as this client writes them). Where they live on
  each system: macOS `com.apple.metadata:_kMDItemUserTags` (what Finder and Spotlight use),
  Linux `user.xdg.tags` (KDE; names only, so a Linux device keeps the colours as they are on
  the drive), Windows nowhere (a Windows device keeps them all as they are). Readers take names
  in NFC. This client syncs at most about 2,000 bytes of tags per item; an item with more
  keeps what the drive has.
- A reader that finds `v > 1` treats the node as read-only. A computer of this version reads
  it as a row it can't read (below, and sync semantics §8a): kept as it is, never downloaded,
  and what is in it waits. So whoever designs v2: bump `v` on files only, never on folders,
  until every client reads v2 (a v2 folder hides everything in it from v1 clients), and never
  on the fixed folders (the root, the role folders, Recovered files).
- Content is capped by the server at 10,240 characters **[verified]**; with names capped at 255
  UTF-8 bytes by this client, v1 content stays far below it.

## Payload `sync_data`

| Field | Value |
|---|---|
| `key` | `sync_data` (matches `^[a-z0-9_]{8,10}$` **[verified]**) |
| `contentType` | `application/octet-stream` |
| `iv` | the payload IV (`encryption.md`) |
| `hash` | `{ "algorithm": "blake3", "storedHash": b64, "contentHash": b64 }` |

- This client writes BLAKE3. It accepts `sha256` nodes written by other clients and computes
  SHA-256 for the local file when it needs to compare.
- `storedHash` is over the ciphertext; `contentHash` is the plaintext hash, encrypted
  (`encryption.md`). (On an unencrypted payload, which this client doesn't write, the server
  requires them equal **[verified]**, rule 5 of `../odin-core/docs/payload-hashes.md`.)
- A file node with no `hash`, or no `sync_data` payload, isn't a node: the client skips the row
  as unreadable and reports it. So is one whose payload's `bytesWritten` isn't the stored
  (encrypted) size of its `size`, `(size / 16 + 1) · 16` (`encryption.md`).

## Identity and tombstones
- `uniqueId` is the node id. Creates are idempotent: a retry that gets `ExistingFileWithUniqueId`
  (4105) means the create already landed **[verified]** `FileSystemStreamWriterBase.cs:269-283`.
- Tombstones come only from a delete for good that leaves a trace (Recently deleted, below).
- A tombstone keeps `fileId`, `groupId`, `fileType` and nulls `uniqueId`, `content` and payloads
  **[verified]** `DriveStorageServiceBase.cs:1923-1965`. Clients therefore map
  `fileId → node id` themselves.
- Tombstoned UniqueIds are free again (the duplicate check skips tombstones **[verified]**), so
  a deleted node can be re-created under the same node id; it gets a new `fileId`.
- A node is one folder or one file for its life: a version that changes its `fileType` isn't
  read. Nor is a node whose `uniqueId` is the root id, nor a file whose `uniqueId` is a role
  folder's or Recovered files'. The client reports such a row, as one that doesn't decrypt,
  and keeps it as there: what is in a folder it can't read stays in it, and a node whose newer
  version it can't read keeps the version it had (an edit made on a computer then goes up as
  a new node beside it).
- A node whose `groupId` is a file's node id is moved into Recovered files by the computers,
  as an orphan is.

## Recently deleted
- A delete removes: one metadata-only update that sets `archivalStatus` to `2`, keeping the
  content, payload, `groupId` and name (`update-archival-status-batch`, many files in one
  request, each at its `versionTag`: homebase-api-notes). Every removed node is marked so
  itself: a removed node is out of the folder tree for every client, and restorable.
- **When:** a node's removal time is its header's `updated`, the server's time of the write
  that removed it (odin-core's `record.modified`, **[verified]** `FileMetadata.cs`). Nothing
  writes to a removed node, so it stays that time (see the rules below).
- **A folder first:** deleting a folder removes the folder by itself, then everything in it
  (batches of the files below it), so what was deleted with a folder is removed no earlier than
  the folder, even if the delete stops half way.
- **Deleted with its folder:** a removed node whose folder is removed too was deleted with it if
  it was removed no earlier than the folder; one removed before was deleted on its own. A
  Recently deleted view lists the items deleted on their own, each with what was deleted with
  it. (Writers from before 2026-10-15 removed what was in a folder first: their folder deletes
  list as separate items until they expire.)
- Restore is the update back: `archivalStatus` `0` (and new bytes, when an edit
  restores it; the same batch when nothing else changes). A user's restore (from Recently
  deleted) of a folder also restores the removed nodes below it that were deleted with it (not
  what was deleted from it before); a restore by a write (below, The server's time decides)
  doesn't. A node whose parent is removed brings the parent back first, and one whose parent is gone for good comes back as it was, an orphan
  the computers move into Recovered files (see above). If a folder
  doesn't come back (changed meanwhile), what is in it stays removed. A name a live sibling took
  meanwhile gets ` (restored)`; so does `.hbsync`, in any case, at the top of the drive or of a
  role folder (each computer's own state folder there, never synced).
- **The purge**, the one rule for deleting for good (a user emptying Recently deleted follows
  it too, but for 3.). Any client runs it (the mobile browser too, at its own pace: most people
  rarely open a desktop app), by the server's clock: its `queryTime` against each `updated`.
  1. **What goes:** an item deleted on its own, with everything deleted with it, once all of it
     was removed more than 30 days ago (every client's, not a setting; strictly: `updated`
     before `queryTime` − 30 days), each node by its own removal: one removed later (in a
     folder a live node held, say) has its own 30 days, and the item waits with it. Files go before their folders, each at the version the client knows: one written
     since stays. An item holding a removal whose time the client doesn't know yet waits for a
     purge that knows it. A user emptying an item takes everything removed below it, through
     removed folders: what was deleted from it before too (a sub-folder deleted first, listed
     on its own), else the folder would stay holding it; as it was listed to the user: a
     removal that arrived below it since stays, and so does the folder holding it.
  2. **Never a live node:** the purge and emptying delete removed nodes only, never a live
     node. A folder holding a live node, or a removed node that isn't going, stays with the
     folders it is in, emptied of the rest: it never goes for good while it holds one, however
     old. **What a delete left:** a delete that stopped half way (the device that made it
     never came back, and no syncing device finished it) leaves live nodes in the removed
     folder that its delete took along (below). So the purge first removes them (to Recently
     deleted, restorable), for the items it would delete (removed more than 30 days ago): a
     live node below a removed folder of such an item, directly or through live folders,
     written no later than the removal of the nearest removed folder above it (the server's
     `updated` of the version the client knows, against the folder's removal). It reads each
     removed folder that directly holds such a node first, and goes on only if it is still
     removed at the version the client knows. Then, top-down (a folder before what is in it,
     a level at a time), it removes them with `update-archival-status-batch`, each at the
     version the client knows: one written since (`versionTagMismatch`), or gone, stays live,
     and so does what is in it. It leaves alone a node whose time the client doesn't know, a
     node written after the removal (an edit, a folder moved in) and a role folder, the live
     folders above them, and what is in a folder one of them keeps. A live folder it removes is
     the nearest removed folder of what is in it from then on, so a node in it the client
     didn't know of (written since its last read, or one it doesn't read) would count as taken
     along by it: once the server's queries show what was written up to its last removal (1.1
     s), it asks, in one question, whether any of those folders still holds a live node, and
     puts those back (`archivalStatus` 0, at the version it wrote), with the ones above them
     it removed, top-down; what went with them stays removed. Each node it removes is deleted
     with its folder (removed after it) and has its own 30 days: the item goes once they are
     past too. What the purge
     doesn't take (a node written after the removal, one whose time isn't known, a file this
     client doesn't read, such as another app's) keeps its folder in Recently deleted,
     restorable, until it is restored or the node is removed. Before a pass of folders goes, the server is asked,
     in one question, whether a file that isn't deleted is in any of them, live or removed (one
     this client doesn't read, or doesn't know of yet, too: homebase-api-notes, Change feed),
     leaving out the rows of the later passes at the versions the client knows (round a cycle
     of removed folders, a folder holds one still to go). The earlier passes' files are gone by
     then and nothing in a pass is in another node of it, so such a file isn't going: its
     folder stays, with the folders it is in, until that file goes too (a purge that knows it;
     one no client reads keeps it indefinitely, as a live one does). So a folder goes for good
     only with everything removed below it, but round a cycle of removed folders, where one
     goes first. As that query leaves out what changed in its last
     second, the client asks 1.1 s after the pass begins, when it shows all written before (a
     pass deleting no folder asks nothing, and doesn't wait).
     What a client still misses (a live file written into a folder in the moment between the
     question and the delete) is left with no folder: computers move it into Recovered files
     (above).
  3. **Without a trace, or with one:** each node by its own trace and time. The purge takes
     only nodes removed more than 30 days ago (1.), and each goes without a trace (a hard
     delete: `hard-delete-batch`, odin-core #1968), nothing of the file left, in the feed
     either. What a user empties by hand is deleted softly, leaving a tombstone, the
     trace (`soft-delete-batch`, odin-core #1977, many files a request, each at its version);
     any client deletes a tombstone hard more than 30 days after its own `updated`, at its
     version, both from its row in the feed (the soft delete's answer gives the time, not the
     version). A node whose time isn't known never goes without a trace.
  4. **Why 30 days:** the feed keeps a trace of a delete that long and no longer, so every
     device that reads the feed at least every 30 days learns of every delete. One that hasn't
     read it for 29 days (a day's margin) reads all of it again: what it has that the feed no
     longer shows is asked of the server (the feed leaves out some rows it has, **[verified]**:
     homebase-api-notes, Change feed) and gone only if the server says it is a tombstone, or
     says not found twice, a minute to an hour apart, at the version known (5.); one it can't
     ask now stays, asked again at its next read (a hundred at a time). A re-read cut short and
     taken up after as long starts over.
  5. **Gone for good:** a hard delete is announced on the notification socket, and a device
     takes it at once. A write at a version the client knows that finds the file not there,
     and its header not found either (or a tombstone; an answer per file that says it is one
     needs no header read), forgets it: a tombstone at once, a header not found only once it is
     not found again a minute to an hour on, at the same version (one wrong 404 never makes a
     live file gone; a row of the file or a header found meanwhile clears the doubt; a "not
     found" counts only from the identity itself, homebase-api-notes). The next sync puts what the
     device has up anew. A delete, soft or hard, answered not found for that file (not a failed
     request) is checked the same way. A request that fails leaves the files it didn't answer
     for the next purge, whatever its error. A client keeps the ids of files
     deleted for good, so no stale row brings one back, and forgets one 30 days after it went
     (its tombstone too, if it left one): no page can show it by then (a page shows only what
     the server had when it read it, a pull applies its pages in order and never replays older
     rows, and a re-read starts over after 29 days).
- **The server's time decides** what a folder's delete took along. A live node in a removed
  folder (directly, or through live folders it took along too) whose version was written no
  later than the removal of the nearest removed folder above it (its header's `updated`
  against the folder's) was in the folder when it was deleted: it is deleted with it, though
  its own removal may still be on its way (a folder goes first). A client takes it as removed,
  and any client may mark it so; one that doesn't know the node's time (its own write, not
  back from the feed yet) takes it as written before. Deleting for good never takes it while
  it is live: the purge removes it first, 30 days after the folder's removal (The purge, 2.). A node written after the removal (an edit, a new file, a folder moved in, by
  a device that didn't know of the delete yet) is a change of its own: it stays with all that
  is in it, and brings its folders back: each folder with what its delete took along that is
  still live (not changed on the device that brings it back), but not the removed nodes below
  it, nor what a removed folder below took along (sync-semantics §6.3). One rule decides this
  for the reconciler and Recently deleted (`crate::along::taken_along`). A removed folder
  outside the part of the drive a device syncs, or one it ignores, isn't its to bring back.
- Homebase gives `archivalStatus` no meaning: each app picks its values. `2` is odin-js mail's
  trash and the `Removed` of odin-js's and chat-kmp's types. It is replaced, like the whole
  header, on every update: a write that leaves it out sets it to `0` **[verified]**
  `StandardFileUpdateWriter.cs:69`, `UploadAppFileMetaData.cs:21`. Every write sends it.

## Rename and move
One metadata-only update of `groupId` and/or `content.name`, guarded by `versionTag`.

## Rules for other clients
What a client that isn't this one (the mobile Filesync mini-app in chat-kmp, odin-js) must do to
write nodes this client syncs. The file browser's own guide, built on these rules, is
`file-browser.md`.
- **Create:** `v`, `name`, and for a file `mtime` and `size`, plus one `sync_data` payload with its
  hash. Every optional key may be left out; send `ct` only if the platform knows the item's real
  creation time (a photo's capture date counts), never the upload time.
- **Update** (rename, move, new bytes): read the node's content, change only the keys the update
  is about and write the rest back as read, unknown keys included. Never rebuild content from a
  model that knows only some keys: that drops `ct` and `x` (and any later key) for every device.
  If a client can't keep unknown keys, it doesn't update nodes.
- **Read:** ignore keys you don't use. Showing `ct` and `tags` is optional. Leave nodes with
  `archivalStatus` `2` out of the folder tree (a "Recently deleted" view may show them).
- **Delete:** remove (set `archivalStatus` `2`), never delete a live node outright; a folder
  first (Recently deleted). `update-archival-status-batch` does it for up to 1,000 files in a
  request, each at its `versionTag`, without reading or rewriting content: send a folder's
  removal before (not in the same request as) what is in it, and leave a file it answers
  `versionTagMismatch` for as it is.
- **A removed node** is never written to except to restore it: its `updated` is its removal
  time. That also rules out what odin-core counts as a change of it **[verified]**
  `TableDriveMainIndex.cs` (`UpdateReactionSummaryAsync`, `UpdateTransferSummaryAsync`,
  `UpdateLocalAppMetadataAsync`): no sharing or sending it to another identity, no reactions or
  comments, no local app data (local tags, local content, read time).
- **Tags:** a client that lets people change tags writes the whole `tags` list as it should
  be now (the other keys as read), never a part of it. A client that doesn't keeps `tags` as
  read, like any other key.
- **Every write** sends `archivalStatus`: Homebase sets a missing one to `0`.
- **Recovered files** (see above): show it as any folder, known by its `uniqueId`. Moving
  orphans into it is the computers' job; another client may leave it to them, as it may the
  purge's first step. One that does it follows the rules above: the fixed id, and a restore
  rather than a create when the folder is in Recently deleted.
- **Role folders** (see above): keep them at the top: never move one into a folder (a computer
  then leaves it out there), and never create one, under any parent (that is the computers'
  job; restore one that is in Recently deleted). Renaming one changes only the drive's name:
  computers know it by its id and keep their own folder's name. Deleting one lasts only if no
  computer syncs it: one that does brings it back, with what was in it, at its next sync (sync
  semantics §12); say so before deleting one. Show them at the top, by their own icons.

## Changes
- 2026-10-23: a computer reads a node with `v > 1` as a row it can't read (it never wrote over
  one before either, but nothing stopped it). The node is the same.
- 2026-10-23: a live node written into a removed folder at a place a computer ignores is left
  there for the computers that sync that place, which bring the folder back (before, a
  computer that ignored it could move it into Recovered files). The node is the same.
- 2026-10-22: a computer moves an orphan into Recovered files (or to the top) only where it
  syncs that place: one at a place every computer ignores stays where it is. The node is the
  same.
- 2026-10-22: a folder brought back by a write keeps what its delete took along that is still
  live (computers have done so since 2026-10-22; this page said "the folders alone"); the
  removed nodes below it stay removed. The node is the same.
- 2026-10-21: a restore never brings a node back as `.hbsync` (any case) at the top of the
  drive or of a role folder: it takes ` (restored)` there, as a name taken.
- 2026-10-21: a computer keeps a row it can't read (one that doesn't decrypt, a newer
  writer's, a change of `fileType`, now not read) as there: what is in such a folder is no
  longer moved into Recovered files, and an edit of a node whose newer version it can't read
  goes up as a new node instead of failing at every sync. The node is the same.
- 2026-10-20: a rewrite keeps what it doesn't act on as another writer wrote it: `tags` in their
  form until the set changes, no `mtime` on a file that had none (before, `0`; a download no
  longer dates such a file 1970), and a folder's `mtime`, `size` and `x`. The header fields
  sync doesn't use are listed as reset on write (they always were). The node is the same.
- 2026-10-20: readers take a fractional `mtime` (rounded) and `x` as `1` or `0`, which
  JavaScript and Kotlin writers may send (before, such a node was unreadable). A node with the
  root's id, or a file with a folder's fixed id, isn't read. A node whose `groupId` is a file
  moves into Recovered files, as an orphan does. Writers write as before; the node is the same.
- 2026-10-20: a reader skips a file whose payload's `bytesWritten` isn't the encrypted size of
  its `size` (every download of it would fail). This client writes them so (**[verified]** by
  the live test `nodes_are_stored_encrypted`; other writers inferred, as encryption requires);
  the node is the same.
- 2026-10-20: a file whose header is not found is gone for good only once it is not found again
  a minute to an hour on, at the version known; a tombstone (a header that says deleted, a 4002
  answer) is proof at once (before, the first "not found" made it gone). The rule is a
  computer's: a client that keeps no record of gone files, as the mobile browser, is unaffected.
- 2026-10-20: a user emptying an item takes it as it was listed to them: a removal that arrived
  below it since stays in Recently deleted, and so does the folder holding it (before, the
  desktop took everything removed below it when it ran).
- 2026-10-20: a restore whose folder is gone for good undoes only the removal (`archivalStatus`
  `0`, `groupId` as it was), leaving an orphan the computers move into Recovered files (before,
  the restore moved it to the top under a free name).
- 2026-10-19: Recovered files, a folder at the top with the fixed node id
  `fa4646cb-2df0-4f18-a3c6-1fc2b2f6ac4b`, named `Recovered files` when made: a live node whose
  folder is nowhere on the drive is moved into it on the drive (before, computers moved it to
  the top, or, where its folder was in Recently deleted with no folder above it, it showed
  nowhere). The node is the same.
- 2026-10-19: before a pass of folders goes for good, the server's answer counts any file in
  them that isn't deleted, a removed one too, but the later passes' rows at their versions
  (The purge, 2.): a removed node the client didn't know of (removed since its last read, or
  one it doesn't read) keeps its folder, which stays in Recently deleted until that node goes.
  Before, only a live file did, and the folder could go for good while that node stayed. The
  node is the same.
- 2026-10-19: deleting for good softly (emptying by hand, and the purge's younger nodes) goes
  through `soft-delete-batch` (odin-core #1977), up to 1,000 files a request, each at its
  version; the answer gives the tombstone's time, from which a client keeps the file's id 30
  days (The purge, 5.). The node is the same.
- 2026-10-19: the purge removes what a delete that stopped half way left live (The purge, 2.):
  the live nodes the delete of an item it would delete took along (written no later than the
  removal, by the server's time, at the version the client knows; the folder confirmed still
  removed), top-down, with `update-archival-status-batch`; a live folder it removed that the
  server then says still holds a live node is put back. They go for good 30 days after their
  own removal, the folder with them. A removed folder no longer stays indefinitely for
  a delete that never finished; one holding a node written after the removal, or one the
  client doesn't read, still does. The node is the same.
- 2026-10-18: emptying an item of Recently deleted by hand takes everything removed below it,
  what was deleted from it before too (it stayed, holding that); the node is the same.
- 2026-10-18: removals and restores that change nothing else go through
  `update-archival-status-batch` (odin-core #1964), many files a request; the node is the same.
- 2026-10-16: emptying and the purge delete for good only removed nodes, never a live node: a
  folder holding a live node stays until the purge has marked those nodes removed (next).
- 2026-10-17: the purge (above, the one rule for deleting for good): each node goes without a
  trace once its own removal is 30 days old (a hard delete, odin-core #1968), else leaving a
  tombstone, deleted hard 30 days on; a device away 29 days reads the whole feed again; ids of
  files gone for good are kept 30 days. Any client purges, the mobile browser too. It never
  writes a live node, so it doesn't mark what a delete took along (the "until the purge has
  marked" above is dropped): a removed folder still holding a live node stays in Recently
  deleted, restorable, until its delete finishes or it is restored, possibly indefinitely. The
  server is asked about a pass's folders 1.1 s after the pass begins. An item goes once all of
  it is 30 days old, each node by its own removal. A file is gone only by the server's answer
  for it, confirmed by its header.
- 2026-10-16: what a folder's delete took along goes by the server's time too: a live node in a
  removed folder written no later than the removal is deleted with it, one written after keeps
  its folders alive (an edit made before a folder's delete goes with it, into Recently deleted).
- 2026-10-15: Recently deleted by time: a delete sets only `archivalStatus` `2` (no `rm`), a
  folder before what is in it; the removal time is the header's `updated`, and what was deleted
  with a folder is what was removed no earlier than it. Nothing writes to a removed node: no
  sharing, reactions or local app data either. An `rm` from before is ignored, and the next
  write drops it.
- 2026-10-15: the rules for role folders match the computers' (§12): renaming one is harmless,
  deleting one lasts only where no computer syncs it, moving one into a folder is never done.
- 2026-10-14: role folders: Documents, Pictures, Music, Videos and Downloads have fixed node ids
  at the top; computers sync them where they are, never inside the main folder.
- 2026-10-12: `tags`, the Finder tags and colours (optional, files and folders), synced.
- 2026-10-12: Recently deleted: a delete sets `archivalStatus` `2` and `rm`, kept 30 days.
- 2026-10-12: other apps find the drive by slug too (the alias also works); readers keep a file
  without `mtime`.
- 2026-10-12: `ct`, the creation time (optional, files and folders).
- 2026-10-11: every file's `sync_data` carries its hash; a file without one isn't a node.
- 2026-10-10: every node encrypted.
- 2026-10-08: v1.
