# Filesync drive: browsing the files on demand

Homebase Filesync (homebase-id/filesync) syncs folders on desktop computers with the user's
Homebase. A mobile browser can list and open those files without syncing them: it shows one
folder at a time and downloads a file only when it's opened. Full details:
filesync `docs/spec/node-schema-v1.md` and `docs/spec/encryption.md`. The schema doc is kept
current: read its "Changes" list before touching the mini-app. This guide is kept in filesync
(`docs/spec/file-browser.md`, moved from chat-kmp's `docs/filesync-browser.md` on 2026-10-14) and
changes in the same pull request as the schema.

## The drive
- Find it by slug: `GET /api/v2/apps/filesync/drives/filesync` (the drive type GUID
  `5da48122-e946-487a-8b9d-589642f3c91d` is provisional, so don't hard-code it).
- The app needs a read grant on the drive (write too, for uploading).

## Nodes
Every file and folder is one Homebase file:

| Field | Folder | File |
|---|---|---|
| `fileType` | `7101` | `7102` (both provisional: keep them in one constant) |
| `uniqueId` | node id | node id |
| `groupId` | parent's node id | parent's node id |
| payload | none | one, key `sync_data` |

- **Root id:** `2a1c0d55-8ad3-417e-a5d7-42c3d0baba0e`, the same on every drive. The root is
  never stored. An item with `groupId` = root id is at the top level.
- **Encrypted content (`appData.content`):**
  `{"v":1, "name":"Report.pdf", "mtime":1759920000123, "size":52311, "x":true, "ct":1451606400123}`.
  - `mtime` and `ct` (creation time, optional, files and folders) are in Unix milliseconds;
    `size` is the plaintext size; `x` means executable and is left out when false. Folders have
    `v`, `name` and maybe `ct`.
  - `tags` holds Finder tags, `[{"n": "Red", "c": 6}]` (`c` 1 grey … 7 orange, optional). The mini-app shows them as dots and
    chips and filters on them, and writes them back as read.
  - Ignore keys you don't know. If `v` > 1, show the item but don't change it.

## Role folders
Five top-level folders have fixed node ids, the same on every drive: Documents
`0274575b-c2cf-4ac7-b39b-d56b2c121b75`, Pictures `c2c9af9b-4873-48d8-a8ff-4779802f038f`, Music
`0f4cd8f9-a914-476d-86d0-b1d1a5215a85`, Videos `b133291b-9309-46c5-9bfd-ac6c81669049`, Downloads
`5290e709-bc6d-4e12-827e-96d6dd006403` (node schema, "Role folders"). A computer syncs each one in
place, from its own Documents folder and so on, never inside the main folder.
- Recognise them by `uniqueId`, never by name: show the role's icon (the schema suggests Fluent UI
  System Icons), perhaps listed first at the top. A plain folder with the same name is listed too.
- Otherwise they are ordinary folders: list, open, upload into them, rename and delete them as any other.
- Never create one with a new random id: if the role's id is in Recently deleted, restore it
  (Homebase refuses a second node with the same `uniqueId`). Creating them is the desktops' job.
- Deleting one while a computer syncs it doesn't last: that computer brings it back, with what
  was in it, at its next sync. Say so before deleting one; don't upload into a deleted one.

## Recovered files
A top-level folder with the fixed node id `fa4646cb-2df0-4f18-a3c6-1fc2b2f6ac4b`, named
`Recovered files` when made (node schema, "Recovered files"). The desktop computers move into it
what lost its folder for good: a live node whose `groupId` is no node on the drive (another
device wrote into a folder as it was deleted for good, another app deleted a folder outright, or
a restore brought back something whose folder went for good).
- Recognise it by `uniqueId`, never by name: the user may rename it, and a plain folder may be
  called `Recovered files` too (both are listed).
- Hide it while it is empty, or show it dimmed (slightly transparent); show it as any folder
  once it holds something. The desktops can't dim a folder: there it exists only once an orphan
  appears, and stays until the user deletes it (nothing removes it when empty: that would only
  add to Recently deleted).
- Otherwise it is an ordinary folder: list, open, move things out, rename or delete it.
- Leave the adopting to the desktops, as the purge's first step: don't move orphans into it,
  and don't create it. An item with no folder is simply not listed by the mini-app (no listing
  reaches it) until a computer moves it in.

## Listing a folder
- One `query-batch` with `groupId: [<folder's node id>]`, `fileType: [7101, 7102]`,
  `fileState: [active]` and `archivalStatus: [0]`, in pages. A deleted node is only removed:
  `archivalStatus` 2, still in its folder (filesync's "Recently deleted").
- Every row is encrypted: names, sizes and times. Decrypt each one, then sort on the device.
- Skip any row that won't decrypt, isn't encrypted or has a different file type; a name that
  breaks the name rules under "Uploading"; a file without `size` or without a hashed
  `sync_data` payload; and one whose payload's `bytesWritten` isn't `(size / 16 + 1) · 16`.
  Read a fractional `mtime` (round it) and `x` as `1` or `0`.
- A file never holds anything: a node whose `groupId` is a file is an orphan, which the
  computers move into Recovered files (not one that every computer filters: it stays, unseen
  by them; don't list it). Never change a node's file type, and never move a node into a
  file.
- odin-core has no index on `groupId` yet, so on a big drive each listing scans the whole drive
  (to fix in odin-core).

## What the list shows
Everything comes from the node's HomebaseFile; nothing is stored beside it.
- Kind and icon come from the name's extension: the payload's `contentType` is always `application/octet-stream`.
- Size is `content.size`; modified is `mtime` (files only); created is `ct` when set; `x` shows a "runs" badge.
- "On Homebase since" is the header's `created`; "Last uploaded" is `sync_data`'s `lastModified`; the info pane names the
  payload's hash algorithm.
- There are no thumbnails, and folders have no size or item count.

## Downloading
- Fetch payload `sync_data` and decrypt it with the file's key (AES-CBC, the same as other
  encrypted payloads).
- Range reads work: start one block before the range, for videos and resuming.
- Check the hash: the payload descriptor's `hash.contentHash` is the plaintext hash, encrypted
  with the file key and IV = payload IV XOR `0xAA…AA`. `hash.algorithm` is `blake3` (written by
  Filesync) or `sha256`: verify with the one it names. Refuse a file with an unknown algorithm,
  no hash, or a hash that doesn't match.

## Uploading (new file or folder)
- `uniqueId` = a new random GUID, `groupId` = the parent folder's node id, and always
  `isEncrypted: true` with a new random key.
- Files need a `sync_data` payload (`application/octet-stream`) with a random IV and both hashes:
  - `storedHash` over the ciphertext;
  - `contentHash` over the plaintext, encrypted as above;
  - Write BLAKE3. SHA-256 is accepted, but it costs Filesync an extra pass per download and can
    cause needless conflict copies. A file without both hashes is ignored by Filesync.
- Content: `v`, `name`, and for a file `mtime` and `size`, whole numbers (`x` a boolean). Send `ct`
  only when the platform knows the real creation time (a photo's capture date), never the upload
  time.
- Names: Unicode NFC, at most 255 UTF-8 bytes, no `/` or NUL, not `.` or `..`.
- `.hbsync` at the top or directly in a role folder (Documents, Pictures, Music, Videos,
  Downloads), in any case (`.HBSYNC`, `.HbSync`), is reserved: each synced folder's root holds
  the computer's own state folder there, which the computers never sync. Don't create, rename,
  upload or restore anything to it there: number around it (`.hbsync (1)`, `.hbsync (restored)`). One that
  already exists (another writer's) is listed with a note that it won't sync to computers.
  - Unique within the folder, ignoring case and Unicode form (as on macOS and Windows): add
    `(1)`, `(2)` and so on if needed.
- Rename or move: a header-only update of `content.name` and/or `groupId`, guarded by the
  `versionTag`. Any update decodes the content it read as a JSON object, changes only its own
  keys and writes the rest back, unknown keys included: rebuilding it from `FilesyncNodeContent`
  would drop `ct`, `x` and later keys for every device.
- Delete: remove (`archivalStatus` 2, content as read), a folder first (node schema, "Recently
  deleted"). `update-archival-status-batch` with `archivalStatus` 2 does it without reading or
  rewriting the content: up to 1,000 files a request, each with its `versionTag`, answered per
  file (`versionTagMismatch`: changed since, leave it). The folder in one request, then what is
  in it.
  Desktop clients delete their local copies of what the phone deletes, and keep nothing:
  Recently deleted on the drive is the only copy, for 30 days.
- A retry keeps the node id, including one the user asks for after a failure, so a retry that gets 4105
  (`ExistingFileWithUniqueId`) means an earlier attempt already succeeded.

## Recently deleted
filesync's own rules are in its node schema ("Recently deleted") and the engine's `deleted/` module (its pure rules in `deleted/rules.rs`); the mini-app follows them exactly.
- **Removed:** `archivalStatus` 2, at the header's `updated` (the server's time of the removal). Any other
  status is live. An `rm` in the content (from before 2026-10-15) means nothing; leave it out when writing.
- **Deleted with its folder:** a removed node whose folder is removed too, removed no earlier than the folder.
  One removed before it was deleted on its own. A live node in a removed folder (its delete still under way)
  written no later than the folder's removal is deleted with it too: it doesn't show anywhere, and comes back
  with the folder; one written after it keeps the folder alive (node schema, "Recently deleted").
- **Lists:** the whole drive with `archivalStatus: [2]`. The items are the ones not deleted with their folder;
  each counts what is below it in Recently deleted (filesync's `along`): what was deleted with it, which
  restoring it restores, and what was deleted from it before, listed on its own too.
- **Emptying an item** takes everything removed below it, through removed folders: what was deleted with it and
  what was deleted from it before (a sub-folder deleted first). Taking only what was deleted with it leaves the
  folder holding the earlier delete, so it stays. Take it as you listed it: a removal that arrived below it
  after the user looked stays, and so does its folder (the desktop does the same since 2026-10-20).
- **Deleting for good** (emptying, or the 30-day purge) takes removed nodes only, never a live node: a folder
  holding a live node, or a removed node that isn't going, stays with the folders it is in, emptied of the
  rest. So a removed folder still holding a live node stays in Recently deleted, restorable; it never goes for
  good while it holds one. What a delete that stopped half way left live, the purge removes first (below), so
  the folder goes later; a node written after the removal, or one no client reads, keeps it there until it is
  restored. Files go first, then each folder after what is in it.
- **Never write to a removed node** except to restore it: no rename, no sharing or sending to another
  identity, no reactions or comments, no local app data (local tags, local content, read time). odin-core
  counts each as a change, which moves its `updated`, its removal time.
- **Restore** is a metadata-only update by fileId:
  - Back under its own name in its own folder: `update-archival-status-batch` with `archivalStatus` 0,
    each item at its `versionTag`, a level at a time (each folder's request before what is in it).
  - Under a new name: an update with the node's own key with a new IV, its `versionTag`, an
    empty manifest (the payload stays) and `archivalStatus` 0.
  - The content is written back as read, unknown keys included, without an `rm` (the batch leaves the
    content, an old `rm` too: it means nothing).
  - Order: the removed folders above the item first, outermost first, then the item, then what was deleted
    with it (not what was deleted from it before).
  - An item whose folder is gone for good comes back as it was, in the same batch (its `groupId` names no
    node: an orphan, which the computers move into Recovered files; don't move it yourself). Until a
    computer is on, no listing shows it: say where it will appear ("in Recovered files, once one of
    your computers syncs"). One whose folder didn't come back stays removed.
  - A name a live sibling took meanwhile gets ` (restored)`, then ` (restored 2)` up to 99, before the
    extension.
  - A `VersionTagMismatch` or not-found skips the item: another device changed it. A folder above
    that is not found went for good: what was to come back into it comes back as it was, as above.
- **Delete forever:** only what the user saw.
  - Use `soft-delete-batch` (odin-core #1977), up to 1,000 files a request, each with its
    `versionTag` and no recipients: an item changed since isn't deleted, and the rest of the
    batch goes ahead. Read the answer per file: `errorCode` null is deleted (an item already a
    tombstone too), `versionTagMismatch` was written since and stays (with the folders above
    it), `fileNotFound` is done once its header is not found too (or a tombstone), any other
    code stays. A request that fails leaves the files it didn't answer as they were.
  - Delete files before folders, a folder only once what is in it went: soft, leaving
    tombstones (node schema, "The purge", 3.).
  - Not `delete-batch/by-file-id`: one stale tag refuses its whole batch.
- **The purge:** the mini-app purges too (when it opens, say), as every client does, by the
  one rule in node schema, "The purge": all of it (what goes, never a live node, without a
  trace or with one per node, asking the server 1.1 s into a pass before its folders go). On
  the wire: `hard-delete-batch` (odin-core #1968) without a trace, `soft-delete-batch`
  (odin-core #1977, as Delete forever) leaving one, each file with its `versionTag`; read the
  answer per file: `versionTagMismatch` was written since and stays (with the folders above
  it), `fileNotFound` is done once the file's header is not found too (or a tombstone), any
  other code stays and makes the purge fail (it runs again later). A request that fails
  leaves all its files for the next purge. A
  folder's contents on the server: a `groupId` query (homebase-api-notes, Change feed). Before a
  pass of folders goes, any active row in one (live or `archivalStatus` 2, of any type; minimal
  rows carry `archivalStatus` and `versionTag`, **[verified]** homebase-api-notes) keeps it, but
  the rows of your later passes at their `versionTag` (a cycle of removed folders): a removed file
  you didn't know of keeps its folder too.
  - **First, what a delete left** (node schema, "The purge", 2.): for each item the purge would delete
    (removed more than 30 days ago), the live nodes below its removed folders (directly or through live
    folders) that the delete took along: written no later than the removal of the nearest removed folder
    above (`updated` of the version you have, against the folder's). Skip a node whose `updated` you don't
    know, one written after, and a role folder, and the live folders above them. Read the header of each
    removed folder that directly holds one first: go on only if it is still removed at the version you
    have. Then remove them top-down, a level at a time, with `update-archival-status-batch`
    (`archivalStatus` 2, each at its `versionTag`): one answered `versionTagMismatch` or not found stays
    live, and so does everything in it. Then, 1.1 s after the last of those requests, ask the server (a
    `groupId` query) whether any live folder you removed still holds a live node (one you didn't know of
    would count as taken along by your removal), and put those back (`archivalStatus` 0, at the version you
    wrote), with the folders above them you removed, top-down. Don't delete them for good in the same
    purge: each has its own 30 days from this removal. A mini-app that doesn't do this leaves it to the desktop clients' purge; it
    must never delete a live node for good instead.
- **An item with `v` > 1** is listed, but restore and delete forever leave it as it is.
- **Every write sends `archivalStatus`:** the server replaces the whole header and takes a missing one for 0.
- **Only the header fields of the node schema last:** a computer's next write of the node resets the rest
  (an ACL that shares it, `appData.tags`, `userDate`, a preview, an `archivalStatus` other than 0 or 2).
  Don't build on them.
