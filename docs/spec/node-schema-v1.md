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
  `5da48122-e946-487a-8b9d-589642f3c91d` (`crates/hbsync-homebase/src/app.rs`). Clients find it
  by slug (`GET /api/v2/apps/filesync/drives/filesync`), not by GUID. The GUIDs are
  **[provisional]** until odin-core reserves a drive kind (foundations §5 item 5).
- Another app's client finds it the same way: the route names the app that owns the drive
  (`filesync`), not the caller, and returns the drive to any app or owner allowed to see it
  **[verified]** `V2AppDriveController.GetAppDrive`, `DriveManager.GetDriveBySlugAsync`. The
  alias `cfceb937-b868-496b-91cb-3402f18320f8`, fixed in the Filesync app's drive request, also
  works meanwhile; if it ever changes, a line under [Changes](#changes) says so.
- The drive should have `requirePayloadHashes = true` (owner setting; apps can't set it
  **[verified]** `OwnerDriveManagementController.cs`).

## Root
- Root node id: `2a1c0d55-8ad3-417e-a5d7-42c3d0baba0e`, the same on every drive.
- The root is never stored as a file. A node with `groupId == root id` is top level.

## File header fields

| Field | Folder | File |
|---|---|---|
| `appData.fileType` | `7101` **[provisional]** | `7102` **[provisional]** |
| `appData.dataType` | `0` | `0` |
| `appData.uniqueId` | node id | node id |
| `appData.groupId` | parent node id | parent node id |
| `appData.content` | JSON, see below | JSON, see below |
| `appData.tags` | none | none |
| `isEncrypted` | `true` (`encryption.md`) | `true` (`encryption.md`) |
| payloads | none | exactly one, key `sync_data` |

The file types must stay outside the server's reserved range `2100000000..int.MaxValue`, which
app uploads may not use **[verified]** `ReservedFileTypes.cs`, `StandardFileStreamWriter.cs:35`.

## `appData.content`

```json
{ "v": 1, "name": "Report Q3.pdf", "mtime": 1759920000123, "size": 52311, "x": true, "ct": 1451606400123 }
```

| Key | Type | Required | Meaning |
|---|---|---|---|
| `v` | int | yes | schema version, `1` |
| `name` | string | yes | original name, Unicode NFC. Never contains `/` or NUL, never `.` or `..`, not empty |
| `mtime` | int | files | local modification time, Unix ms |
| `size` | int | files | plaintext size in bytes |
| `x` | bool | no | executable bit; omitted when false |
| `ct` | int | no | creation time, Unix ms (files and folders): the item's own when the node is created, then never changed. Shown and applied (Windows, macOS; Linux can't set one), never a reason to sync, like `mtime`. Readers round a fractional value and take anything else as none (and keep it) |

- "Required" is for writers. A reader skips a file without `size` (an encrypted payload's stored
  size is padded, `encryption.md`) but keeps one without `mtime`, as a file with no date (this
  client reads it as 0).
- Readers ignore unknown keys. Writers keep unknown keys they read (forward compatibility).
- `ct` is kept the same way through every rewrite (new bytes, a rename, a move): editors that
  save by writing a new file and renaming it over the old one would otherwise reset it. A
  download gives it to the file once in place (after the mtime). A node without one (from
  before `ct`, or a client that sends none) gets its file's on its next update or rename.
- A reader that finds `v > 1` treats the node as read-only.
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
  as unreadable and reports it.

## Identity and tombstones
- `uniqueId` is the node id. Creates are idempotent: a retry that gets `ExistingFileWithUniqueId`
  (4105) means the create already landed **[verified]** `FileSystemStreamWriterBase.cs:269-283`.
- A tombstone keeps `fileId`, `groupId`, `fileType` and nulls `uniqueId`, `content` and payloads
  **[verified]** `DriveStorageServiceBase.cs:1923-1965`. Clients therefore map
  `fileId → node id` themselves.
- Tombstoned UniqueIds are free again (the duplicate check skips tombstones **[verified]**), so
  a deleted node can be re-created under the same node id; it gets a new `fileId`.

## Rename and move
One metadata-only update of `groupId` and/or `content.name`, guarded by `versionTag`.

## Rules for other clients
What a client that isn't this one (the mobile Filesync mini-app in chat-kmp, odin-js) must do to
write nodes this client syncs:
- **Create:** `v`, `name`, and for a file `mtime` and `size`, plus one `sync_data` payload with its
  hash. Every optional key may be left out; send `ct` only if the platform knows the item's real
  creation time (a photo's capture date counts), never the upload time.
- **Update** (rename, move, new bytes): read the node's content, change only the keys the update
  is about and write the rest back as read, unknown keys included. Never rebuild content from a
  model that knows only some keys: that drops `ct` and `x` (and any later key) for every device.
  If a client can't keep unknown keys, it doesn't update nodes.
- **Read:** ignore keys you don't use. Showing `ct` is optional.

## Changes
- 2026-10-12: other apps find the drive by slug too (the alias also works); readers keep a file
  without `mtime`.
- 2026-10-12: `ct`, the creation time (optional, files and folders).
- 2026-10-11: every file's `sync_data` carries its hash; a file without one isn't a node.
- 2026-10-10: every node encrypted.
- 2026-10-08: v1.
