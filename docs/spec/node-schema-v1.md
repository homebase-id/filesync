# Sync-node schema v1

How one sync node is stored as one Homebase drive file. Every client that reads or writes a
"Files" drive (this client, odin-js, chat-kmp) follows this schema. Fixtures live in
[fixtures/](fixtures/).

Markers: **[verified]** = checked in `../odin-core` code; **[provisional]** = our choice, not yet
reserved in odin-core.

## Drive
- One Homebase drive per sync root, of the "Files" drive type.
- The Filesync app's drive: slug `filesync`, type slug `filesync`, drive type GUID
  `5da48122-e946-487a-8b9d-589642f3c91d` (`crates/hbsync-homebase/src/app.rs`). Clients find it
  by slug (`GET /api/v2/apps/filesync/drives/filesync`), not by GUID. The GUIDs are
  **[provisional]** until odin-core reserves a drive kind (foundations §5 item 5).
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
{ "v": 1, "name": "Report Q3.pdf", "mtime": 1759920000123, "size": 52311, "x": true }
```

| Key | Type | Required | Meaning |
|---|---|---|---|
| `v` | int | yes | schema version, `1` |
| `name` | string | yes | original name, Unicode NFC. Never contains `/` or NUL, never `.` or `..`, not empty |
| `mtime` | int | files | local modification time, Unix ms |
| `size` | int | files | plaintext size in bytes |
| `x` | bool | no | executable bit; omitted when false |

- Readers ignore unknown keys. Writers keep unknown keys they read (forward compatibility).
- A reader that finds `v > 1` treats the node as read-only.
- Content is capped by the server at 10,240 characters **[verified]**; with names capped at 255
  UTF-8 bytes by this client, v1 content stays far below it.

## Payload `sync_data`

| Field | Value |
|---|---|
| `key` | `sync_data` (matches `^[a-z0-9_]{8,10}$` **[verified]**) |
| `contentType` | `application/octet-stream` |
| `iv` | the payload IV (`encryption.md`); empty when unencrypted |
| `hash` | `{ "algorithm": "blake3", "storedHash": b64, "contentHash": b64 }` |

- This client writes BLAKE3. It accepts `sha256` nodes written by other clients and computes
  SHA-256 for the local file when it needs to compare.
- Encrypted: `storedHash` is over the ciphertext; `contentHash` is the plaintext hash,
  encrypted (`encryption.md`).
- Unencrypted (nodes written before encryption): `contentHash == storedHash` **[verified]**
  rule 5 of `../odin-core/docs/payload-hashes.md`.
- A node with no `hash` (drive without the requirement) is downloaded, hashed locally and
  marked unverified.

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
