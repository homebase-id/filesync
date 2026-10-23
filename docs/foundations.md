# Homebase desktop sync client: foundation decisions

## Context
Before building the Rust sync client (Windows/macOS/Linux), four foundation decisions need to be
fixed, because they shape the remote data model and every module above it:
1. content hashes in the Homebase file metadata
2. how the folder structure is represented in a (flat) Homebase drive
3. how to avoid a Dropbox-style expensive start-up
4. how remote-only / offline files are supported

Explored 2026-10-08: `../odin-core` server model and `../chat-kmp` `HomebaseFile`.
**[read]** = seen in code; **[inferred]** = a conclusion that was not verified directly.

## 0. What the Homebase model gives us today

- **No content hash anywhere** [read]: not in the server model, not in payload descriptors, not in chat-kmp, and no ETag. `VersionTag` is a sequential GUID that changes on every header write. It is a concurrency token, not a content identity.
- **Payload `LastModified` is server time at upload** [read]. `BytesWritten` is the stored size (ciphertext when encrypted). Neither can serve as the file's local mtime or plaintext size.
- **Drives are flat** [read]. There are no folders, paths or parent ids. Available primitives:
  - `UniqueId`: client-chosen, unique per drive.
  - `GroupId`: indexed, non-unique, has delete-by-groupId.
  - `Tags`, `FileType`/`DataType`: indexed.
  - `AppData.Content`: ≤10,240 chars, encrypted with the file.
- **Payload key must match `^[a-z0-9_]{8,10}$`** [read]. A filename can never be the payload key.
- **Header writes are version-checked by the DB** [read]. A stale `VersionTag` → `VersionTagMismatch`. One update PATCH can replace a payload and the header together.
- **Change feed** [read]: `query-batch` with `AnyChangeDate`, `OldestFirst`, no FileState filter, and a persisted cursor returns creates, updates and tombstones. The WebSocket push is best effort only.
- **Tombstones** [read]:
  - They keep fileId, GroupId, FileType and GlobalTransitId.
  - They **null the UniqueId** and clear Content and Payloads.
  - [inferred] User-delete tombstones are never reaped.
- **Large-file limits** (odin-core #1892, #1893; reads stream and ranges are 64-bit since PR #1921, the upload byte count since PR #1932; the rest of this list is as read before them):
  - **Uploads stream; downloads do not** [read]. `GetPayloadStreamAsync` loads the payload or range into a `MemoryStream` (`LongTermStorageManager.cs:308,312`). A streaming `OpenReadAsync` exists but only payload move uses it.
  - **32-bit sizes** [read]:
    - `FileChunk.Start` / `Length` are `int`, parsed with `Convert.ToInt32`.
    - An open-ended range becomes `int.MaxValue`.
    - The stream writer returns `(uint)output.Length`.
  - **No resumable upload** [read]: one HTTP request per payload.

## 1. Content hashes

**Two hashes per payload, both computed by the client:**
- **Stored hash**: over the bytes as uploaded and stored. That is the ciphertext for encrypted payloads, and the plaintext otherwise.
- **Content hash**: over the original plaintext.

An encrypting client hashes the plaintext going in and the ciphertext coming out, in the pass that sends it. The descriptor before the payload names the algorithm, and the values follow the payload (the deferred hash, odin-core #1935), so this client reads each source once (`spec/encryption.md`).

**Rules:**
- **Algorithm:** the client picks **SHA-256 or BLAKE3**. The set is closed at those two. The choice is recorded once per payload, and both hashes use it.
- **Optional but recommended.** A drive type may *require* hashes: the "Files" sync drive does.
- **Both or none.** The server rejects a payload that carries only one of them.
- **The server checks the stored hash** while the upload streams in. On a mismatch it rejects the upload; on a match it stores the hash. Nothing is returned, because the client already has both values.
- **Unencrypted payloads:** the two hashes must be equal, and the server checks both.
- **Encrypted payloads:** the content hash is stored **encrypted with the file's key**. The server treats it as opaque and only checks that it is present. A content hash in the clear would let anyone with server or storage access test whether you hold a specific known file.
  - **IV:** `hashIv = payloadIv XOR 0xAA…AA` over the full IV length. It is derived, so nothing extra is stored. XOR with a non-zero constant always gives a different IV; it never equals the payload IV.
  - **Binding rule:** the encrypted content hash is tied to the payload IV. A *different* hash may never be encrypted under an unchanged payload IV. Changing the algorithm or correcting a hash requires re-uploading the payload, which rotates the IV. Otherwise GCM would reuse a nonce.
  - Pinned with golden vectors.
- **Peer transfer:** the stored bytes are identical over peer, so both hashes travel unchanged. The receiving server checks the stored hash the same way.
- **The server never computes a hash on its own.** No hash from the client means no hash stored.
- **Scope:** payloads only; thumbnails are not hashed.

**Where they live:** on `PayloadDescriptor`, next to `Uid` and `LastModified`, as one optional
object (as implemented in odin-core #1895 / PR #1899, see `../odin-core/docs/payload-hashes.md`):
- `hash.algorithm` (`"sha256"` | `"blake3"`)
- `hash.storedHash`
- `hash.contentHash` (encrypted when the payload is)

They are stored in the existing header JSON (`hdrFileMetaData`; there is no payload table), so no DB migration is needed. Hashes are byte arrays serialized as **base64**, about 160 characters per payload against the column's 60,000-character cap. Tracked in odin-core **#1895**. A main-index column or hash→fileId table is added only if lookups by hash (dedupe) are ever wanted.

**Sync client usage:**
- It writes BLAKE3 for its own uploads.
- When comparing a local file against a node written with SHA-256, it computes SHA-256 for that one file on demand.
- It requires them: a file node without a content hash (or without its payload) isn't synced. The row is skipped as unreadable and reported, like one that doesn't decrypt. Every decision about a file's content rests on its hash; a node that could only be compared after a download is a way to lose an edit, and no writer needs it.

**What the hashes buy:**
- **Upload integrity**: the server rejects corrupted transfers.
- **Download verification**: the client checks the content hash of what it decrypted (a resumed or ranged download has no whole ciphertext to check the stored hash against).
- **Change detection** that doesn't depend on mtime.
- **Adopting an existing local copy** without downloading it.
- **Move detection**.
- **No false conflicts** when both sides made the same edit.

**Effect on other clients:**
- Apps that write can use SHA-256, which is native everywhere, so odin-js and chat-kmp don't *have* to ship BLAKE3.
- They need BLAKE3 only if they want to verify BLAKE3 payloads they download.
- The server needs both: SHA-256 is built into .NET; BLAKE3 needs a NuGet package (*to pick: licence and platform coverage*).

## 2. Folder structure: id-based tree, parent in `GroupId`

**Every node (file or folder) is one Homebase file:**

| Field | Use |
|---|---|
| `FileType` | `SyncFolder` / `SyncFile` (new reserved constants) |
| `UniqueId` | **node id**: a client-generated GUID, stable across rename and move. Nodes can be created offline, and creates are idempotent (`ExistingFileWithUniqueId` on retry = already landed) |
| `GroupId` | **parent node id**. The root is a fixed well-known GUID per sync drive |
| `AppData.Content` | `{ v:1, name, mtime, size, x? }`. `name` is the original Unicode (NFC). `mtime` is the original local mtime in ms. `size` is plaintext bytes. `x` = executable bit |
| Payload | one payload with a fixed key (e.g. `sync_data`) carrying both hashes in its descriptor. Folders have no payload |

**Why id-based:**
- Rename and move are one metadata-only, VersionTag-guarded update, whatever the subtree size.
- `GroupId` is indexed, so web and mobile clients can list a folder with one query.
- Empty folders exist naturally.

**Invariants the client enforces** (the server can't, because names are encrypted):
- **Unique names per folder** under NFC + case-fold, the strictest of the three OSes. A concurrent clash resolves deterministically: the lower node id keeps the name; the other becomes `name (conflict n)`.
- **No cycles.** Concurrent moves that would form a cycle: undo the later move by modified order.
- **Orphans.** If a parent is deleted while a child is added elsewhere, resurrect the parent.
- **Folder delete** tombstones the descendants bottom-up, then the folder. Server delete-by-groupId is one level only.
- **Local name mapping.** Names that are illegal on the local OS are escaped locally and the mapping is recorded. The remote name never changes because of one OS.

**Tombstones lose `UniqueId`**, so the state DB maps node id ↔ `fileId` and matches deletes by `fileId`.

**Drive layout:** one Homebase drive per sync root (a new "Files" drive type). No symlinks in v1.

## 3. Start-up performance: avoid the full rescan

Where Dropbox-class clients spend the time:
- walking the tree with one stat/open per file
- re-hashing
- holding the whole tree in RAM
- re-listing the remote

Each has its own fix.

**Tiered catch-up, using the best tier the OS and volume allow:**

| Tier | Mechanism | Cost at start |
|---|---|---|
| A. OS owns change tracking | macOS File Provider (replicated extension) | ~zero, no walk |
| B. Journal replay | Windows **USN journal** from the stored (JournalId, lastUsn) (unprivileged read via `FSCTL_READ_UNPRIVILEGED_USN_JOURNAL`, *to verify*); macOS **FSEvents** `sinceWhen = lastEventId` | proportional to *changes* |
| C. Fast metadata walk | Windows `NtQueryDirectoryFile`; macOS `getattrlistbulk`; Linux `getdents64` + `statx` (io_uring) | proportional to tree size, metadata only |

Fall back from A→B→C when the journal wrapped or its id changed, the FSEvents UUID changed or
`MustScanSubDirs` was set, or the volume is FAT/exFAT/SMB.

**Always, whatever the tier:**
- **Hash only when (size, mtime, ctime, file id) changed.** Never re-hash at start. A new file
  nothing on the drive can be compared with is hashed while it is sent (sync semantics §3,
  Hash on send): read once, not twice.
- **Streaming merge** of a sorted walk against sorted SQLite rows, so memory stays O(depth).
- **Don't block sync on catch-up.** It runs at background priority and only gates uploads of local state that is still unknown.
- **Remote:** resume from the persisted cursor; never re-list.
- **Safety net:** an idle-time verification walk.
- **First run** is the only full read. Adopting an existing local copy is hash-only.

**Linux** has no persistent unprivileged journal, so tier C is the default there. Target: a ~10^6-file
metadata walk in seconds on an SSD *(estimate, to benchmark)*.

## 4. Remote-only / offline files

There are three content states, separate from whether a node exists: **online-only**,
**local** (can be evicted) and **pinned**.

| OS | Mechanism | Notes |
|---|---|---|
| Windows | Cloud Files API placeholders | Pin states map 1:1; partial `FETCH_DATA` → needs ranged GET |
| macOS | File Provider (replicated extension) | Root lives in `~/Library/CloudStorage`; also gives tier-A change tracking |
| Linux | none native | v1 selective sync; optional FUSE mount later |

A placeholder needs name, size, mtime and hash without any content. The node's `Content` gives
the first three; the payload descriptor gives the hash.

## 5. odin-core work before the sync project

| # | Work | Status |
|---|---|---|
| 1 | Streamed payload and thumbnail reads via the existing `OpenReadAsync`, plus a ranged variant | odin-core **#1892**, done (PR #1921) |
| 2 | `long` for payload sizes and ranges throughout (`FileChunk`, `Convert.ToInt32`, `(uint)` byte count) | odin-core **#1893**, done (ranges PR #1921, the upload byte count PR #1932) |
| 3 | Payload hashes per section 1: descriptor fields, both-or-none, stored-hash check while streaming (also on peer incoming), equality check when unencrypted, per-drive "hashes required" setting, BLAKE3 package, spec + golden vectors | odin-core **#1895** |
| 4 | Resumable / chunked payload upload; the hash check runs once, when the staged file is finalized | *not filed yet* |
| 5 | "Files" drive type + reserved `SyncFile` / `SyncFolder` file types | *not filed yet*; can wait until the Homebase adapter (roadmap Layer 5) |

With #1892, #1893 and #1895 filed, the Homebase side of the design is settled. The rest of the
design work is on the client.

## Decisions (2026-10-08)
- **Hashes:** client-computed stored + content hash; SHA-256 or BLAKE3; optional on the server but required by the sync client; both or none; the server (and a receiving peer server) checks the stored hash; the content hash is encrypted on encrypted payloads with IV = payloadIv XOR 0xAA…AA; base64 on `PayloadDescriptor` (#1895), or deferred to a `payloadHash` part after the payload (#1935, how this client sends them). The sync client writes BLAKE3.
- **Folders:** id tree (`UniqueId` = node id, `GroupId` = parent id).
- **Large files:** fix the server first (#1892, #1893, both done; resumable upload still to file). Until then the client caps one file at 50 GiB, as a failed upload is sent again from the start.
- **Linux offline:** selective sync in v1, FUSE later.

