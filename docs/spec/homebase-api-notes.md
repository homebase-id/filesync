# Homebase API notes (for the Layer 5 adapter)

Collected 2026-10-08 from `../odin-core` (C#) and `../chat-kmp` (Kotlin). **[verified]** = seen in
code (paths relative to `odin-core/src` unless they start with `KMP/` =
`chat-kmp/homebase-api/src/commonMain/kotlin/id/homebase/api`); **[inferred]** = a conclusion.

## API version
Use the unified **V2** API under `/api/v2`, as chat-kmp does **[verified]**
`KMP/client/OdinApiProviderBase.kt:82-83`. Drive routes take `driveId` = the drive alias.

## Authentication
- **YouAuth** **[verified]**:
  1. `GET https://{identity}/api/owner/v1/youauth/authorize` with `client_id` (= app id),
     `client_type=app`, `client_info`, `public_key` (P-384 JWK, base64url),
     `permission_request` (JSON, keys `n`, `appId`, `as`, `fn`, `d` drives, `p`, `return`, `o`),
     `state`, `redirect_uri`, optional `cipher`. `KMP/youauth/YouAuthFlowManager.kt:212-296`.
  2. The owner registers the app (if new) and consents; the callback gets `identity`,
     `public_key`, `salt`, `state`. `apps/Odin.Hosting/Controllers/OwnerToken/YouAuth/YouAuthUnifiedController.cs:140-217`.
  3. ECDH, then `POST /api/owner/v1/youauth/token` with `secret_digest`. The response carries
     the shared secret and the client auth token (CAT), both AES-CBC encrypted under the exchanged
     secret. `YouAuthUnifiedController.cs:344-368`, `KMP/youauth/YouAuthProvider.kt:98-154`.
  - Desktop redirect: `http://localhost:{port}/authorization-code-callback` (chat-kmp JVM).
- **Requests**: `Authorization: Bearer <CAT>` on V2 **[verified]**
  `apps/Odin.Hosting/UnifiedV2/Authentication/UnifiedAuthenticationHandler.cs:203-236`.
- **Drives**: an app cannot create a drive. The owner app creates a requested drive at
  registration when the request entry carries a name and drive type slug **[verified]**
  `odin-js/packages/apps/owner-app/src/hooks/apps/useApp.ts:55-88`. Drive request entry keys:
  `a`, `t`, `n`, `d`, `p` (Read=1, Write=2), `ds`, `ts`, `r`, `s`. The server requires both
  slugs (`ds`, `ts`) and derives none: a missing, empty or malformed one is refused
  (odin-core #1901, PR #1941; `DriveManager.CreateDriveAsync`, `OdinSlug.AssertRequired`)
  **[verified]**.

## The Filesync app on an identity (`crates/hbsync-homebase`, tried on frodo.dotyou.cloud 2026-10-09)
- **Ids and slugs** **[verified]**: YouAuth still requires an app id (`client_id`, and
  `appId` in `permission_request`; `YouAuthAppParameters.cs:10-13`), and a drive request names
  the drive by alias and type. Registration also requires the app slug `as`
  (`AppRegistrationService.AssignSlugAsync`), and the owner app requires `ts` (and reads `ds`)
  on every requested drive and refuses a request without `cd` (`RegisterApp.tsx:74`). Slugs:
  `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`, at most 14 characters (`OdinSlug.cs`). Filesync uses app
  slug `filesync` and one drive `filesync`/`filesync`; its ids are fixed in `app.rs`.
- **Signing in by slug** **[verified]** (odin-core #1902, PR #1933): a YouAuth `client_id` is
  the app id or the app's slug, any case (`YouAuthUnifiedService` looks a non-GUID up with
  `GetAppRegistrationBySlugAsync`), and `appId` in `permission_request` is needed only while the
  app still has to register. So a device that knows only the slug can sign in. Filesync still
  sends its fixed app id: registration needs one, and the same id on every device is harmless.
- **Addressing by slug** **[verified, tried]**: once registered, the client finds its drive
  with `GET /api/v2/apps/filesync/drives/filesync` (`V2AppDriveController.cs`); the answer
  carries `targetDrive {alias, type}`, `driveSlug`, `driveTypeSlug`, `appId`, `attributes`.
  File routes still take the alias.
- **Registration flow** **[verified, tried]**: an unregistered app is sent to
  `/owner/appreg`, which creates the requested drives and registers the app, then returns to
  the authorize URL (`YouAuthUnifiedController.cs:155-162` sets `return`), so the flow resumes
  by itself. An app cannot create drives itself.
- **Circles** **[verified]** (odin-core #1939, PR #1943, v0.1.960; odin-js #976): a circle has a
  slug, unique within the app that owns it, immutable. An app declares its circles at
  registration, in the YouAuth `ac` parameter (`YouAuthAppParameters.AppCirclesParamName`): a
  JSON list in a string of `{s: slug (required), n: name (required), d, e, g: [{a, t, p}], k}`,
  which the owner console shows on the install screen and sends as
  `AppRegistrationRequest.Circles`; they are created with the app, and may grant only what the
  registration grants it (`AssertDeclaredCirclesAsync`). Only the first install reads them, so a
  device finds the circle with `POST /api/v2/connections/circles/create` and the same `slug`
  (`{name, slug, description, emoji, driveGrants: [{permissionedDrive: {drive, permission}}],
  permissions: {keys}}`): when the app holds that slug it returns that circle's id and changes
  nothing (`CircleMembershipService.CreateAppCircleAsync`); without a slug every call makes a
  new circle. Circles made before had slugs made from their names (`CircleSlugGenerator`:
  "Filesync" is `filesync`, a second one `filesync-2`). Filesync's circle (`filesync`) grants
  Read on its drive and starts empty.
- **Desktop callback** **[tried]**: `http://localhost:{port}/authorization-code-callback`.
  Firefox may first open a connection the callback server can't read (an https attempt);
  ignore such connections rather than failing.

## Shared-secret encryption (mandatory, even for unencrypted files)
**[verified]** `apps/Odin.Hosting/Middleware/SharedSecretEncryptionMiddleware.cs`:
- JSON request bodies (POST/PUT/PATCH/DELETE) are `{"iv": b64, "data": b64}`, AES-CBC under the
  shared secret.
- GET query strings are encrypted into one `?ss=<envelope JSON>` parameter (`:181-185`).
- Encrypted responses carry `X-SSE: 1` (`:257`).
- Opted out per endpoint: multipart create/update, payload and thumbnail reads.
- The multipart `metadata` part is always AES-CBC encrypted with the shared secret and
  `instructions.transferIv` **[verified]** `FileSystemStreamWriterBase.cs:364-367`.

## Endpoints
Checked on odin-core branch `payload-hashes` @ 467d0b531 (the local dev host) **[verified]**,
and all used by `drive::HomebaseRemote` against frodo **[tried]** (the contract suite).

| Operation | Route | Notes |
|---|---|---|
| Create | `POST /api/v2/drives/{d}/files` | multipart, plaintext request and response (`V2DriveCreateFileController.cs`). Parts: `instructions` first, `metadata` second, then `payload`* (filename = payload key, case-sensitive) and `thumbnail`* (`V1DriveUploadControllerBase.cs:205-270`). Duplicate UniqueId → 400 4105, also when the node holding it is only removed (`archivalStatus` 2, in Recently deleted: **[verified]** live), so a role folder in Recently deleted is restored, never created again. Response `{fileId, driveId, globalTransitId, recipientStatus, newVersionTag, payloads[{key, uid, lastModified}]}` |
| Update | `PATCH /api/v2/drives/{d}/files/{fileId}` (or `/by-uid/{uid}`) | multipart, plaintext; `metadata` required. `fileMetadata.versionTag` required (4159), stale → 4160. Instructions need `locale: "local"` (missing → 500) and a `manifest`; `payloadDescriptors: []` is a header-only update that keeps the payloads. The key header sent is the file's own key with a new IV; the client has the key from its last write of the file or the file's last header or feed row (its token, `encryption.md`), so an update is one request. Ops `appendOrOverwrite` / `deletePayload`; unlisted payloads kept. Unknown fileId → 4106, deleted file → 4002 |
| Delete (soft) | `POST /api/v2/drives/{d}/files/soft-delete-batch` | up to 1,000 files a request, each at its `versionTag`, an answer per file: see below. The single `.../{fileId}/delete` (encrypted body `{recipients: [], versionTag}`; unknown file → 200 `localFileNotFound: true`; deleting a tombstone rewrites it: new versionTag, new feed row, another `fileDeleted`, **[verified]** `UpsertAllButReactionsAndTransferAsync`) the client no longer uses |
| Query | `POST /api/v2/drives/{d}/files/query-batch` | encrypted body `{queryParams, resultOptionsRequest}`; see below |
| Header | `GET .../files/{f}/header`, `GET .../files/by-uid/{uid}/header` | encrypted response with `content`. A deleted file → 200 `fileState: "deleted"`; unknown → 404 |
| Payload | `GET .../files/{f}/payload/{key}` | raw bytes; this route **ignores Range** |
| Ranged payload | `GET .../files/{f}/payload/{key}/{start}/{length}` | a `Range: bytes=a-b` header here wins and gives 206. With odin-core PR #1921 (merged 2026-10-11) **[verified]**: offsets are 64-bit, reads stream, an end past the payload is cut short there, a start at or past it → 416 (the client's `StaleRange`: the file changed since its header was read; it reads the header again, never takes it for a delete). Before it: an end past the payload → 416, offsets `int` (#1893), every read buffered whole in server memory (#1892) |
| Notify | `wss://{id}/api/v2/notify/ws-token` | see below |

`appData.archivalStatus` (an int Homebase gives no meaning) is how the client removes a node
to Recently deleted (node schema): **[verified]** an update copies it from the request
(`StandardFileUpdateWriter.cs:69`) and sets it to `0` when the request leaves it out (`int` in
`UploadAppFileMetaData.cs:21`); headers return it (`AppFileMetaData.cs:32`); query-batch filters
on it only when `archivalStatus` lists values (`QueryBatch.cs:130`), so the feed returns removed
nodes. odin-js mail uses `1` archived and `2` trash, chat `2` deleted and `3` archived.

A soft delete leaves a tombstone, how the other clients learn of a delete through the feed; a
hard delete leaves no trace in the feed **[verified]** (`DriveStorageServiceBase.cs`, tried on
frodo). Which the client uses when: node schema, "Recently deleted". It learns of another
device's hard delete from its notification only (`fileDeleted` with just `previousFileRef`,
below) and takes it at once, also while paused: no gathering, a commit to the account's DB (the
folders sync it at the resume), and the file id is never let back in (`gone_files`). What can
still miss one: a crash between the notification and that commit (moments), a commit that keeps
failing until the daemon stops (kept and retried meanwhile, 10,000 at most), and a hard delete
made while the client wasn't connected. Each leaves the file's row stale until it is next
written to (a file deleted for good answers not found, which forgets the row), or until the
client reads the whole feed again after a long absence (node schema, Recently deleted).

`POST /api/v2/drives/{d}/files/hard-delete-batch` (odin-core #1968) **[verified]**
`V2DriveBatchFileWriteOperationsController.HardDeleteBatch`, `HardDeleteBatchV2.cs`,
`FileVersionTagV2.cs` (commit 70c9d15bc):
- Body, encrypted like the other V2 bodies: `{files: [{fileId, versionTag}]}`, 1 to 1,000
  (`FileVersionTagBatchV2.MaxFiles`), each with both ids, none twice; else 400.
- Each file, active or a tombstone, is deleted with its payloads and thumbnails only at its
  `versionTag` (checked against the header read, and in the `DELETE` itself).
- Answer `{results: [{fileId, errorCode}]}`, one per file in request order: `errorCode` null
  when gone, `versionTagMismatch` (written since: left as it is), `fileNotFound` (missing,
  hidden by the ACL, which the owner's app never is, or deleted meanwhile: done, on a retry
  too), or another client error. One file's error never stops the others; only a server error
  or a drive-level denial fails the request.
- Nothing is left in the feed and nothing goes to peers; each deleted file is announced as
  `fileDeleted`, with only `previousFileRef` on a minimal subscription (**[verified]** live,
  `hard_purge` in `crates/hbsync-dev/tests/live.rs`).
- The old single `.../{fileId}/hard-delete` takes no version tag; the client doesn't use it.

`POST /api/v2/drives/{d}/files/soft-delete-batch` (odin-core #1977) **[verified]** on odin-core
main e0d52f336 (frodo runs it): `V2DriveBatchFileWriteOperationsController.SoftDeleteBatch`,
`SoftDeleteBatchV2.cs`, `V2DriveControllerBase.SoftDeleteFileAsync`, `FileVersionTagV2.cs`:
- The sibling of `hard-delete-batch` in the same controller, sharing its limits
  (`FileVersionTagBatchV2.AssertValidBatch`). Not the older `delete-batch/by-file-id` there,
  which refuses its whole batch for one stale tag.
- Body, encrypted like the other V2 bodies: `{files: [{fileId, versionTag?, recipients?}]}`, 1
  to 1,000 files, none twice, no empty `fileId` or `versionTag`; else 400 and nothing is
  touched. The client sends every file's `versionTag` and no recipients: with none, the
  transit permission isn't asked (`ResolveRecipientTokensAsync` checks it only for a
  recipient) and nothing goes to peers.
- Each file in request order, one after another (`PerFileAsync`): a tombstone already is
  answered as deleted, its tag not checked and the file not rewritten (a retry, another
  device's delete); a live file (removed too) goes only at its `versionTag`
  (`SoftDeleteLongTermFile`, checked against the header read), leaving a tombstone with a new
  version (`SaveFileHeader` makes one) and its row in the feed.
- Answer `{results: [{fileId, errorCode, updated, recipientStatus}]}`, one per file in request
  order: `errorCode` null when deleted, `versionTagMismatch` (written since: left as it is),
  `fileNotFound` (missing, or hidden by the ACL), or another client error; one file's error
  never stops the others, a server error fails the request. `updated` is the tombstone's time
  as the feed shows it (Unix ms; a tombstone already answers its own), null when it stayed.
  The answer carries no `versionTag`: the tombstone's comes with its row in the feed.
- The client deletes softly through it (`HomebaseRemote::delete`: emptying Recently deleted,
  and the purge's younger nodes), a request per 1,000 files. It records each file gone at once,
  since `updated` (`gone_files.gone_at`, which the 30 days of keeping its id count from), and
  the tombstone, at its version and time, from the feed's row, which the hard delete 30 days
  on needs (node schema, The purge, 3.).

`POST /api/v2/drives/{d}/files/update-archival-status-batch` (odin-core #1964, v0.1.962)
**[verified]** `V2DriveBatchFileWriteOperationsController.UpdateArchivalStatusBatch`,
`UpdateArchivalStatusBatchRequestV2.cs`, `UpdateArchivalStatusBatchResultV2.cs`,
`DriveStorageServiceBase.UpdateArchivalStatus` (odin-core main 4eb50e5db):
- Body, encrypted like the other V2 bodies: `{archivalStatus, files: [{fileId, versionTag}]}`,
  the files as for `hard-delete-batch` (1 to 1,000, none twice); `archivalStatus` required
  (missing is a 400, never a silent 0).
- Each file in request order, one after another (`PerFileAsync`): only `appData.archivalStatus`
  is set, the rest of the header (content, key header, payloads, `groupId`) stays, so neither
  the key nor the content is needed. A deleted file answers `cannotUpdateNonActiveFile`
  before the version is looked at, a stale tag `versionTagMismatch`, a missing one (or hidden
  by the ACL) `fileNotFound`. A file already at that status is left alone: its version tag and
  `updated` stay (a removal keeps its time). Otherwise the write moves its version and
  `updated` (`modified` = max(old + 1, now)) and raises the usual drive event. The client
  doesn't put a folder and what is in it in one request: a folder whose removal (or restore)
  is refused must hold back what is in it, and a request goes on past a refused file.
- Answer `{results: [{fileId, newVersionTag, updated, errorCode}]}`, one per file in request
  order; `updated` as Unix ms (**[tried]** live, `write_answers_carry_the_feeds_time`). One
  file's error never stops the others.
- The client removes and restores through it (`HomebaseRemote::set_removed`): a request per
  level of a folder's tree and per 1,000 files, a folder's level before what is in it. A
  `cannotUpdateNonActiveFile` answer is a tombstone (a file is active or deleted:
  `AssertActiveForUpdate`, **[verified]**): the file is taken as deleted for good without
  reading its header; a `fileNotFound` one is still checked by a header read. A
  restore under a new name (` (restored)`) or at the top is a PATCH, as it rewrites content
  or `groupId`.

Upload instructions **[verified]** `UploadInstructionSet.cs`, `UploadManifest.cs`:
`{transferIv, storageOptions?, transitOptions?, manifest: {payloadDescriptors: [{payloadKey,
contentType, iv, hash?}]}}`. `transferIv` is required and must not be all zero. Omit
`storageOptions` rather than sending null (null → 500). Unencrypted files need null or zero
payload IVs. Payload keys match `^[a-z0-9_]{8,10}$`.

The `metadata` part is the raw AES-CBC ciphertext (not an envelope) of
`{encryptedKeyHeader, fileMetadata}` under the shared secret with `transferIv`
(`FileSystemStreamWriterBase.cs:104,366`). The key header must be present even when the file
is unencrypted (4109 otherwise); chat-kmp encrypts an all-zero `KeyHeader` with the shared
secret. `fileMetadata`: `{isEncrypted, allowDistribution, accessControlList, appData:
{uniqueId, groupId, fileType, dataType, content, ...}, versionTag}`.

Encrypted files **[verified]** on odin-core `main` @ 07657309e (the format: `encryption.md`):
- **Upload:** with `isEncrypted: true`, the server opens `encryptedKeyHeader` (`iv || key`,
  AES-CBC under the shared secret) and stores it re-encrypted with the drive storage key
  (`FileSystemStreamWriterBase.cs:455-458`, `DriveStorageServiceBase.cs:688-696`). That key
  needs a drive grant with Read (`ExchangeGrantService.cs:187-214`). Every payload of an
  encrypted file needs a strong 16-byte `iv`; an unencrypted file's payloads need none
  (`FileSystemUpdateWriterBase.cs:467-477`). "Strong": not every group of four bytes the same
  (`ByteArrayUtil.IsStrongKey`).
- **Update (PATCH):** the key header must still be strong in both IV and key
  (`FileSystemUpdateWriterBase.cs:504-512`), but the server keeps the file's key and only
  takes the IV as the new header IV (`DriveStorageServiceBase.cs:2297-2309`). So an update
  sends the file's own key with a new IV, and `content` encrypted with those. `isEncrypted`
  can't change on an update (`:2274-2278`). An overwritten payload needs a new IV
  (`PayloadHash.AssertIvsRotatedAsync`).
- **Download:** a header carries `sharedSecretEncryptedKeyHeader` (the file key and header IV,
  under the shared secret, `DriveFileUtility.cs:44-51`). Payload responses on both payload
  routes, ranged included, carry `PayloadEncrypted: True|False` and, when encrypted,
  `SharedSecretEncryptedHeader64`: the key with that payload's IV, as IV (16) + encrypted key
  (48) + version (4, little-endian), base64 (`DriveStorageControllerBase.cs:138-146`,
  `EncryptedKeyHeader.ToBase64`). The bytes are the stored ciphertext, ranges included. A
  ranged response's `Content-Range` gives the stored size (`DriveStorageControllerBase.cs:171-176`
  before PR #1921; `OdinControllerBase.PayloadResult` since).
- **Hashes:** `storedHash` is over the ciphertext and checked while it streams in;
  `contentHash` is the plaintext hash encrypted with the file key under the payload IV XOR
  `0xAA…` (48 bytes), and the server checks only its length (`PayloadHash.cs`).
- **Deferred hash** **[verified]** (odin-core PR #1936, #1935; `docs/payload-hashes.md`,
  "Deferred hash"): the descriptor's `hash` carries the algorithm and a sentinel for both values
  (every byte `0x11`: 32 bytes, 48 for the sealed `contentHash`); a part `payloadHash` after the
  payload, file name the payload key, carries `{"storedHash", "contentHash"}`. Values that don't
  match the bytes give 4175 and nothing is stored; no such part gives 4178. Stored headers hold
  the real values. This client always defers: it reads and encrypts the source once. It sends
  an exact `Content-Length`, so it can't leave the part out: a source that changed while it was
  sent gets values no bytes hash to (4175); one that ended early aborts the request.

**Update replaces the metadata, it doesn't merge** **[verified]**
`StandardFileUpdateWriter.cs:48-85`: an update that leaves out `uniqueId` clears it, and a
missing `accessControlList` resets to owner. Every update resends the whole `appData`.

Limits **[verified]**: `content` at most 10,240 UTF-16 code units (4167); file types in
`2100000000..` are reserved (4120); one upload payload at most 4 GiB (`uint` byte count); out
of quota → 507 with `Retry-After`. App callers get `serverMetadata: null` in headers.

## Change feed
**[verified]** `QueryBatch.cs`, `KMP/sync/DriveSync.kt:173-295`:
- `resultOptionsRequest`: `sorting: "anyChangeDate"`, `ordering: "oldestFirst"`,
  `resultShape` (below; `includeMetadataHeader: true` before odin-core #1967), `maxRecords`,
  `cursorState` (opaque string, persist after each page).
- `queryParams.fileState` null → tombstones included.
- Only rows with modified < now are returned; with `oldestFirst` the cursor resumes after the
  last change. Page until `hasMoreRows == false`.
- **The feed leaves out rows the server has** **[verified]** (commit 70c9d15bc): a row
  modified within the last second (`QueryBatch.HoldBackWindowMs`, #1923: a page read to the
  end may lack a row written that moment, which the next read brings), a file whose TTL came
  due (`DriveQueryServiceBase.FilterVisibleAsync`, as by id), and one the caller may not read
  (its ACL, ibid.). The Filesync app never meets the last: an app token's caller is the owner
  (`AppRegistrationService`, `securityLevel: Owner`), and `DriveAclAuthorizationService.
  CallerHasPermission` admits the owner to every file. A header read for that app is 404 only
  for a file that isn't there (deleted for good, or a TTL due; our nodes have none): an ACL
  denial throws (`GetServerFileHeaderInternal`, `AssertCallerHasPermission`), not a 404. So
  a re-read takes a file it had as gone only on a 404 for its header.
- **What is in some folders**, any app's files too: `queryParams` `{groupId: [folders...],
  fileState: ["active"]}`, minimal rows, paged while `hasMoreRows` (a page may be empty with
  more to come) (`HomebaseRemote::holding_live`); `groupId`, `fileState` and
  `archivalStatus` are filters of the query **[verified]** (`QueryBatch.cs:130-142, 351`,
  `FileQueryParams.cs`). Asked before folders are deleted for good, a pass of them in one
  question: a live file in one that this client doesn't read (not in its mirror) keeps it.
- `sorting` must be sent: the default is creation order. `maxRecords: 0` → 500.
- `cursorState` is a JSON string (`{paging:{time,row}}`), not base64. An empty page returns the
  input cursor. **An unparsable cursor silently restarts from the beginning**
  (`QueryBatchCursor.cs:135-150`).
- Every header write sets `modified = max(modified + 1, now)`, so a change never lands behind
  the cursor.
- Tombstones: `fileState: "deleted"`, `uniqueId: null`, `content: ""`, `payloads: null`;
  `fileId`, `groupId`, `fileType` kept.
- **Slim pages** (odin-core #1967, **[verified]** in its branch: `QueryBatchResultShape.cs`,
  `QueryBatchFileRef.cs`, `DriveQueryServiceBase.CreateBatchResultAsync`):
  `resultOptionsRequest.resultShape`: `"full"`, `"noContent"` or `"minimal"` (unknown → 400
  InvalidQuery; without it `includeMetadataHeader` decides as before, and a server without
  #1967 ignores it). A minimal page has `fileRefs` instead of `searchResults`: per row
  `fileId`, `fileState` (`"active"`/`"deleted"`), `fileType`, `dataType`, `archivalStatus`,
  `versionTag`, `created`, `updated` and `fileByteCount`, plus `globalTransitId`, `uniqueId`,
  `groupId`, `localVersionTag`, `userDate` and `senderOdinId` when set. The rows, cursor and
  `hasMoreRows` are those of the same query in any shape: they come from the query before
  the rows are filtered for the caller, and every shape filters alike. `versionTag` changes
  only when the header is written; `updated` also on reactions and transfer history. Measured
  against frodo (2,000 files, pages of 500, bytes received over TLS): a full row 2,510 bytes,
  a minimal one 588 (the encrypted envelope and its base64 included).
- How the client reads with it: `hbsync_engine::feed::pull`. A server without `fileRefs` in
  its answer is refused, not read as an empty page.
- **A kept answer could hide a write** (odin-core #1909, fixed by #1973, merged as
  4eb50e5db; filesync #90): `QueryBatchCached` keeps a query-batch answer under a key made
  of the query's parameters (drive, `maxRecords`, cursor, sorting, filters; not the caller,
  so every client of the drive shares it) for 2 h (`AbstractTableCaching.DefaultTtl`), and a
  write clears the drive's answers after its commit (`TransactionalCache.InvalidateAsync`, a
  post-commit action) **[verified]**. Before the fix, an answer computed while a write
  committed was stored after the clearing and kept hiding the write from the same query
  (FusionCache dates an entry by when it is stored). Tried on frodo at fe067124b: a thread
  reading the feed from a cursor at the end again and again while client b created a
  folder, then one read with the same query 1.1 s after b's answer: 70 of 70 rounds the
  folder wasn't there (`hasMoreRows` false), still so 11 s later; with `maxRecords` 99 it
  was there every time. Every caught-up client holds the same cursor, so one device's read
  could hide another device's write from all of them. The fix counts the cache's removes,
  and a fill that saw the count move doesn't store its answer **[verified]**
  (`RemovalCounter.cs`, `FusionCacheWrapper.cs`); tried on frodo at 2f0ff3592: 0 of 20
  rounds hid the write. Not covered by it **[verified]** in its PR: FusionCache's own tag
  expiry across two hosts on one Redis, when the removing host's clock lags the storing
  host's. The client relies on the fix: every read asks for pages of `FEED_PAGE` (500), and
  an announced change is read for once. Until the fix it varied the page size per read and
  read for an announced change again for up to six reads (removed with filesync #90). The
  live checks `a_page_read_during_a_write_may_be_kept_stale` and
  `another_clients_write_is_in_the_feed_at_once` (`hbsync-dev/tests/live.rs`) fail if the
  server hides a write again.
- **Rows held back**: a row written in the server's last millisecond or so is held back,
  answered as an empty or short page with `hasMoreRows` true (odin-core #1906,
  `QueryBatch.HoldBackWindowMs`, 1000 ms, **[verified]**); tried: 3 of 60 reads right after
  another client's write, for 9 to 12 ms. `feed::pull` reads on while `hasMoreRows`, so it
  costs a read, and the fake models it. A row further ahead than the window is left out
  without more rows (#1923; **[inferred]** only when clocks disagree).
- **A write is announced after it is visible** **[verified]**: `DriveStorageServiceBase`
  publishes the drive notification after the write's transaction is disposed, and disposing
  it commits and then awaits the post-commit actions, the cache clearing among them
  (`ScopedConnectionFactory.DisposeAsync`). So a read the announcement starts finds the
  row, or a page that says more rows follow.

## Errors
**[verified]** `ExceptionHandlingMiddleware.cs`: plaintext `application/problem+json` (never
enveloped) `{title, status, correlationId, errorCode}`. `errorCode` is the camelCase enum name
(`versionTagMismatch`); accept numbers too. `OdinClientException` → 400,
`OdinSecurityException` → 403, a bad or missing token → 401 with an empty body. A `NotFound()`
on an enveloped route comes back as 404 with an encrypted empty body: check the status first.

| Code | Name |
|---|---|
| 4002 | `cannotUpdateNonActiveFile` |
| 4105 | `existingFileWithUniqueId` |
| 4106 | `fileNotFound` |
| 4159 / 4160 | `missingVersionTag` / `versionTagMismatch` |
| 4175 / 4176 / 4177 / 4178 | `payloadHashMismatch` / `invalidPayloadHash` / `payloadHashRequired` / `payloadHashMissing` |

## Notifications (WebSocket)
**[verified]** `V2NotificationSocketController.cs`, `AppNotificationHandler.cs`,
`KMP/client/websockets/OdinWebSocketClient.kt`:
- `wss://{identity}/api/v2/notify/ws-token`, app tokens only. Auth in one header:
  `Sec-WebSocket-Protocol: odin.notify.v1, odin.bearer.<base64url CAT, no padding>`; the server
  answers with `odin.notify.v1`.
- Client frames: envelopes of `{command, data}` where `data` is a JSON string. Subscribe with
  `establishConnectionRequest` and `{"drives":[{"alias","type"}]}` (Read on the drive
  needed); the server answers `deviceHandshakeSuccess`. `ping` → `pong`.
- Server frames: `{isEncrypted, payload}`, `payload` an envelope string of
  `{notificationType, data}`. `fileAdded`, `fileModified`, `fileDeleted` carry the header
  unless the subscription asks for the minimal shape (below), which this client does. Reaction
  updates also send `fileModified`. A frame whose `data` isn't what the client expects still
  counts by its `notificationType`: a file event then reads the feed.
- `resultShape: "minimal"` in the subscription (odin-core #1970, not released yet;
  **[verified]** `EstablishConnectionOptions`, `ClientDriveNotification.For` at 9087c252c, and
  live at 5cbd1d363): every drive event, the subscriber's own writes included, carries
  `targetDrive` and whichever of `fileRef` (the file now) and `previousFileRef` (as it was, on
  a delete) it has: the rows of a minimal `query-batch`, no header keys. A hard delete has only
  `previousFileRef`. The client takes them as slim feed rows (`daemon.md`, the sync loop).
  Measured once on frodo at 5cbd1d363 (2,000 files, text frames as received): an add or update
  event 4,514 B full → 1,355–1,376 B minimal, a delete 7,438 → 2,217 B.
- `excludeOwnWrites: true` in the subscription (odin-core #1971, not merged; **[verified]** on
  its branch at 50923aa02: `EstablishConnectionOptions.ApplyTo`, `AppNotificationDispatcher`,
  `OdinContext.RequestTokenId`; **[tried]** on frodo): no drive events for writes made by a
  request with the socket's own token, the client registration's (`AccessRegistrationId`):
  creates, updates, deletes and hard deletes, and what the same request changed on the side.
  Still sent: writes with another token (another computer's registration), peer files and
  read receipts arriving, the outbox's transfer-history updates, reaction previews, and any
  write the server can't attribute to a request (jobs, system work: `OriginTokenId` null).
  Sockets sharing one token (a copied credentials file) miss each other's writes. A peer
  socket asking for it is refused (`InvalidQuery`). A server without #1971 ignores the key
  **[inferred]** (an unknown JSON property): the echoes come, and are taken as known versions.
  Measured on frodo at 50923aa02 (`own_write_echoes` in `hbsync-dev/tests/live.rs`: one folder
  synced as the daemon syncs it, 2,000 files created, deleted to Recently deleted, deleted for
  good): without the option 2,001 frames a phase, 1,355 / 1,377 / 2,217 B each (2.7, 2.8 and
  4.4 MB), and the process received 5.4, 5.3 and 5.7 MB in all; with it no drive event at all
  (one pong), and 2.6, 2.5 and 1.2 MB in all. The feed reads are the same either way (8 slim
  pages for each of the first two phases, 1 for the last, none whole): the echoes were taken
  as known versions and never needed the feed. Again at fe067124b, with each write's time
  taken from its answer and no feed read after own writes: no feed page at all in any phase,
  and with the option 1.46, 1.34 and 1.28 MB received in all (the echoes alone would add 2.7,
  2.8 and 4.4 MB). The rows of the own writes stay in the feed, each file's last only, and are
  read slim at the next read the client makes anyway.
- **Write answers carry the write's time** (odin-core #1971; **[verified at fe067124b]**
  `CreateFileResult.Updated`, `UpdateFileResult.Updated`, `DeleteFileResultV2.Updated`,
  `UpdateArchivalStatusResultV2.Updated`; `UnixTimeUtc` serializes as Unix ms). It is the
  file's modified time after the request's own last change to it, not a header read after the
  write: every drive event published with the request's context records the file's `updated`
  (`RequestOwnWrites.Record`, from `TryPublishAsync`), and the answer takes the later of that
  and the time its own header write returned (`UpdatedAfterOwnWrites`); a soft delete's is
  the tombstone's final time (clearing the reaction summary moves it once more, and the
  tombstone and its `fileDeleted` event carry that). Changes made within the request on
  others' behalf (inbox items, synced posts, reaction summaries) and later work aren't in it.
  (At fcfc3b422 the answer read the header after the whole write instead.) Create and update
  answer `updated` next to `fileId` and `newVersionTag` (null only for a peer-only update);
  a delete (single, batch, by group) the tombstone's time, null when `localFileNotFound`; the
  archival-status batch and `soft-delete-batch` (#1977) per result, null with an `errorCode`.
  Hard-delete-batch answers none.
  It is the time the feed shows for that write (odin-core `WriteResponseUpdatedTests`;
  **[tried]** on frodo at fe067124b for a create, a new version, a removal and a restore by
  PATCH, the archival-status batch and a delete: `write_answers_carry_the_feeds_time`). The
  client records a version's and a removal's time from it (`drive::HomebaseRemote`'s
  `Target::changed_at`) and reads no feed after its writes. A soft delete's gives the
  tombstone's time, which the client records as the time the file went; the tombstone's
  version still comes with the feed's row (`soft-delete-batch`, above). Before #1971 the answers had no time (`payloads[].lastModified` is
  the payload's, not the header's `updated`).

## Conventions
- camelCase JSON, enums as camelCase strings, bytes as standard base64, times as Unix ms.
- Errors are RFC 7807 ProblemDetails with `errorCode` as a number **or** a camelCase string.
- Payload hashes: `../odin-core/docs/payload-hashes.md` (errors 4175–4178).

## Known gaps
- **No file-sync drive kind** **[verified]**: the drive flag `RequirePayloadHashes` (#1899)
  can't be requested at registration (no field in `d`), the owner app drops a requested
  drive's attributes when it creates it, and no owner UI sets the flag or a kind. App clients
  can't read the flag either (`ClientDriveData` lacks it). To do in odin-core / odin-js.
- **Circle permission keys lost** **[verified]**: odin-js sends `cp`, but the server's
  `YouAuthAppParameters` has no `cp`, so they vanish on the redirect to `/owner/appreg`.
- Delete takes an optional `versionTag` since odin-core PR #1931 (#1903, v0.1.960).
  `delete-batch/by-file-id` takes a `versionTag` per request and checks them all before
  deleting any (**[verified]** `DeleteFileIdBatchInternal`): one stale tag fails its whole batch,
  and its 4160 names the tag, not the file (**[verified]**, `AssertVersionTagMatches`). The client
  uses neither: `soft-delete-batch` (#1977, above) answers per file.
- Payload reads were buffered whole in server memory (#1892) and ranges were `int` (#1893)
  until odin-core PR #1921 (merged 2026-10-11). The client is written for #1921: a read is one streamed
  request, whatever its size. Uploads over 4 GiB still fail (the `uint` byte count, the half of
  #1893 the PR leaves open), so the client refuses payloads that large.
- App clients can't read the drive's `requirePayloadHashes` **[verified]** (`ClientDriveData`
  has no such field); the client always sends hashes.
- **The feed could hide a write until the next one** (fixed in odin-core #1906, 2026-10-09)
  **[verified, tried]**: `QueryBatchCached` cached query-batch results until the next write
  to the drive, and an `anyChangeDate` result depends on the clock (`modified < now`). A
  query in the same millisecond as a write was cached without that row, and identical
  queries missed it until the next write. It took a query landing within the write's
  millisecond, so it showed on a local dev host (about one write in twelve on frodo, polling
  right after the WebSocket wake-up) more than over a real network. Since the fix, such an
  answer isn't cached; an oldest-first page can come back short or empty with
  `hasMoreRows: true`, and the next query returns the held-back rows. The contract scenario
  `every_write_shows_up_promptly` passes without any client workaround (odin-core #1905,
  filesync #1).
- **A kept answer could hide a write for hours** (fixed by odin-core #1973, merged as
  4eb50e5db, 2026-10-17) **[verified, tried]**: an answer computed while a write committed
  outlived the write's cache clearing (odin-core #1909, filesync #90; the change feed,
  above). The client's workaround (page sizes varied per read, announced changes read for
  again) is gone.
