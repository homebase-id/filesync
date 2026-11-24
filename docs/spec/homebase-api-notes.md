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
  File routes still take the alias. It carries `created` too (Unix ms, when the drive was
  made: the `Drives` row's `created`, **[verified]** `TableDrivesCRUD`), from odin-core v0.1.973
  (PR #2042, #2038; **[verified]** live on frodo). Compare it for a change, not for order: two hosts
  that made versions of one drive (after an identity move) need not agree on the time.
- **A private (zero-knowledge) drive** (odin-core v0.1.973, `docs/zero-knowledge-drive-api-plan.md`
  step 3; **[verified]** in code, not yet tried on a converted Filesync drive): the authorize
  request's `zk_app_key=1` gets the token answer a `zkAppKey` (`ZkSeal` v1 to the request's
  `public_key`); the security context's drive grant carries `zkAppKeyEncryptedStorageKey`
  (`RedactedDriveGrant`); key headers are `zkDrive` (13) under the drive's key; payload
  responses carry no `SharedSecretEncryptedHeader64`. A device not handed the key sees none of
  the drive's encrypted files (`ZkDriveFiles.VisibleTo`: left out of queries, refused by id) and
  can't write (`driveIsZeroKnowledge`, 9012). Converting keeps `created` (true-zero, from code).
  A drive request's `sm` (`collaborative`, `private`, `selectiveShare`; absent: collaborative)
  makes a new drive so; the app drive answer carries `sharingModel`, and the security context's
  drive grant `isZeroKnowledge`, set whether the app's copy of the key is there or not (odin-core
  #2046, odin-js #1013, step 4; **[verified]** in code, in production from odin-core v0.1.974).
  The app's locked copy can lag behind: private with no copy means sign in again. Up to
  v0.1.974 the security context lists **no** drive grants for an app that asks for no
  permission keys, as a Filesync install from before it asked for 305
  (`PermissionGroup.Redacted` returns an empty list when the group's `PermissionSet` is null;
  access checks use the real grants, so the drive answers; **[verified]** live on frodo, cause
  from true-zero): the client reads a missing grant as a
  drive whose privacy the drive's answer (`sharingModel`) tells (`encryption.md`); fixed in
  odin-core #2047 (tracking: filesync #335).
  How this client uses it: `encryption.md`.
- **A drive made again** **[verified]**: its id is the alias the app asks for
  (`DriveManager.CreateDriveAsync`), fixed in `app.rs`, so a drive deleted and made again has the
  same alias and slugs. Of what the client sees, only `created` differs (its storage key does
  too, which the client never sees). The client keeps it per account DB and sets
  a DB that knows another aside (`daemon.md`).
- **Registration flow** **[verified, tried]**: an unregistered app is sent to
  `/owner/appreg`, which creates the requested drives and registers the app, then returns to
  the authorize URL (`YouAuthUnifiedController.cs:155-162` sets `return`), so the flow resumes
  by itself. An app cannot create drives itself.
- **A registered app without its drive** **[verified]** (2026-10-27): logging in to an app that
  is registered goes straight to consent, which creates nothing, so a drive deleted since stays
  missing; finding it by slug answers 400 `invalidDrive` ("No drive 'filesync' on app
  'filesync'", the same for an unknown app). The owner app's `/owner/appupdate` (odin-js
  `ExtendAppPermissions.tsx`, params `appId`, `d`, `cd`, `return`) creates a requested drive
  that is missing and not another app's well-known one, owned by the app asking
  (`ensureAppOwnedDrives`); one that exists is only granted (`ensureDrive`). Cancelled, it
  returns with `status=canceled`. The same alias and type come back, so nothing tells the new,
  empty drive from the old one: setup asks for it only on a computer that never synced with
  the identity (`setup.rs`, `give_drive_back`).
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
  Read, React and Comment on its drive (13) and starts empty; for that the app asks for all
  four on the drive (15): a circle is given no more than the app holds
  (`AssertRegistrationMayGrantDrivesAsync` at install, where the owner console has made the
  drive, owned by the app, just before; `AssertAppMayGrantDrivesAsync` for `circles/create`,
  **[verified]**). The chat app's Drive asks for the drive and declares the circle the same
  way (it asks for no permission key at install, this client for 305), and shares only at an
  access the circle grants. An install made by this client before 2026-11-20 holds Read and
  Write and has a circle with Read (or none, from before circles were declared: this
  client's `circles/create` at setup is refused then, as the app holds less than the circle
  is to give, said in the log). Neither changes at a sign-in. The drive's grant is the
  owner's to widen, on `/owner/appupdate` with `d` (the page replaces the app's grant on that
  drive by the one asked for, **[verified]** in odin-js `ExtendAppPermissions.tsx`) or on the
  app's page in the console. The circle's is the owner's too, in the console; or the app,
  once it holds the access, deletes the circle and makes it again (`circles/delete`: an app
  may delete a circle it owns, refused while it has members unless asked with
  `removeMembers`, **[verified]**), under a new id: what was kept by the old one (a circle's
  sealed key, share rows) no longer fits.
- **Permission keys** **[verified]**: registration asks for them in `p`, numbers joined by
  commas (odin-js `permissionParamToPermissionSet`); an installed app gets no more at a sign-in
  (`YouAuthUnifiedService.AppNeedsRegistration` is false once registered), only on
  `/owner/appupdate` with `p`, which adds them to the ones it has, a key it holds a second time
  too, shown to the owner as more access (`ExtendAppPermissions.tsx`): ask only for what it
  lacks. Device tokens gain them without signing in again (`AppRegistrationService`, from its
  code).
  The security context lists them per permission group (`permissionContext.permissionGroups[]
  .permissionSet.keys`). Filesync asks for `UseTransitRead` (305) since shared-folder sync, for
  the identity's `/api/v2/peer/...` routes; an install from before lacks it until the owner
  approves the update. Any transit key also gives the app the identity's connection key (ICR
  key) and ReadWrite on the transient temp drive (`AppRegistrationService.GrantInputsAsync`,
  `HasRequestedTransit`). Holding a key also lists the app's drive grants on a server up to
  v0.1.974 (#2047: the group's permission set is no longer null; **[inferred]** from code).
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
written to (a file deleted for good answers not found, which forgets the row once its header
is not found twice, a minute apart: sync semantics §8b), or until the
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
- Answer `{results: [{fileId, errorCode, updated, versionUpdated, recipientStatus}]}`, one per file in request
  order: `errorCode` null when deleted, `versionTagMismatch` (written since: left as it is),
  `fileNotFound` (missing, or hidden by the ACL), or another client error; one file's error
  never stops the others, a server error fails the request. `versionUpdated` is the
  tombstone's time as its header shows it (Unix ms; a tombstone already answers its own),
  null when it stayed; the client reads that one ("A version's own time", below).
  The answer carries no `versionTag`: the tombstone's comes with its row in the feed.
- The client deletes softly through it (`HomebaseRemote::delete`: emptying Recently deleted,
  and the purge's younger nodes), a request per 250 files, so a stop waits less (a request of
  1,000 took ~70 s on the user's Homebase). It records each file gone at once,
  since `versionUpdated` (`gone_files.gone_at`, which the 30 days of keeping its id count from), and
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
- Answer `{results: [{fileId, newVersionTag, updated, versionUpdated, errorCode}]}`, one per
  file in request order; both times as Unix ms (**[tried]** live,
  `write_answers_carry_the_versions_time`); the client reads `versionUpdated`. One
  file's error never stops the others.
- The client removes and restores through it (`HomebaseRemote::set_removed`): a request per
  level of a folder's tree and per 1,000 files, a folder's level before what is in it. A
  `cannotUpdateNonActiveFile` answer is a tombstone (a file is active or deleted:
  `AssertActiveForUpdate`, **[verified]**): the file is taken as deleted for good without
  reading its header; a `fileNotFound` one is still checked by a header read. A
  restore under a new name (` (restored)`) is a PATCH, as it rewrites content; one whose
  folder went for good is batched like any other (its `groupId` as it was: an orphan, for
  Recovered files).

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
`2100000000..` are reserved (4120); out of quota → 507 with `Retry-After`. No payload size
limit since odin-core PR #1932 (#1893, v0.1.960) **[verified]**: the byte count is 64-bit
through the write chain (before it, `uint`: an upload over 4 GiB failed), Kestrel has
`MaxRequestBodySize = null`, and the quota guard looks only at the identity's status. Uploads
stage on disk, then go to long-term storage: on disk a copy, on S3 `TransferUtility` in 16 MB
parts, so not bound by S3's 5 GB single PUT (**[verified]** `S3AwsStorage.UploadFileAsync`).
The server answers once that is done: 45 s after the body for 4 GiB on frodo (**[verified,
tried]**: a file of 4 GiB + 1 MiB went up and down, `live_big`). The client caps one file at
50 GiB itself (`MAX_FILE`: no resumable upload), and waits for an upload's answer a second
longer per 8 MiB: S3 takes the parts one at a time, 60 MB in 1.4-2.7 s by odin-core's own
comment (**[verified]** `UploadConcurrencyLimit`). App callers get `serverMetadata: null` in headers.

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
  a re-read takes a file it had as gone only on a 404 for its header, and that only once it is
  answered again a minute on (sync semantics §8b).
- **What is in some folders**, any app's files too: `queryParams` `{groupId: [folders...],
  fileState: ["active"]}`, minimal rows, paged while `hasMoreRows` (a page may be empty with
  more to come) (`HomebaseRemote::holding`); `groupId`, `fileState` and
  `archivalStatus` are filters of the query **[verified]** (`QueryBatch.cs:130-142, 351`,
  `FileQueryParams.cs`). Asked before folders are deleted for good, a pass of them in one
  question: any active file in one (live or removed) keeps it, one this client doesn't read
  or doesn't know of yet (not in its DB) too, but the rows still to go in later passes, by
  `fileId` and `versionTag`.
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
  `groupId`, `localVersionTag`, `userDate`, `senderOdinId` and `versionUpdated` (#2143, V2
  only) when set. The rows, cursor and
  `hasMoreRows` are those of the same query in any shape: they come from the query before
  the rows are filtered for the caller, and every shape filters alike. `versionTag` changes
  only when the header is written; `updated` also on reactions and transfer history. Measured
  against frodo (2,000 files, pages of 500, bytes received over TLS): a full row 2,510 bytes,
  a minimal one 588 (the encrypted envelope and its base64 included). 2026-10-19, 100,000
  tombstones: a full row 1.84 KB, a minimal one 0.52 KB; pages of 2,000 minimal rows work
  (measured; no cap on `maxRecords` in query-batch found in odin-core, inferred).
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
  host's. The client relies on the fix: a read starts with a page of 500 rows (`FEED_PAGES`;
  slim pages grow to 2,000 in a run of them, and a slim page's rows before its first new one
  are asked for by their count), and an announced change is read for once. Until the fix it
  varied the page size per read and read for an announced change again for up to six reads
  (removed with filesync #90). The live checks `a_page_read_during_a_write_may_be_kept_stale` and
  `another_clients_write_is_in_the_feed_at_once` (`hbsync-dev/tests/live.rs`) fail if the
  server hides a write again.
- **Only a change order is held back** **[verified]** (`QueryBatch.cs`): `anyChangeDate` and
  `onlyModifiedDate`; `createdDate` and `fileId` hold nothing back, in an order a write doesn't
  change, but a file made while the pages are read may be missed by them (the cursor is
  `created` and the row, and a row may commit after a later one; **[inferred]** for Postgres).
  A held-back page may come with the cursor it was asked from and `hasMoreRows` true.
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
(`versionTagMismatch`); accept numbers too. An error that names none carries `noErrorCode`
(0, the enum's default: every `OdinSecurityException` that sets none), which the client reads
as no code (**[verified]** live, odin-core efc317ad4). A 403 for want of a permission key
says so: `errorCode: "missingPermissionKey"` (1006) with `permissionKeys`, the keys any one of
which would have done (`[305]` on the token route, `[210]` on a send and on the drive's
public key, `[210, 305]` where a drive addressed by slug is resolved and the app holds
neither; odin-core PR #2164, **[verified]** live at b79aefa99 for the token route and, through
the client, a send; the rest in `PermissionContext`). It comes on this identity's own routes
too (a new file for another identity is uploaded to one), where a 403 is otherwise this
login's: the client takes one with this code as the app's want of the key, never as the
login refused. A 403 for a missing drive grant has no code still.
`OdinClientException` → 400,
`OdinSecurityException` → 403, a bad or missing token → 401 with an empty body. A `NotFound()`
on an enveloped route comes back as 404 with an encrypted body (`X-SSE: 1`): check the status first.

**Retry-After** **[verified]** in odin-core's code: an `OdinRetryLaterException` answers with
the status its service chose (507 out of quota) and `Retry-After` in whole seconds
(`ExceptionHandlingMiddleware`); a paused identity answers 503 with `Retry-After: 600`
(`MultiTenantContainerMiddleware`, `TenantStatusRules.PausedRetryAfterSeconds`). The client reads
the seconds form only.

**Who answered** **[verified]**: every answer of odin-core carries `X-Odin-Version` (below), a
success too, so one without it (a captive portal's login page, a proxy's) isn't the identity's:
the client takes it for the network not being there yet, as it does a redirect it was given.
That holds for a 401 or 403 too (an API call's, the notification socket's upgrade): only the
identity's own refuses the login (a bad or missing token answers 401 after the header is set,
verified in odin-core's code). Inferred, not verified: the hosting in front of an identity (a
load balancer) answers no 401 or 403 of its own; one that did would show offline, not refused.

**Who sent a 404** **[verified]** in odin-core's code: `OdinVersionNumberMiddleware` sets
`X-Odin-Version` on every answer, first after logging (`Startup.Configure`; odin-core tells a
peer that is an Odin server by it, `PeerDriveQueryService`). A route odin-core matched answers a
missing file sealed (`X-SSE: 1`, the header read: `SharedSecretEncryptionMiddleware` seals only
a matched endpoint's answer) or, unsealed, with ProblemDetails of the same `status` (the payload
read: `NotFound()`, or 410 for a payload whose bytes are missing, through `[ApiController]`'s
client-error mapping, which odin-core leaves at its default; an exception through
`ExceptionHandlingMiddleware`). A path with no endpoint answers 404 with the header and no body;
a proxy's or load balancer's own 404 has no header. The client takes a 404 or 410 (and
`fileNotFound`, and `cannotUpdateNonActiveFile` as "a tombstone") as "not there" only with the
header and one of the two; anything else is passing (`ApiError::read`). Even then a file not
there is gone for good only once it is not there again a minute on (sync semantics §8b). Live: frodo's header and payload reads of a missing file are both "not there"
(`hbsync-dev` live test `a_missing_file_is_not_found`).

**A deleted file's payload or thumbnail** (odin-core PR #2141, merged 2026-11-14 as aa560aea9,
after v0.1.991; **[verified]** there: `DriveStorageServiceBase.AssertNotDeletedForPayloads`,
`docs/file-access.md`) is a 404 with `errorCode: fileDeleted` (4181): on the owner and app
routes by file id and by global transit id (tested on the owner's V2 routes; the V1, app and
guest routes go through the same lookup), from the sharer's server with a member's peer token,
and through the member's own server, which passes the code on. By unique id a deleted file is
a plain 404 (a tombstone keeps no unique id), a missing file a plain 404, and one the caller may
not read a 403. A file deleted between the lookup and the read of its bytes is a plain 404
too (**[inferred]** from the PR's description). The adapters read a 404 with `fileDeleted` as
`RemoteError::Deleted`, and `Remote::read` of a tombstone answers that for the owner and a
member alike (the contract's `a_removed_node_deleted_for_good`): a tombstone is proof on its
own, so the engine doesn't read the header to make sure, as it does after a "not there"
(`gone::gone_after`). Both adapters read a private drive's header before its payload, for
the key, and a tombstone's header says the same; a server without the PR answers so too.

| Code | Name |
|---|---|
| 4002 | `cannotUpdateNonActiveFile` |
| 4105 | `existingFileWithUniqueId` |
| 4106 | `fileNotFound` |
| 4159 / 4160 | `missingVersionTag` / `versionTagMismatch` |
| 4175 / 4176 / 4177 / 4178 | `payloadHashMismatch` / `invalidPayloadHash` / `payloadHashRequired` / `payloadHashMissing` |
| 4181 | `fileDeleted` (odin-core PR #2141) |

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
  PATCH, the archival-status batch and a delete). Since #2143 each of these answers carries
  `versionUpdated` beside it ("A version's own time", below), and the client records a
  version's and a removal's time from that one (`drive::HomebaseRemote`'s
  `Written::changed_at`; `write_answers_carry_the_versions_time`); it reads no feed after its
  writes. A soft delete's gives the
  tombstone's time, which the client records as the time the file went; the tombstone's
  version still comes with the feed's row (`soft-delete-batch`, above). Before #1971 the answers had no time (`payloads[].lastModified` is
  the payload's, not the header's `updated`).

## Shared with you
What reading another identity's share (synced into `~/Homebase/Shared/<them>`) uses. The owner's
own server answers the peer routes, which need `UseTransitRead` ("Permission keys"); the sharer's
server answers the rest, to a token their server made for this identity's connection.
- **A token for their server** **[verified]** (`V2PeerNotificationController`,
  `PeerAppNotificationService`, `CircleNetworkService.CreatePeerIcrClientForCallerAsync`): `POST
  /api/v2/peer/notify/token` `{"identity": "<sharer>"}` (needs `UseTransitRead`) answers
  `{authenticationToken64, sharedSecret}`. Each call adds a client registration on their
  server. It lives 30 days from its last use and goes with the connection it was made under;
  one made under an earlier connection with the same identity is refused (odin-core #2132,
  **[verified]** at 625085b90: `PeerIcrClient.TimeToLiveSeconds` and `ConnectionCreated`,
  `CircleNetworkStorage.GetPeerIcrClientAsync`, `PeerAppNotificationService`; before it a
  token lived 20 years and was never removed). So keep it, and ask again only once their
  server refuses it (401: unknown, run out, or the connection gone or blocked): a device
  that was off for more than 30 days asks at its first read. **[tried]** on the dev server:
  one token read a share every five minutes for 24 hours (2026-11-20), and a token from
  before #2132 was answered 401 and replaced with one request (2026-11-21). The own server retries the call to theirs, and an answer lost on the way may still
  have left a row: don't retry blindly. Refusals: not connected here, 400
  `notAConnectedIdentity` (this client then waits an hour, `RemoteError::Waiting`, saying
  so: no failure, and no token asked of theirs); their server unreachable, 400 with no code (503 from it:
  `remoteServerReturnedUnavailable`); their 400, 400 `invalidGlobalTransitId`; their 500, 400
  `remoteServerReturnedInternalServerError`; their 403 (they disconnected or blocked this
  identity), 403 with no code.
- **Their circle's key** **[verified]** (`PeerCircleShareKeyService`, `AssertCanUsePeer(Read)`:
  `UseTransitRead` only): `GET /api/v2/peer/<sharer>/circles/<circleId>/share-key` answers
  `{sealedKey}`, sealed to this identity's drive (`encryption.md`, "Files shared with you");
  `null` until a device of theirs sealed it (it does when next online); 403
  `notReviewedByOwner` until they reviewed the connection; another 403 (no code): not a member,
  the circle off, or they no longer count this identity as connected. Their server down (or a
  socket error, a timeout): 400 `remoteServerReturnedUnavailable` (read as a passing outage);
  their 400, 400 `argumentError`; a server too old for the route, 400
  `remoteServerReturnedNotFound`; anything else, 400 `remoteServerReturnedInternalServerError`.
- **Whose a refusal is.** Without the permission they need, the peer routes answer 403
  `missingPermissionKey` (Errors; the token route's title is "Forbidden: Does not have
  permission", **[verified]** live). That is always this app's: another identity's own such
  refusal reaches this one as a plain refusal (**[verified]** odin-core b79aefa99: a peer's
  403 is thrown anew as "Remote server returned 403", with no code:
  `PeerAppNotificationService`, `PeerDriveQueryService`, and `PeerCircleShareKeyService`
  for a circle's key (it keeps `notReviewedByOwner` alone); the code itself in
  `PermissionContext.MissingPermissionKey`). Before asking
  for a circle's key, the client refuses a drive without a write-only key that opens
  (`Refusal::NoKey`, without asking the sharer). It never looks up first whether the app
  holds `UseTransitRead`: the ask tells, also once the owner has given the permission
  (which no reconnect says; **[inferred]**: the permission update closes no socket). A 403
  without that code is the sharer's (**[verified]** odin-core `PeerServiceBase`;
  `Client::whose`). Lacking the permission, the share stops, refused
  (`Refusal::Permission`) until the owner approves; a 401 there is this login's. On the
  sharer's own server (`Client::for_peer`) a 401 refuses the token (`peer::refused`), a 403 a file
  or drive not shared with this identity.
- **Reading on their server** **[verified]** (odin-core `PeerDirectReadAttribute`, #2127;
  `drive/shared.rs`, `PeerRemote`): with the token as `Authorization: Bearer`, the routes of
  their drive by its alias (the marker's `driveAlias`), `/api/v2/drives/<alias>/files/...`:
  `query-batch` (the feed and `groupId` listings alike; no other query route), `{fileId}/header`
  and `{fileId}/payload/{key}[/{start}/{length}]`, sealed with the token's shared secret as an
  app's are (payloads aren't). A member's query lists only files with a share row for one of
  its circles, with the same cursor, hold-back and `resultShape` as the owner's, never cached;
  a header carries `shares` (the live rows the file reaches them through, by circle id) and no
  server metadata. Each of those rows has `expiresAt` (Unix ms, `null` where the share has
  no end), and a row is live while `expiresAt` is after the server's now (**[verified]**
  odin-core 139c0f618: `ShareRow.From`, `TableDriveAclIndex.IsLive`, `CallerShares.LiveRowsOf`;
  the file is read until its last live row ends). **Nothing is written when a share
  expires** (**[verified]**: the row stays, only `IsLive` changes; there is no job for it):
  the file doesn't move in the feed and nobody is notified, a read by id is refused from
  then on (the codeless 403 of a file not shared), and the query shows the tombstone below
  only where it reaches the file's row: from its beginning, or after the file's next
  change. The client therefore keeps the time and asks (sync semantics §13, "A share that
  ends"). A share withdrawn or expired (or above an unreviewed connection's level)
  comes as a tombstone without its version (all zeros), parent, node id or key; a soft delete
  by the sharer as the usual tombstone; a hard delete takes the rows with it, so the file just
  isn't listed any more, and no notification tells the member (the rows go in the delete's own
  transaction, before it is announced, and the socket then finds no row naming the member:
  **[verified]** `DriveStorageServiceBase.HardDeleteHeaderAsync`,
  `MainIndexMeta.DeleteEntryAsync`, `DriveEventMessage.MessageForAsync`): only a full re-read
  finds it gone, which a member does a week after its last (sync semantics §13, "Deleted for
  good by the sharer": the header of a file gone so is a 404 for the member too,
  **[verified]** `DriveStorageServiceBase.ReadForCallerAsync`, the peer header routes of
  `V2PeerAppDriveReadControllers`). The "no longer shared" tombstone keeps the file's id, `fileType` and its own
  `updated` (**[verified]** `DriveStorageServiceBase.NoLongerSharedTombstoneOf`), and is sent
  again after every change of the file while a row still names the member; sharing or
  withdrawing moves the file's `modified` and keeps its version, so a share given again brings
  the file back live at the version it had (**[verified]**
  `TableDriveMainIndex.TouchKeepingVersionTagAsync`). So does a share's row written again
  with other permissions (reading turned to editing, or back: **[verified]** odin-core
  `MainIndexMeta.WriteSharesAsync`, which upserts the row and touches the file keeping its
  version, and `DriveShareService`, which then announces the file; not tried live): the
  member's feed brings the file again at the version it has, its rows as they are now, and
  the client takes whether it may change it from them (`RemoteHeader::writable`). The rows
  can say more than the server allows: a row is checked against its circle's grant on the
  drive when it is written, not after, so a circle that has lost `Write` since leaves rows
  with it, and the member's write is refused (**[verified]** odin-core
  `DriveShareService.AssertCircleFitsAsync`,
  `DriveStorageServiceBase.AssertMemberMayWriteAsync`). That is the header's `updated` too
  (**[verified]** `FileMetadata`: `Updated = record.modified`), so `updated` is when the file
  last changed in the feed, not when its version was written: a removed node's moves with a
  share change too. The client therefore reads no time from `updated` (but the "no longer
  shared" tombstone's, which has no version): "A version's own time", next.
- **Their notification socket** (`notify.rs`, `Notifier::spawn_shared`; **[verified]**
  odin-core `V2PeerNotificationSocketController`, `PeerAppNotificationHandler`,
  `EstablishConnectionOptions.ApplyTo`, and **[tried]** on the dev server 2026-11-21, tom's
  device on pippin's server): `wss://<sharer>/api/v2/peer/notify/ws-token`, upgraded with no
  credentials. The first frame says who connects and subscribes at once, in the clear:
  `{"clientAuthToken64": <the token>, "sharedSecretEncryptedOptions": {iv, data}}`, the
  second an envelope of the `establishConnectionRequest` command sealed with the token's
  shared secret; every later frame, either way, is as on the identity's own socket
  ("Notifications", above). It is the token the files are read with; the socket's context is
  cached apart from the reads'. `excludeOwnWrites` is refused there (`InvalidQuery`); the
  shapes are the same, and this client asks for `minimal`. Tried: subscribed 80 ms after the
  connect; a change to a file shared with the member came 40 ms after the write, as a
  minimal `fileRef` with the new version, the parent and `versionUpdated` (1,719 B for the
  handshake and that event together); a change to a file not shared with it brought nothing.
  A token their server doesn't take gets an unsealed `authenticationError` ("Invalid Token"),
  then a 1008 (PolicyViolation) close: the client reads the frame as the refusal at once
  (**[tried]** 2026-11-22 on frodo at odin-core 3ace44b69, with a token it never made: refused
  on the first connection, 6 s after the probe began; before odin-core PR #2149, merged
  2026-11-21 as b8946173b, the server closed the socket first, with no frame). Their server
  still ends a connection without a word when it stumbles: a notification of its own sent
  while a connection still says who it is closes that connection (**[verified]**
  `PeerAppNotificationDispatcher.SendMessageAsync` removes a socket without a context), and
  so does an error while it looks the token up. So the client makes a connection ended
  before its subscription again once, soon, and reads a second one running as the refusal:
  asked again as for a refused login, not at once.
  Every notification of
  their server that isn't a drive's goes to every peer socket (**[verified]**
  `PeerAppNotificationDispatcher.WsPublishAsync`; none came in 70 idle seconds): unread here.
  A reconnect with the same token subscribes at once, and so does a second socket beside
  the first (**[tried]** at d347a25a6). It needs odin-core #2132 (merged 2026-11-19 as
  625085b90, #2144): before it a token subscribed once an hour only, as the handler set the
  socket's name on the token's cached context and every later handshake with the token
  failed until the cache dropped it (**[tried]** at 139c0f618: a reconnect a second later, a
  second socket, and a new process five minutes later were each closed).
- **A version's own time** (`versionUpdated`, odin-core PR #2143, merged as 5e4cd493f on
  2026-11-17, #2142; **[verified]** there: `DriveFileUtility.VersionUpdatedOf` and
  `AnswersVersionUpdated`, `ClientFileMetadata.VersionUpdated`, `FileRef.VersionUpdated`,
  `WriteTimes`): the `updated` of the write that gave the file its current `versionTag`,
  Unix ms. It moves with every write that makes a new tag (content, metadata, archival
  status, a soft delete's tombstone) and stays on what keeps the tag: a share given or
  withdrawn, the reaction summary, the transfer history, local app data. Answered on the V2
  API only (and to another identity's server, which relays it to its own V2 callers): in a
  header's `fileMetadata`, in a minimal row and a socket event's `fileRef`, and in the answers
  to a create, an update, the archival-status batch and a soft delete (single and batch);
  left out where there is none: the "no longer shared" tombstone, and a row whose column is
  0, which only a server still on the older code can have written while the newer one ran
  (the column's default; its next write sets it). A file written before the server had the
  field answers its `modified` of then (**[verified]**
  `TableDriveMainIndexMigrationV202611171200`: the migration copies `modified`). It is `RemoteHeader::changed_at` (`wire::Times`): what the client judges
  "written after its folder's removal" and a removed node's 30 days by, on first sight too
  (filesync #385, closed by it). The feed's order and cursor stay on `modified`.
  A tombstone's age (when it may be deleted hard) doesn't move: withdrawing a share of a deleted
  file withdraws it (the member reads it no more, and their feed has the "no longer shared"
  tombstone at the file's place) but leaves its time, its place in the feed and its age as they
  were (odin-core PR #2141: `DriveShareService.WithdrawShareAsync`, `FileShares.MovesModified`;
  **[verified]** at aa560aea9); sharing a tombstone is refused. The
  client can tell a share withdrawn from a delete
  by its zero version and its time (`RemoteHeader::is_unshared`, `ChangeRef::is_unshared`);
  the engine goes by the zero version alone on another identity's drive (the state DB's
  `withdrawn`: no delete for good is named to a member, and a withdrawal taken for a delete
  would never come back), and drops the file from its picture without recording it gone (the
  file leaves the member's devices, and the share given again brings it back: sync semantics
  §13, "Another identity's drive"). A member's notification socket hears what its query gets: a file through its
  share, the same tombstone once the share ends (after every change of the file while a row
  names the member), and nothing of a file no row names them on (**[verified]**
  `DriveEventMessage.MessageForAsync`, `DriveStorageServiceBase.NoLongerSharedViewOf`); a
  share written or withdrawn raises the file's ordinary change event, a withdrawal of a share
  withdrawn already none (**[verified]** `DriveShareService.WithdrawShareAsync`); a deleted
  file's withdrawal raises it too, at the tombstone's unchanged time (PR #2141,
  `PublishChangedAsync`: **[verified]** at aa560aea9).
  `FakeRemote::member` (`hbsync-remote`) models this member's view, and the contract's
  read-only subset (`Bench::shared`) checks the fake now (a member that writes,
  `FakeRemote::writing`, passes its editing subset, `Bench::editing`: what it changes of what
  is shared with it for editing, and that it deletes nothing). Live it needs a sharer that writes
  share rows, which this client doesn't do yet (the chat app shares): until it does, the
  live check is the daemon syncing what the chat app shared (**[verified]** 2026-11-20 on
  the dev server: a folder with a subfolder and 20 files, a member's token kept and reused). Refusals: 401 the token (unknown,
  the connection gone, their keys invalid), 403 a file not (or no longer) shared, 404 none
  there. A share-key answer not sealed yet or not reviewed waits (`RemoteError::Waiting`, asked
  again in 15 minutes); reads never write: every write refuses before a request.
- **What a member adds, on the owner's drive** **[verified]** live (dev server, odin-core
  3ace44b69, 2026-11-23: a file added as a member through the member's own server, read with
  the owner's Filesync login): the file is an ordinary row of the owner's feed, its
  `senderOdinId` the member, its ACL `connected`, its `sharedSecretEncryptedKeyHeader` of
  type `sealed` (12): `iv` the header IV, `encryptedAesKey` a `ZkSeal` version 1 value of
  `iv ‖ key`, sealed to the drive's write-only key (encryption spec, "A private drive"). An
  owner's update of it with a `zkDrive` key header is taken, and the header is `zkDrive` from
  then on; the member's share row keeps opening it (the file key stays). Until then a member's
  edit must come sealed too (`recipientIdentityReturnedInvalidKeyHeader` otherwise). chat-kmp
  also sends the re-locked key header alone, at the version read
  (`POST /drives/{driveId}/files/transform-key-headers`: the sealed key header is replaced
  by the `zkDrive` one sent, with the same IV, and nothing else changes, "not the version
  tag, not modified, and no file-changed event"; **[verified]** odin-core
  `V2DriveBatchFileWriteOperationsController.TransformKeyHeaders`, chat-kmp
  `SealedKeyHeaders`, `DriveFileOperationsProvider`): this client doesn't. The sealed IV must
  be the header's own (the server can't check it; a member reads with the header's,
  `EncryptedKeyHeader.ForShare`): this client leaves a file whose two differ unread.

## Writing to what others share
What a change to another identity's share uses (`drive/shared/writes.rs`), where they shared
it for editing. Built, and not turned on: a share is still read-only in the product
(`PeerRemote::writing`). **[verified]** live means seen on the dev server (odin-core
3ace44b69, 2026-11-23), a member writing to a share made with "Edit" in the Homebase app.
- **Only through this identity's own server.** Their server takes no write from a member
  directly (**[verified]**: no such route; its peer token reads). This identity's server
  stages the write on its transient temp drive (alias and type
  `90f5e74a-b7f9-efda-0ac2-98373a32ad8c`), sends it on from its outbox, and removes the staged
  copy once sent. The bytes pass through it: twice the network of an own upload.
- **The permission.** The app must hold `UseTransitWrite` (210): without it a send answers
  `403 "Sending, updating or deleting files on a peer requires UseTransitWrite"`, its
  `errorCode` `missingPermissionKey` (Errors; **[verified]** live at b79aefa99, through the
  client: told as the permission, their file as it was). It leaves nothing behind (the
  permission is asked before anything is stored: odin-core #2155, PR #2159, **[verified]**
  live). The client asks the security context first all the same, and sends only where it
  holds 210 (while it doesn't, the context is read again at most once a minute): a send
  refused has uploaded its bytes for nothing. The app doesn't ask for it at registration
  yet.
- **Whether it may.** Their server asks drive `Write` and, for a file there already, a live
  share row with `Write` on it (**[verified]** odin-core
  `DriveStorageServiceBase.AssertMemberMayWriteAsync`, `DriveAclAuthorizationService
  .CallerMayAsync`). "Edit" in the Homebase app gives both: the row, and membership of the
  app's "Filesync editors" circle (slug `filesync-edit`), which grants drive `Write`
  (**[verified]** chat-kmp `FilesyncSharing.share`, and live). A row's `permissions` come as
  odin-core's flag names (`"read"`, `"read, write"`, `"readWrite"` = read, write, react and
  comment): the client reads them, and refuses a write to a file no row gives `Write` on
  before sending.
- **A change to a file** (its header, with or without new bytes): `PATCH
  /api/v2/peer/{owner}/drives/{driveAlias}/files/by-gtid/{globalTransitId}`, the multipart
  form of an own update with `locale: "peer"` and `recipients: [owner]` in `instructions`.
  The `metadata` part is sealed with this login's shared secret, and its header is the whole
  header again (as every update) with `allowDistribution: true` (refused otherwise,
  **[verified]** `FileSystemUpdateWriterBase`), the ACL `connected`, and `versionTag` the
  version of THEIR file that is changed. The deferred payload hash works here as on an own
  upload. Answer: `{globalTransitId, newVersionTag, recipientStatus: {owner: "enqueued"}}`
  (a send that couldn't be queued answers 200 too, with `enqueuedFailed`, and leaves its
  staged copy behind, odin-core #2158: the client waits for no word then, and asks again in a
  minute):
  `newVersionTag` is the version their file will have, known before it lands
  (**[verified]** live). A header alone (a rename, a move, `archivalStatus` 2 for their
  Recently deleted and 0 back) keeps the bytes. There is no batch for a member: several
  files moved to their Recently deleted go one after another, and an answer that isn't one
  file's own (a server not reached, not connected, a send still on its way) stops the rest,
  which would each meet the same after their own wait.
- **Its key header.** The file keeps its key. Where their server holds that key locked with
  the drive's (`zkDrive`), the member sends the new header IV alone: `{type: "share", iv,
  encryptedAesKey: ""}`. Where it holds it sealed to the drive still (a file a member added,
  not yet taken in by a device of the owner's), that is refused, and the member sends it
  sealed again: `{type: "sealed", iv, encryptedAesKey: ZkSeal v1 of iv ‖ key}`. A sealed one
  over a `zkDrive` one is refused in turn, unless the file is the sender's own and nobody
  else changed it since (**[verified]** odin-core `ZkDriveFiles.KeepFileKey`,
  `AssertMayReplace`, and live both ways). A member can't see which it is (its own view of
  the key header is always `share`): the client sends kept first, sealed for a file it added
  itself in this run, and the other way after a refusal. A header alone goes twice by
  itself; a change with bytes fails once (the bytes are read once) and is sent the other way
  when the engine tries again.
- **A new file or folder**: `POST /api/v2/drives/{transient drive}/files` on this identity's
  own server, with `storageOptions.drive` the transient drive and `transitOptions:
  {recipients: [owner], isTransient: true, remoteTargetDrive: {alias, type}}`. Its descriptor
  carries the key header sealed to their drive's write-only key, a `globalTransitId` picked
  by the member, and `shares`: one `{fileId: nil, circleId, permissions, expiresAt,
  lockedFileKey}` per share row of the folder it goes into (the same circle, permissions
  and end), the file's key locked under that circle's key and bound to the global transit id
  (format 3; encryption spec). Without a share the file would be its owner's alone. Their
  drive's key: `GET /api/v2/peer/{owner}/apps/filesync/drives/filesync/public-key` →
  `{publicKeyJwk, publicKeyCrc32, callerMayWrite, isZeroKnowledge, sharingModel}`, asked
  once per share and run. The new file's id on their drive isn't in any answer: the client
  reads it by `GET /drives/{alias}/files/by-uid/{uniqueId}/header` on their server (one
  request). All **[verified]** live; the circles of the folder the member isn't in get their
  rows from a device of the owner's (**[verified]** chat-kmp `FilesyncShareUpkeep`; this
  client's owner side doesn't do that).
- **The version a new file lands as** is made by their server (**[verified]** odin-core
  efc317ad4, `PeerFileWriter`: `CreateVersionTag`; live: never the send's `newVersionTag`),
  and told in one place only, the socket's word of the delivery (`versionTag`, below;
  **[verified]** live: the version their file then has). The client answers the write at that
  version. Without the word it reads their file, and takes its version as the write's only
  while the file is as it was sent (the same header and bytes): one its owner changed in
  between is answered "already there" for the engine to read, as taking the owner's version
  for the member's bytes would count the owner's change as synced. No request more: their
  file is read for its id either way. One case where the word's version isn't the write's:
  a send their server took, whose answer was lost, is sent again by the outbox and found
  landed already, and the word then names the version a move to or from their Recently
  deleted gave the file since (**[verified]** odin-core efc317ad4, `PeerFileWriter`'s
  already-landed answer, `DriveStorageServiceBase.CarrySendersWrite`; not seen live). The
  client takes a told version that is their file's now only where the file is in their
  Recently deleted or not as it was sent, and compares the file otherwise.
- **What became of it** is told on this login's own notification socket: `notificationType:
  "peerSendResult"`, `data` `{recipient, driveId, globalTransitId, sentVersionTag, isUpdate,
  isDelete, status, willRetry, versionTag}`. `sentVersionTag` is the send's `newVersionTag`;
  `status` is odin-core's `LatestTransferStatus`: `delivered` (`versionTag` the version the
  write gave their file: the one sent for a change, another for a new file),
  `recipientIdentityReturnedVersionTagMismatch` (their file changed since: nothing written),
  `recipientIdentityReturnedInvalidKeyHeader` (the key header, above; also one sealed to a
  key their drive no longer has, or an IV not rotated), `recipientIdentityReturnedAccessDenied`,
  `recipientIdentityReturnedBadRequest`; with `willRetry: true` the outbox tries again by
  itself (their server not answering, out of storage). **[verified]** live for the first
  three, the rest in `OutboxWorkerBase`. `GET …/by-gtid/{gtid}/send-status?sentVersionTag=`
  says only whether it still waits in the outbox (`{pending, sending, attempts,
  nextAttempt}`): the same after a delivery and after a refusal (**[verified]** live).
- **How the client waits.** A write waits for its word on the socket (`sends.rs`: the
  notifications of the own drive feed it, a write takes the one for its global transit id
  and sent version). Where none comes in 0.3 s it reads their file itself (their version is
  the one sent: landed), again after 1, 3, 7 and 15 s; then it asks the outbox: still
  pending is `RemoteError::Waiting` (asked again in a minute, and not sent twice in one run:
  the write is remembered by node and by what it wrote), not pending and not landed is
  retried. Measured live, the whole write with the socket connected: 20 to 40 ms for a
  header, 60 to 120 ms with bytes or for a new file; without a socket, the first wait and one
  read more.
- **A new file that is there already.** A node found on their drive where a new one was
  sent is that send's only by its `globalTransitId`. With another's, an earlier send of the
  node landed (one from before a restart, or one this run stopped waiting for), and their
  server refuses the later one (**[verified]** odin-core `PeerFileWriter`: a file sent
  again with shares is an `ArgumentError`, told as `recipientIdentityReturnedBadRequest`):
  the client says `AlreadyExists`, and the engine reads what is there, its bytes too.
- **One write of a node at a time.** The staged copy carries the node's `uniqueId`, unique
  on the transient drive too: a second write of the node while the first is still in the
  outbox answers `400 existingFileWithUniqueId` (**[verified]** live for a stray copy;
  **[inferred]** for one still on its way), taken as "still on its way".
- **Not used:** a member's delete. `POST …/files/senddeleterequest` (with
  `expectedVersionTag`, odin-core PR #2140) writes a tombstone; this client moves a member's
  delete to the owner's Recently deleted instead (`archivalStatus` 2, above), and deletes
  nothing there. (The V2 route takes its JSON body plain, not sealed with the shared secret:
  **[verified]** live, a sealed one reads as empty.)

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
  request, whatever its size. Uploads over 4 GiB failed (the `uint` byte count, the other half
  of #1893) until odin-core PR #1932 (merged 2026-10-14, v0.1.960); the client now takes files
  up to 50 GiB (`MAX_FILE`, its own cap for an upload with no resume).
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
