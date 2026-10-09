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
  `a`, `t`, `n`, `d`, `p` (Read=1, Write=2), `ds`, `ts`, `r`, `s`.

## The Filesync app on an identity (`crates/hbsync-homebase`, tried on frodo.dotyou.cloud 2026-10-09)
- **Ids and slugs** **[verified]**: YouAuth still requires an app id (`client_id`, and
  `appId` in `permission_request`; `YouAuthAppParameters.cs:10-13`), and a drive request names
  the drive by alias and type. Registration also requires the app slug `as`
  (`AppRegistrationService.AssignSlugAsync`), and the owner app requires `ts` (and reads `ds`)
  on every requested drive and refuses a request without `cd` (`RegisterApp.tsx:74`). Slugs:
  `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`, at most 14 characters (`OdinSlug.cs`). Filesync uses app
  slug `filesync` and one drive `filesync`/`filesync`; its ids are fixed in `app.rs`.
- **Addressing by slug** **[verified, tried]**: once registered, the client finds its drive
  with `GET /api/v2/apps/filesync/drives/filesync` (`V2AppDriveController.cs`); the answer
  carries `targetDrive {alias, type}`, `driveSlug`, `driveTypeSlug`, `appId`, `attributes`.
  File routes still take the alias.
- **Registration flow** **[verified, tried]**: an unregistered app is sent to
  `/owner/appreg`, which creates the requested drives and registers the app, then returns to
  the authorize URL (`YouAuthUnifiedController.cs:155-162` sets `return`), so the flow resumes
  by itself. An app cannot create drives itself.
- **Circles** **[verified, tried]**: circles have no slug. An app creates its own after
  registration: `POST /api/v2/connections/circles/create` with `{name, description, emoji,
  driveGrants: [{permissionedDrive: {drive, permission}}], permissions: {keys}}`; only drives
  the app owns and keys it holds (`CircleMembershipService.CreateAppCircleAsync`). Returns the
  circle id. Filesync's circle grants Read on its drive and starts empty.
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
| Create | `POST /api/v2/drives/{d}/files` | multipart, plaintext request and response (`V2DriveCreateFileController.cs`). Parts: `instructions` first, `metadata` second, then `payload`* (filename = payload key, case-sensitive) and `thumbnail`* (`V1DriveUploadControllerBase.cs:205-270`). Duplicate live UniqueId → 400 4105. Response `{fileId, driveId, globalTransitId, recipientStatus, newVersionTag, payloads[{key, uid, lastModified}]}` |
| Update | `PATCH /api/v2/drives/{d}/files/{fileId}` (or `/by-uid/{uid}`) | multipart, plaintext; `metadata` required. `fileMetadata.versionTag` required (4159), stale → 4160. Instructions need `locale: "local"` (missing → 500) and a `manifest`; `payloadDescriptors: []` is a header-only update that keeps the payloads. Ops `appendOrOverwrite` / `deletePayload`; unlisted payloads kept. Unknown fileId → 4106, deleted file → 4002 |
| Delete (soft) | `POST /api/v2/drives/{d}/files/{fileId}/delete` | encrypted body `{recipients: []}` (send it; an empty body likely fails binding **[inferred]**). **No versionTag** (odin-core #1903). Unknown file → 200 `localFileNotFound: true`. Deleting a tombstone rewrites it: new versionTag, new feed row, another `fileDeleted` |
| Query | `POST /api/v2/drives/{d}/files/query-batch` | encrypted body `{queryParams, resultOptionsRequest}`; see below |
| Header | `GET .../files/{f}/header`, `GET .../files/by-uid/{uid}/header` | encrypted response with `content`. A deleted file → 200 `fileState: "deleted"`; unknown → 404 |
| Payload | `GET .../files/{f}/payload/{key}` | raw bytes; this route **ignores Range** |
| Ranged payload | `GET .../files/{f}/payload/{key}/{start}/{length}` | a `Range: bytes=a-b` header here wins and gives 206; end past the payload → 416. Offsets are `int` (#1893) and every read is buffered whole in server memory (#1892) |
| Notify | `wss://{id}/api/v2/notify/ws-token` | see below |

The client only soft-deletes: the tombstone is how the other clients learn of a delete
through the feed. A hard delete (`.../hard-delete`) leaves no trace in the feed and is never
used. Only the Filesync app writes to its drive (other apps need a grant), so no hard deletes
come from elsewhere either.

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

**Update replaces the metadata, it doesn't merge** **[verified]**
`StandardFileUpdateWriter.cs:48-85`: an update that leaves out `uniqueId` clears it, and a
missing `accessControlList` resets to owner. Every update resends the whole `appData`.

Limits **[verified]**: `content` at most 10,240 UTF-16 code units (4167); file types in
`2100000000..` are reserved (4120); one upload payload at most 4 GiB (`uint` byte count); out
of quota → 507 with `Retry-After`. App callers get `serverMetadata: null` in headers.

## Change feed
**[verified]** `QueryBatch.cs`, `KMP/sync/DriveSync.kt:173-295`:
- `resultOptionsRequest`: `sorting: "anyChangeDate"`, `ordering: "oldestFirst"`,
  `includeMetadataHeader: true`, `maxRecords`, `cursorState` (opaque string, persist after each page).
- `queryParams.fileState` null → tombstones included.
- Only rows with modified < now are returned; with `oldestFirst` the cursor resumes after the
  last change. Page until `hasMoreRows == false`.
- `sorting` must be sent: the default is creation order. `maxRecords: 0` → 500.
- `cursorState` is a JSON string (`{paging:{time,row}}`), not base64. An empty page returns the
  input cursor. **An unparsable cursor silently restarts from the beginning**
  (`QueryBatchCursor.cs:135-150`).
- Every header write sets `modified = max(modified + 1, now)`, so a change never lands behind
  the cursor.
- Tombstones: `fileState: "deleted"`, `uniqueId: null`, `content: ""`, `payloads: null`;
  `fileId`, `groupId`, `fileType` kept.

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
| 4175 / 4176 / 4177 | `payloadHashMismatch` / `invalidPayloadHash` / `payloadHashRequired` |

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
  `{notificationType, data}`. `fileAdded`, `fileModified`, `fileDeleted` carry the header.
  Reaction updates also send `fileModified`: treat all of them as a hint to read the feed.

## Conventions
- camelCase JSON, enums as camelCase strings, bytes as standard base64, times as Unix ms.
- Errors are RFC 7807 ProblemDetails with `errorCode` as a number **or** a camelCase string.
- Payload hashes: `../odin-core/docs/payload-hashes.md` (errors 4175–4177).

## Known gaps
- **Apps can't sign in by slug** **[verified]**: YouAuth needs the app id GUID
  (`AppNeedsRegistration`: "App id must be a uuid"), so an app with a per-identity id can't
  authorize a second device; own-identity file routes exist only by drive GUID (slug routes only
  under `/peer/`). Filed as odin-core #1902 (bug, high priority). Until it's fixed Filesync keeps
  fixed ids for registration and resolves its drive by slug.
- **No file-sync drive kind** **[verified]**: the drive flag `RequirePayloadHashes` (#1899)
  can't be requested at registration (no field in `d`), the owner app drops a requested
  drive's attributes when it creates it, and no owner UI sets the flag or a kind. App clients
  can't read the flag either (`ClientDriveData` lacks it). To do in odin-core / odin-js.
- **Circle permission keys lost** **[verified]**: odin-js sends `cp`, but the server's
  `YouAuthAppParameters` has no `cp`, so they vanish on the redirect to `/owner/appreg`.
- `docs/mandatory-slugs-and-app-ids-plan.md` calls `DriveSlug`/`DriveTypeSlug` required; the
  server derives a missing one instead (only the owner app insists on `ts`).
- Delete has no VersionTag guard → a delete can wipe a concurrent edit. Filed as odin-core
  #1903 (optional `versionTag` on delete, 4160 on mismatch; also: deleting a tombstone should
  be a no-op). Until then the client reads the header and compares right before deleting.
- Payload reads are buffered whole in server memory (#1892) and ranges are `int` (#1893): the
  client reads in chunks and refuses payloads of 2 GiB or more until #1893 lands.
- App clients can't read the drive's `requirePayloadHashes`; the client always sends hashes.
- **The feed can hide a write until the next one** **[verified, tried]**: `QueryBatchCached`
  caches query-batch results for two hours (`AbstractTableCaching.DefaultTtl`) and drops
  them only when the drive is written. An `anyChangeDate` result also depends on the clock
  (`modified < now`). A query in the same millisecond as a write is therefore cached without
  that row, and every identical query (same cursor, same page size) misses it until the next
  write to the drive. On frodo, about one write in twelve stayed invisible to a polling
  client. Workaround in the client: never send the same page size twice in a row (it is
  part of the cache key). Contract scenario `every_write_shows_up_promptly`. Filed as
  odin-core #1905 (fix: don't cache an answer that left out rows with `modified >= now`);
  tracked in filesync #1.
- **A content update of a deleted file is a 500** **[verified, tried]**: a PATCH with an
  `appendOrOverwrite` payload on a tombstone throws a NullReferenceException in
  `DriveStorageServiceBase.UpdateBatchCopyFilesAsync` / `ProcessAppendOrOverwrite` (the
  tombstone's payload list is null) before the 4002 check. Workaround in the client: after a
  failed update, read the header; a tombstone means `NotFound`. Filed as odin-core #1904
  (a header-only update of a tombstone correctly answers 4002); tracked in filesync #6.
