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
| Operation | Route | Notes |
|---|---|---|
| Create | `POST /api/v2/drives/{d}/files` | multipart, parts in order `instructions`, `metadata`, `payload`*, `thumbnail`* (`V1DriveUploadControllerBase.cs:205-270`). Duplicate UniqueId → 400, `errorCode` 4105 |
| Update | `PATCH /api/v2/drives/{d}/files/{fileId}` (or `/by-uid/{uid}`) | multipart; `fileMetadata.versionTag` required; mismatch → 4160. Manifest ops `appendOrOverwrite` / `deletePayload`; unlisted payloads kept |
| Delete | `POST /api/v2/drives/{d}/files/{fileId}/delete` | encrypted body `{recipients?}`; **no versionTag** |
| Query | `POST /api/v2/drives/{d}/files/query-batch` | encrypted body `{queryParams, resultOptionsRequest}`; see below |
| Header | `GET .../files/{f}/header`, `GET .../files/by-uid/{uid}/header` | encrypted response |
| Payload | `GET .../files/{f}/payload/{key}` | raw bytes; this route **ignores Range** |
| Ranged payload | `GET .../files/{f}/payload/{key}/{start}/{length}` | a `Range` header here wins and gives 206 |
| Notify | `wss://{id}/api/v2/notify/ws-token` | CAT in `Sec-WebSocket-Protocol: odin.bearer.<b64url CAT>`; frames are envelopes; subscribe with `establishConnectionRequest` |

Upload instructions **[verified]** `UploadInstructionSet.cs`, `UploadManifest.cs`:
`{transferIv, storageOptions?, transitOptions?, manifest: {payloadDescriptors: [{payloadKey,
contentType, iv, hash?}]}}`. Omit `storageOptions` rather than sending null. Unencrypted files need
empty payload IVs. The decrypted metadata part is `{encryptedKeyHeader, fileMetadata}`; the key
header must be present even when unencrypted (chat-kmp encrypts an all-zero `KeyHeader`).

## Change feed
**[verified]** `QueryBatch.cs`, `KMP/sync/DriveSync.kt:173-295`:
- `resultOptionsRequest`: `sorting: "anyChangeDate"`, `ordering: "oldestFirst"`,
  `includeMetadataHeader: true`, `maxRecords`, `cursorState` (opaque string, persist after each page).
- `queryParams.fileState` null → tombstones included.
- Only rows with modified < now are returned; with `oldestFirst` the cursor resumes after the
  last change. Page until `hasMoreRows == false`.

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
- Delete has no VersionTag guard → a delete can wipe a concurrent edit. Proposed: optional
  `versionTag` on delete, 4160 on mismatch; draft in
  [../issues/odin-core-delete-version-tag.md](../issues/odin-core-delete-version-tag.md).
- `FileChunk` ranges are `int` (odin-core #1893, being fixed separately).
