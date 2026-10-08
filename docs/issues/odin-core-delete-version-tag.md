# Draft odin-core issue: optional `versionTag` on file delete

*Draft for filing in odin-core; not filed.*

## Problem
`POST /api/v2/drives/{driveId}/files/{fileId}/delete` takes no version tag (verified in
`V2DriveSingleFileWriteOperationsController.cs`; the request body is `{recipients?}`). A
delete therefore always wins, even when another client changed the file after the deleting
client last saw it. Because a tombstone drops the payloads (`DriveStorageServiceBase.cs`
~1923–1965), that concurrent edit is lost on the server.

The desktop sync client works around it by reading the header and comparing the version tag
right before deleting, which leaves a small race window.

## Proposal
- Accept an optional `versionTag` on delete (single and batch).
- When present and it doesn't match the current tag: 400 `VersionTagMismatch` (4160), as
  for updates.
- When absent: today's behavior, so existing clients are unaffected.

## Why it matters
Sync semantics say an edit beats a concurrent delete (no data loss). With a guarded delete the
server enforces that; without it, the race can only be narrowed, not closed.
