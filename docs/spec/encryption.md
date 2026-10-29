# Encryption

How the client encrypts what it syncs, in Homebase's own format (AES-CBC), so the web app, the
mobile apps and WebDrop can read the same files. Code: `crates/hbsync-homebase/src/drive/sealed.rs`
(the crypto), `drive/wire.rs` (headers and upload parts), `drive/mod.rs` (reads and writes). Facts
about the server: `homebase-api-notes.md`, "Encrypted files".

## In plain words

**The pieces**
- **The shared secret.** When you approved this computer in the browser, it and your Homebase
  agreed on a secret only the two of them know. The client already wraps every request and
  answer with it.
- **The drive key.** Your Homebase has a key of its own for the sync drive, locked away under
  your owner key. The Filesync app may unlock it because you granted it read access to the
  drive.
- **A file key and an IV, new for every file.** For each file it creates, the client makes up a
  random 16-byte key and a random 16-byte starting value (the IV). Nobody else ever chooses
  them.

**Sending a file**
1. The client encrypts the file's bytes with the file key (AES-CBC). Each upload of the bytes
   gets its own new random IV, so two versions of a file never look alike, even when they start
   the same.
2. It encrypts the file's details the same way: its name, size and modified time (the node
   JSON). Folders get a key of their own too, so folder names are hidden as well.
3. It hashes the plain file. That hash is also encrypted with the file key and goes along with
   the file.
4. It sends the file key to Homebase locked with the shared secret, so nothing in between can
   read it. Homebase unlocks it and locks it again with the drive key, and stores it that way
   next to the file. The encrypted bytes are stored exactly as sent.

**Reading a file**
- Homebase sends the encrypted bytes, plus the file key locked with this computer's shared
  secret. The client unlocks the key and decrypts.
- CBC can't tell when bytes were changed, so the client hashes what it decrypted and compares
  that with the encrypted hash that came along. A file that doesn't match is refused and
  fetched again.
- CBC lets you decrypt from any point once you have the 16 bytes just before it. That's how an
  interrupted download resumes, and how a video player seeks, without starting over.

**What it protects, honestly**
- What lies on the server's disk and in its backups is encrypted: file contents, names, sizes
  and times.
- It is zero-knowledge at rest: the server doesn't hold the key to the drive key. Only the
  owner and the apps and devices they approved do. The server unlocks a key in
  memory only for the request that needs it, from one of them. That is how the web
  and mobile Homebase apps work today **[verified]** (odin-js `SecurityHelpers.encryptKeyHeader`,
  chat-kmp `PayloadBundleEncryptionService`, odin-core `FileSystemStreamWriterBase`), and
  they can read the same files: the format is odin-js's byte for byte (the golden
  vectors below) **[verified]**, and chat-kmp's code uses the same layout **[verified in its
  code, not run]**. Neither app shows the sync drive yet.
- Once the owner makes the drive private (zero knowledge, below), the server holds no key that
  opens it at all, even in memory: only the owner's browser and the devices that hold the app's
  zero-knowledge key do.

## The format

All AES is AES-128-CBC with PKCS#7 padding. Byte strings in JSON are standard base64.
The format is checked against odin-js byte for byte: `fixtures/encryption-odin-js.json`, made by
`fixtures/encryption-odin-js.ts` with js-lib's own functions (`sealed.rs`, `matches_odin_js`).

| What | How |
|---|---|
| Key header | `iv` (16 random bytes) and `key` (16 random bytes), both "strong": not every group of four bytes the same |
| Sent key header (`encryptedKeyHeader`) | `{encryptionVersion: 1, type: "aes", iv: transferIv, encryptedAesKey: CBC(sharedSecret, transferIv, iv ‖ key)}` |
| Received key header (`sharedSecretEncryptedKeyHeader`) | the same shape; opened with the shared secret |
| On a private drive (`type: "zkDrive"`, sent and received) | `{encryptionVersion: 1, type: "zkDrive", iv: headerIv, encryptedAesKey: CBC(driveKey, headerIv, iv ‖ key)}`: under the drive's key, with the header's own IV as the wrap IV (odin-core's vector, `sealed.rs`) |
| `SharedSecretEncryptedHeader64` (payload responses) | base64 of IV (16) ‖ encrypted `iv ‖ key` (48) ‖ version (4, little-endian); its `iv` is the payload's |
| `fileMetadata.isEncrypted` | `true` |
| `appData.content` | `CBC(key, headerIv, node JSON)` |
| Payload `iv` | 16 random strong bytes, new at every upload of the bytes |
| Payload bytes | `CBC(key, payloadIv, plaintext)`; stored size `(n / 16 + 1) · 16` |
| `hash.storedHash` | hash of the payload bytes as stored (the ciphertext) |
| `hash.contentHash` | `CBC(key, payloadIv XOR 0xAA…, plaintext hash)`: 48 bytes |

### Writing
- **A new node**, file or folder, is always encrypted, with a new key header.
- **An upload reads its source once.** It hashes the plaintext, encrypts it, hashes the
  ciphertext and sends it, in one pass. The hashes follow the payload (odin-core #1935): the
  instructions carry the algorithm and the deferred sentinel, and a `payloadHash` part after the
  payload carries the stored hash and the sealed content hash. The server can't tell from
  ciphertext that a file changed after it was hashed, so a plaintext that no longer has the hash
  the upload was planned with gets values no bytes hash to: the server refuses the payload
  (`payloadHashMismatch`) and stores nothing **[verified]** (odin-core PR #1936,
  `DeferredPayloadHashes.cs`; live), and the engine plans again. A source that grew is sent only
  up to its planned size and refused the same way; one that ends early ends the request before
  its hash part: nothing is stored **[verified]** (live contract), and it counts as the same
  mismatch. A file whose fingerprint (size, mtime, change time, file id) changed since it was
  hashed isn't sent at all; one that changes while it is sent is looked at every 16 MiB and
  stops there, the request ending before its hash part. One that can't be read is a local
  error, not the server's.
- **An update** sends the file's own key with a new header IV, and the content encrypted with
  those: the server requires a strong key, keeps the file's key, and takes only the IV. New
  bytes get a new payload IV. The key comes from the file's **token**: its key header as the
  server hands it out, sealed with the shared secret (as safe as the server's copy), kept on its
  row in the state DB (`remote.token`, `removed.token`). A write works out the token the server
  will give for the version it wrote (the key header sealed with its own IV as the wrap IV,
  **[verified]** odin-core `DriveFileUtility.CreateClientFileHeader`), so the feed bringing the
  write back changes nothing; another device's write brings its token with it. One request does
  an update; the version tag guards it (4160 if the file changed since; a deleted file 4002). A
  file without a stored token has its header read first, as before; so does one whose token
  doesn't open. Tokens are sealed under a login's shared secret: when the
  account connects with another login than the one its tokens were sealed under (a fingerprint
  of its key, `remote.tokens_sealed_by`), they are all forgotten, and each file's next update
  reads its header once and keeps the token it writes (the feed's slim rows carry none). A token
  is only ever used for its own file: another file's key would leave content no one can read.
  One opens only if it decrypts to exactly 32 bytes, so a stale one (garbage under another key)
  as good as never passes for a key.
- **Every node is encrypted.** A live row that isn't is skipped as unreadable: no client has
  users with unencrypted nodes. (The server couldn't change `isEncrypted` on an update anyway **[verified]**,
  `homebase-api-notes.md`.)

### Reading
- A header or feed row: open the key header with the shared secret, then
  decrypt the content, and the content hash under its payload IV. A row that doesn't decrypt is
  skipped with its reason, like any unreadable row. So is a file whose content has
  no `size` (node schema v1 requires it for files): its stored size is padded, and taking it
  for the file's would make every download fail its hash. So is a file whose payload's
  `bytesWritten` isn't the stored size of `size` (`(size / 16 + 1) · 16`, within odin-core's
  signed 64-bit `BytesWritten`, **[verified]** `PayloadDescriptor.cs`): every download of it
  would fail, in full.
- A tombstone's key header isn't read: one that doesn't parse (null fields, say) is ignored.
- A key header of type `zkDrive` opens with the drive's key (below), then the same way.
- Payload range `p0..p1` (plaintext offsets, `p1` at most the plaintext size): fetch
  ciphertext from the block before `p0`'s block, or from 0, where the payload IV is the block
  before, up to `p1` rounded up to a block. Decrypt and keep `p0..p1`.
- No header is read first: every ranged payload response carries the file's key (with the
  payload IV) and, in `Content-Range`, the stored size **[verified]**
  (`homebase-api-notes.md`).
- A range that reaches the stored payload's last block ends with the padding: it is checked
  and removed, and a range that runs past the plaintext comes back short, as from any
  `Remote`. Otherwise the padding is never needed; the content hash, not the padding, is the
  check on the bytes.
- The extent comes in one request, streamed. A body that goes 60 s without a byte ends the
  call (the client's idle timeout, `client.rs`), and the download resumes from what it
  has.

### A private drive (zero knowledge)
The owner can make the drive private in the owner console (odin-core
`docs/zero-knowledge-drive-api-plan.md`, step 3). The drive keeps its key, so no file is
encrypted again: each key header is relabelled `zkDrive`, the same 48 bytes **[verified]**
(odin-core, 3.3). What changes for this client:
- **The key.** Logging in asks for the app's zero-knowledge key (`zk_app_key=1`); the token
  answer carries it sealed to the login's P-384 key (`ZkSeal` v1: ECDH, HKDF-SHA256 with no salt
  and the info `homebase-zk-seal`, AES-128-GCM), kept with the credentials. On a private drive
  the security context (`GET /api/v2/auth/context`) carries the drive's key locked with it
  (`zkAppKeyEncryptedStorageKey`, AES-128-GCM: a 12-byte IV, the ciphertext, the tag). Both are
  checked against odin-core's vectors (`crypto.rs`).
- **When.** Connecting reads the security context (one request), and so does the first feed
  read after the notifications connect again: making a drive private closes every socket
  granted it (odin-core), so a computer with the key has it before its next read or write, and
  one without stops at once. A `zkDrive` key header met without the key, or a write refused
  with `driveIsZeroKnowledge`, fetches it too (the write is sent again; one under way when the
  drive was made private is sent whole first, as the server checks the key header at its end).
- **Reads.** A `zkDrive` key header opens with the drive's key; its token is then sealed under
  the shared secret like any other, so the tokens of a drive made private still open, and
  nothing is synced again. Payload responses carry no key header there: the payload key is the
  file's key with the payload's IV, from the file's header, read first (one more request per
  read; it gives the size too).
- **Writes.** The key header is locked with the drive's key and sent as `zkDrive`; the server
  refuses any other on a private drive.
- **Without the key** (a login from before this client asked for it): the server leaves the
  drive's files out of every answer, and refuses writes. This computer stops (refused: log in
  again) rather than reading files it can't see as deleted. A file that can't be read is never
  taken as gone (a header read refused is "can't tell", §8b). The tray asks such a login to log
  in again already before the drive is made private.

## What it doesn't do
- **Authentication of its own:** CBC has none. The content hash is the check, after
  decryption. A server could still swap in an older version of a file, with its matching hash.
- **Thumbnails, video segments, GCM:** not used. Other Homebase clients decrypt ranges of
  plain CBC payloads the way this client does **[verified]** (odin-js
  `decryptChunkedBytesResponse`, chat-kmp `DriveFileHttpProvider`), and play an unsegmented
  encrypted video by decrypting it **[verified]** (odin-js `MediaProvider`, chat-kmp
  `VideoContentResolver`). That a synced video would play there is **[inferred]**: neither app
  shows the sync drive yet.

## Cost
Measured on an i7-8700K (AES-NI, no VAES), against a Homebase on the same machine
(`hbsync-dev` test `throughput`, and `sealed.rs` test `throughput`), with an unencrypted mode
the client had then for comparison, and 8 MiB read pieces:

| | unencrypted | encrypted |
|---|---|---|
| upload 1 GiB | 217 MB/s | 172 MB/s |
| download 1 GiB | 427 MB/s | 372 MB/s |
| create / read / update a small file | 2.5 / 6.7 / 5.0 ms | 2.5 / 6.1 / 3.7 ms |

(An unencrypted small read took a second request then.) Since reads are one streamed request
(odin-core #1921, 2026-10-11), the same 1 GiB downloads at about 600 MB/s. Uploads ran at
155–175 MB/s while they read and encrypted the source twice; in one pass (the deferred hash,
odin-core #1936, 2026-10-13) they reach about 210 MB/s. The cipher alone: encrypting 970 MB/s,
decrypting 2.8 GB/s, where OpenSSL reaches 1.6 and 6.6 GB/s. All of it is far above any
internet link. A faster AES (aws-lc-rs) would bring a C build on three OSes, and
isn't worth it until a measurement shows uploads bound by the CPU.
