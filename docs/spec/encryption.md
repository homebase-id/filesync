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
- It is not end-to-end. While serving a request from an app you approved, the server unlocks
  the drive key, and it sees the file key in the clear during an upload. That is how the web
  and mobile Homebase apps work today **[verified]** (odin-js `SecurityHelpers.encryptKeyHeader`,
  chat-kmp `PayloadBundleEncryptionService`, odin-core `FileSystemStreamWriterBase`). In
  exchange, they can read the same files: the format is odin-js's byte for byte (the golden
  vectors below) **[verified]**, and chat-kmp's code uses the same layout **[verified in its
  code, not run]**. Neither app shows the sync drive yet.

## The format

All AES is AES-128-CBC with PKCS#7 padding. Byte strings in JSON are standard base64.
The format is checked against odin-js byte for byte: `fixtures/encryption-odin-js.json`, made by
`fixtures/encryption-odin-js.ts` with js-lib's own functions (`sealed.rs`, `matches_odin_js`).

| What | How |
|---|---|
| Key header | `iv` (16 random bytes) and `key` (16 random bytes), both "strong": not every group of four bytes the same |
| Sent key header (`encryptedKeyHeader`) | `{encryptionVersion: 1, type: "aes", iv: transferIv, encryptedAesKey: CBC(sharedSecret, transferIv, iv ‖ key)}` |
| Received key header (`sharedSecretEncryptedKeyHeader`) | the same shape; opened with the shared secret |
| `SharedSecretEncryptedHeader64` (payload responses) | base64 of IV (16) ‖ encrypted `iv ‖ key` (48) ‖ version (4, little-endian); its `iv` is the payload's |
| `fileMetadata.isEncrypted` | `true` |
| `appData.content` | `CBC(key, headerIv, node JSON)` |
| Payload `iv` | 16 random strong bytes, new at every upload of the bytes |
| Payload bytes | `CBC(key, payloadIv, plaintext)`; stored size `(n / 16 + 1) · 16` |
| `hash.storedHash` | hash of the payload bytes as stored (the ciphertext) |
| `hash.contentHash` | `CBC(key, payloadIv XOR 0xAA…, plaintext hash)`: 48 bytes |

### Writing
- **A new node**, file or folder, is always encrypted, with a new key header.
- **An upload reads its source twice.** The stored hash goes in the instructions, which come
  before the payload. So the first pass encrypts and hashes the ciphertext, and checks that the
  plaintext still has the hash the upload was planned with. The server can't tell from
  ciphertext that a file changed after it was hashed, so a mismatch fails the upload as a hash
  mismatch, and the engine plans again. The second pass encrypts again and sends: the same key
  and IV give the same bytes, with no temp copy. A file changed between the two passes fails
  the server's stored-hash check.
- **An update** reads the file's header first. It sends the file's own key with a new header
  IV, and the content encrypted with those: the server requires a strong key, keeps the file's
  key, and takes only the IV. New bytes get a new payload IV. The header read also finds a file
  deleted or changed since, before anything is uploaded.
- **The mode never changes.** The server refuses to change `isEncrypted` on an update. A file
  stored unencrypted by an earlier version of the client stays unencrypted: it is read and
  updated as before.

### Reading
- A header or feed row of an encrypted file: open the key header with the shared secret, then
  decrypt the content, and the content hash under its payload IV. A row that doesn't decrypt is
  skipped with its reason, like any unreadable row. So is an encrypted file whose content has
  no `size` (node schema v1 requires it for files): its stored size is padded, and taking it
  for the file's would make every download fail its hash.
- The key header is read only when the file is encrypted. On any other row (a tombstone, an
  unencrypted file) one that doesn't parse, null fields say, is ignored.
- Payload range `p0..p1` (plaintext offsets, `p1` at most the plaintext size): fetch
  ciphertext from the block before `p0`'s block, or from 0, where the payload IV is the block
  before, up to `p1` rounded up to a block. Decrypt and keep `p0..p1`. The padding block is never
  needed, and never checked: the content hash is.
- No header is read first: every ranged payload response says whether the payload is
  encrypted and carries its key (with the payload IV). The first request asks for the
  ciphertext extent above; an unencrypted payload (written before encryption) refuses a range
  past its end with 416, and the first request is made again to the plaintext end. Then 8 MiB
  ranges follow in the mode the first response gave.

## What it doesn't do
- **End-to-end:** the server can decrypt; see above.
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
(`hbsync-dev` test `throughput`, and `sealed.rs` test `throughput`):

| | unencrypted | encrypted |
|---|---|---|
| upload 1 GiB | 217 MB/s | 172 MB/s |
| download 1 GiB | 427 MB/s | 372 MB/s |
| create / read / update a small file | 2.5 / 6.7 / 5.0 ms | 2.5 / 6.1 / 3.7 ms |

(An unencrypted small read pays the 416 retry above.) The cipher alone: encrypting 970 MB/s,
decrypting 2.8 GB/s, where OpenSSL reaches 1.6 and 6.6 GB/s. The upload's first pass, about 1.3 s per GB here, is the cost that shows. It is small
next to any internet link. A faster AES (aws-lc-rs) would bring a C build on three OSes, and
isn't worth it until a measurement shows uploads bound by the CPU.
