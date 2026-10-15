# Fixtures

Example file headers in the shape Homebase returns them (only the fields the sync client reads).
`file-node.json`'s payload is the 1025-byte golden-vector input (byte *i* = `i % 251`), so its
`storedHash` is the BLAKE3 vector from `../odin-core/docs/payload-hashes.md`. `removed-node.json` is
the same file in Recently deleted as a writer from before 2026-10-15 left it (`archivalStatus` 2,
with an `rm`, which readers drop now), and `tombstone.json` the same file deleted for good.
