# Fixtures

Example file headers in the shape Homebase returns them (only the fields the sync client reads).
`file-node.json`'s payload is the 1025-byte golden-vector input (byte *i* = `i % 251`), so its
`storedHash` is the BLAKE3 vector from `../odin-core/docs/payload-hashes.md`. `removed-node.json` is
the same file in Recently deleted (`archivalStatus` 2, `rm`), and `tombstone.json` the same file
deleted for good.
