Homebase Filesync: `hbsync` (the command line) and `hbsyncd` (the daemon), for

- Linux x86_64 (static: any distribution),
- macOS (one universal binary for Apple Silicon and Intel, macOS 11 or later),
- Windows x86_64.

Each archive holds both programs and a README. Keep them together, then run
`hbsync setup <identity>`. `SHA256SUMS` lists the archives' checksums.

The builds aren't signed yet: macOS needs `xattr -d com.apple.quarantine hbsync hbsyncd`
before the first run, and Windows SmartScreen may ask for confirmation.

How the daemon, its files and autostart work: `docs/spec/daemon.md`. How files are
encrypted: `docs/spec/encryption.md`.
