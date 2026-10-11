<p align="center"><img src="crates/hbsync-tray/assets/mark.svg" width="96" alt=""></p>

# Homebase Filesync

Keeps a folder on your computer in sync with your [Homebase](https://homebase.id) drive, on
Windows, macOS and Linux. Like Dropbox, but the files live on your own Homebase.

**[Download the latest release](https://github.com/homebase-id/filesync/releases)**: an MSIX
for Windows, a `.dmg` for the Mac, `.deb` and `.rpm` packages for Linux, for Intel and ARM.
They update themselves.

## Why it's nice

- **Written in Rust, from top to bottom.** Three small native programs (about 10 MB each, with
  no runtime, no Electron and no Java): a tray app, a per-user daemon (`hbsyncd`) that does the
  syncing, and a command line (`hbsync`). The Linux programs are fully static.
- **Barely there.** When idle, the daemon uses about 12–20 MB of memory and no CPU to speak
  of: it waits on the OS's file events and on your Homebase's notification socket instead of
  polling. A restart with nothing changed is idle again in about 55 ms. (A big first sync takes
  more for a while: about 160 MB at its peak for 10,000 files.)

  Side by side on one six-year-old Linux desktop (Intel i7-8700K, 2026-10-11, both idle):

  | | Memory (RSS) | Threads |
  |---|---|---|
  | Homebase Filesync (daemon + tray) | **33 MB** | 18 |
  | Dropbox 272.4 | 469 MB | 86 |

  That's about 14 times less memory. To be fair, Dropbox was watching a bigger folder (127,000
  files and 288 GB, against Filesync's 11,700 files and 1 GB). Filesync is built so its memory
  stays flat as a folder grows: it walks the tree a branch at a time and keeps the rest in its
  database, not in memory.
- **Every file is hashed in full with BLAKE3,** one of the fastest cryptographic hashes there
  is, while the file is read. The hash goes along with each file and is checked on the way
  back: a file that doesn't match is refused and downloaded again. The hash also tells real edits from touched
  timestamps, so unchanged files are never uploaded twice.
- **Encrypted on your computer.** Each file gets its own random AES key before it leaves your
  computer, and folder names are encrypted too. It uses Homebase's own format, so your other
  Homebase apps can open the files. The cipher handles about 1 GB/s encrypting and 2.8 GB/s
  decrypting, more than any internet link. (It isn't end-to-end: your own Homebase holds the
  keys. See [docs/spec/encryption.md](docs/spec/encryption.md).)
- **Fast,** even on a six-year-old desktop (Intel i7-8700K). With a test server on the same
  machine, it uploads 1 GiB at about 170 MB/s and downloads it at about 600 MB/s. An edit shows up on your other computer in about half a second. Big files
  are streamed in both directions in constant memory, and a broken transfer picks up where it
  stopped.
- **Careful with your data.** Losing nothing is the first rule:
  - When two computers edit the same file, both versions are kept as a conflict copy.
  - A sync that would delete many files at once waits for you to confirm.
  - Files are swapped into place atomically, never half-written.
  - The sync logic is a pure function, tested with property tests over hundreds of thousands
    of random multi-computer histories.
- **Made for each OS.** It uses the NTFS change journal on Windows, FSEvents on macOS and
  inotify on Linux. It handles names each OS treats differently: case, Unicode forms, and
  characters Windows doesn't allow. Your folder gets its own icon and a place in the sidebar.
- **Yours to tune.** `.hbsync/ignore` takes gitignore-style rules per computer, and well-known
  junk (`.DS_Store`, `Thumbs.db`, editor temp files) is left out on its own. `hbsync` gives you
  status, recent changes, pause and resume, and the logs from a terminal.

## Getting started

Install it, start **Homebase Filesync**, and choose **Log in to Homebase…** in its tray or
menu-bar menu. Your browser opens so you can approve this computer, and the folder
`~/Homebase` starts syncing (or pick another). From a terminal: `hbsync setup <your identity>`.

Your Homebase needs odin-core v0.1.950 or newer.

## For developers

Developer notes are in [CLAUDE.md](CLAUDE.md) and [docs/](docs/). Start with
[docs/foundations.md](docs/foundations.md) and [docs/roadmap.md](docs/roadmap.md). Releases:
[RELEASING.md](RELEASING.md).

```sh
cargo test --workspace
cargo run -p hbsync-tray
```

## Packaging

Installers and updates are made with [Conveyor](https://hydraulic.dev) by Hydraulic, free for
open-source projects.

## Licence

The [Homebase.id License](LICENSE): the GNU Affero General Public License v3
([LICENSE-GPL](LICENSE-GPL)), the same as the other Homebase apps.
