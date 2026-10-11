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

  Side by side on one six-year-old Linux desktop (Intel i7-8700K, October 2026), both started
  at boot and left alone:

  | | Memory (RSS) | Threads | CPU, first 10 minutes after boot | CPU, idle on average |
  |---|---|---|---|---|
  | Homebase Filesync (daemon + tray) | **33 MB** | 18 | **1.2 s** | **≈ 0** (0.03 s a minute) |
  | Dropbox 272.4 | 469 MB | 86 | 18.9 s | 2.7% of a core, all the time (over 8 days) |

  That's about 14 times less memory and 15 times less CPU at start-up. When idle, Filesync
  sleeps until something changes, so a laptop's battery barely notices it.

  To be fair, Dropbox was watching a bigger folder: 127,000 files and 288 GB, against
  Filesync's 11,700 files and 1 GB. Filesync's memory stays flat as a folder grows. It walks
  the tree a branch at a time and keeps the rest in its database. Pointed at that same Dropbox
  folder (137,000 entries), Filesync's start-up check took 4.7 s and left it at 13 MB.
- **Every file is hashed in full with BLAKE3,** one of the fastest cryptographic hashes there
  is, while the file is read. The hash goes along with each file and is checked on the way
  back: a file that doesn't match is refused and downloaded again. The hash also tells real
  edits from touched timestamps, so unchanged files are never uploaded twice.
- **Zero-knowledge encrypted at rest.** Each file gets its own random AES key before it leaves
  your computer, and names, sizes and times are encrypted too. What lies on your Homebase's
  disk and in its backups is ciphertext. The server doesn't hold the key: only you and the
  devices you approved do. It unlocks a file's key for a moment, in memory, only while
  serving a request from one of them. It uses
  Homebase's own format, so your other Homebase apps can open the files. The cipher handles
  about 1 GB/s encrypting and 2.8 GB/s decrypting, more than any internet link. Dropbox, by
  default, encrypts files once they reach its servers, with keys Dropbox holds. With Filesync,
  the files are encrypted before they leave your computer, and the keys stay with your own
  Homebase, not with a company. More: [docs/spec/encryption.md](docs/spec/encryption.md).
- **Fast,** even on a six-year-old desktop (Intel i7-8700K). With a test server on the same
  machine, it uploads 1 GiB at about 170 MB/s and downloads it at about 600 MB/s. An edit
  shows up on your other computer in about half a second. Big files are streamed in both
  directions in constant memory, and a broken transfer picks up where it stopped.
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
