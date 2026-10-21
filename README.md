<p align="center"><img src="crates/hbsync-tray/assets/app-icon.svg" width="96" alt=""></p>

# Homebase Filesync

Keeps a folder on your computer in sync with your [Homebase](https://homebase.id) drive, on
Windows, macOS and Linux. Like Dropbox, but the files live on your own Homebase.

## Getting started

1. **Install** it from the [latest release](https://github.com/homebase-id/filesync/releases/latest):
   - **Windows** 10/11: `homebase-filesync.exe`. It installs the package for your PC and keeps
     it up to date.
   - **macOS** 11+: `Homebase-Filesync-mac-aarch64.dmg` (Apple Silicon) or `…-mac-amd64.dmg`
     (Intel); drag the app to Applications. It updates itself.
   - **Ubuntu, Debian**: `sudo apt install ./homebase-filesync_amd64.deb` (or `_arm64`).
     Updates come with `apt upgrade` (the package adds its repository).
   - **Fedora, openSUSE**: `sudo dnf install ./homebase-filesync-*.x86_64.rpm` (or
     `.aarch64`; `sudo zypper install` on openSUSE). Updates by hand, for now.
   - **Other Linux, servers**: `homebase-filesync-*-linux-amd64.tar.gz` (or `-aarch64`), static
     programs. Updates by hand.
2. **Start Homebase Filesync.** Installing doesn't start it, and it has no window, only an icon:
   - **Windows**: open **Homebase Filesync** from the Start menu. Its icon is in the
     notification area at the right of the taskbar (behind the **^** arrow at first).
   - **macOS**: open **Homebase Filesync** from Applications. Its icon is in the menu bar.
   - **Linux**: open **Homebase Filesync** from the app menu (Activities on Ubuntu), or run
     `setsid homebase-filesync` in a terminal (from the tarball:
     `setsid homebase-filesync-*/bin/homebase-filesync`). Its icon is in the top bar or panel;
     GNOME needs the AppIndicator extension, which Ubuntu has.
3. **Choose Log in to Homebase…** from that icon. Your browser opens so you can approve this
   computer, and the folder `~/Homebase` starts syncing (or pick another). From then on it
   starts by itself when you log in.

From a terminal instead of steps 2 and 3: `hbsync setup <your identity>`.

After an update on Linux, the menu offers **Restart to finish the update**.

Your Homebase needs odin-core v0.1.962 or newer.

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

  Syncing 2,041 files (387 MB) on the same desktop (October 2026), as seen on the computer
  that made the change:

  | | Filesync 0.7 | Dropbox 274 |
  |---|---|---|
  | Encrypted before it leaves your computer | **Yes** | No |
  | Memory while idle, before / after the upload | **28 / 43 MB** | 134 / 361 MB |
  | CPU while idle | **≈ 0** (0.01 s per 10 minutes) | 0.25% of a core (1.4–1.8 s per 10 minutes) |
  | CPU to upload them | **2.5 s** ¹ | 38.7 s |
  | CPU to delete them | **0.4 s** | 4.8 s |
  | Time to upload them | TBD | 87 s |
  | Time to delete them | TBD | 57 s |
  | Memory at its peak while uploading | **43 MB** | 466 MB |
  | Read back from disk to upload | **0 MB** | 134 MB |
  | Written to disk while uploading | **74 MB** | 113 MB |
  | Sent while uploading | **406 MB** | 445 MB |
  | Written to disk to delete them all | **2.5 MB** | 16.7 MB |
  | Idle: disk writes per 10 minutes | **0** | 0.3–2.7 MB |
  | Idle: network traffic per 10 minutes | **≈ 7 KB** (keep-alive) | 0.4–2.7 MB |

  ¹ To a Homebase on the same machine; Dropbox went over Wi-Fi to its servers, which costs CPU
  too (Filesync 0.6 used 7–10 s over that Wi-Fi). Times will follow from a like-for-like run.

  Deleting those 2,041 files takes 11 requests to your Homebase (0.6 needed 4,148), and an
  idle daemon wakes about once every 12 seconds.
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

  Privacy, from each vendor's own terms (October 2026):

  | | Homebase Filesync | Dropbox | Google Drive | OneDrive |
  |---|---|---|---|---|
  | Encrypted before it leaves your computer | ✅ Yes | ❌ No[^e2e] | ❌ No[^e2e] | ❌ No |
  | Vendor holds the key to your files | ✅ No[^key] | ❌ Yes | ❌ Yes | ❌ Yes |
  | Vendor's terms say it scans your content | ✅ No | ❌ Yes[^dropbox] | ❌ Yes[^google] | ❌ Yes[^microsoft] |
  | Vendor's terms let it modify your content | ✅ No | ✅ No | ❌ Yes[^google] | ✅ No[^xbox] |
  | Vendor's terms let it remove your content | ✅ No | ❌ Yes[^dropbox] | ❌ Yes[^google] | ❌ Yes[^microsoft] |
  | Vendor's terms let it remove content it judges misleading | ✅ No | ✅ No[^dropbox-misleading] | ❌ Yes[^google-misleading] | ⚠️ If intentionally deceptive[^microsoft-misleading] |
- **Fast,** even on a six-year-old desktop (Intel i7-8700K). With a test server on the same
  machine, it uploads 1 GiB at about 220 MB/s and downloads it at about 600 MB/s. An edit
  shows up on your other computer in about half a second. Big files are streamed in both
  directions in constant memory, and a broken transfer picks up where it stopped.
- **Careful with your data.** Losing nothing is the first rule:
  - When two computers edit the same file, both versions are kept as a conflict copy.
  - A sync that would delete many files at once waits for you to confirm.
  - Whatever you delete, on any of your computers, waits in **Recently deleted** on your
    Homebase for 30 days. Restore it from the tray menu or with `hbsync restore`, and it comes
    back on every computer, a folder with everything that was in it.
  - Files are swapped into place atomically, never half-written.
  - The sync logic is a pure function, tested with property tests over hundreds of thousands
    of random multi-computer histories.
- **Made for each OS.** It uses the NTFS change journal on Windows, FSEvents on macOS and
  inotify on Linux. It handles names each OS treats differently: case, Unicode forms, and
  characters Windows doesn't allow. Your folder gets its own icon and a place in the sidebar,
  on Windows at the top of Explorer's, beside OneDrive and Dropbox.
  Finder tags and their colours sync between Macs and with KDE on Linux. A Windows PC
  can't show them, but keeps them intact. Creation dates are kept too, even when an editor saves by
  replacing the file.
- **Yours to tune.** `.hbsync/ignore` takes gitignore-style rules per computer, and well-known
  junk (`.DS_Store`, `Thumbs.db`, editor temp files) is left out on its own. `hbsync` gives you
  status, recent changes, pause and resume, Recently deleted, and the logs from a terminal.

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

[^e2e]: Only on business tiers: Dropbox's encrypted team folders (Business Plus and up,
    [help](https://help.dropbox.com/files-folders/sort-preview/dropbox-vault)) and Google's
    client-side encryption (Workspace Enterprise Plus and Education,
    [help](https://support.google.com/a/answer/10745596)).
[^key]: Your Homebase unlocks a file's key in memory only while serving a request from one of
    your devices; at rest, the server and its backups hold ciphertext only
    ([docs/spec/encryption.md](docs/spec/encryption.md)).
[^dropbox]: [Dropbox Terms](https://www.dropbox.com/terms): "Dropbox accesses, stores, and scans
    Your Stuff"; "We reserve the right to delete or disable content alleged to be infringing".
[^google]: [Google Terms](https://policies.google.com/terms): "automated systems and algorithms
    to analyze your content"; the license lets Google "modify your content, such as reformatting
    or translating it"; "we reserve the right to take down some or all of that content".
[^microsoft]: [Microsoft Services Agreement](https://www.microsoft.com/en-us/servicesagreement):
    "automated systems and humans to review content"; "we reserve the right to delete or disable
    access to Your Content".
[^xbox]: Except content on Xbox services, which the agreement lets Microsoft modify.
[^dropbox-misleading]: [Dropbox Acceptable Use Policy](https://www.dropbox.com/acceptable_use):
    misleading content only where it breaks the law.
[^google-misleading]: [Google Drive program policies](https://support.google.com/docs/answer/148505),
    "Misleading Content": "Do not distribute content that deceives, misleads, or confuses users",
    covering civic processes, harmful conspiracy theories, harmful health practices and
    manipulated media; Google may restrict access to such content or remove it. (Restrict or
    remove: the policies don't say Google edits it.)
[^microsoft-misleading]: Microsoft's Code of Conduct forbids "generating or sharing content that
    is intentionally deceptive"; content that breaks it may be blocked or removed.
