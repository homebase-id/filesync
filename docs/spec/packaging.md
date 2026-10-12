# Packaging: installers and updates

Filesync is packaged with [Conveyor](https://hydraulic.dev) 22.3, as chat-kmp's desktop app is
(`../chat-kmp/desktopApp/conveyor*.conf`). It is free for open-source projects: the repo
carries the Homebase.id License (AGPL-3.0) and the README says Conveyor packages it, as its
terms ask. Running it means accepting its licence (`CONVEYOR_AGREE_TO_LICENSE=1`; agreed
2026-10-11).

Config: `packaging/conveyor.conf`. The binaries are built by our own jobs (no Rust inside
Conveyor) and staged as `packaging/stage/{windows,mac,linux}-{amd64,aarch64}`; Conveyor
makes the "site" (packages, update feeds, a download page), signs it and uploads it.

## What each OS gets (spike, 2026-10-11, `conveyor-spike` branch, all checked on CI runners)

| | Package | Where the programs are | Starts at login | Updates |
|---|---|---|---|---|
| Windows | MSIX, plus Conveyor's small installer EXE; a zip without updates | `C:\Program Files\WindowsApps\HomebaseFilesync_<version>_x64__<id>\` | the package's startup task (`HomebaseFilesync.StartupTask`) | the `.appinstaller` feed; Conveyor's `updatecheck.exe` runs first at logon (`updates = aggressive`) |
| macOS | `Homebase Filesync.app` per architecture, in a zip; our own `.dmg` around it (`packaging/macos/make-dmg.sh`) | the tray in `Contents/MacOS`, `hbsyncd` and `hbsync` in `Contents/Helpers` | our launchd agent | Sparkle 2, injected by Conveyor (`SUFeedURL` = `appcast-<arch>.rss`) |
| Linux | `.deb` (package `homebase-filesync`) that adds its own apt source and key; a tarball without updates; an `.rpm` of the `.deb`'s files (Fedora's `rpmbuild`, `packaging/linux/`), no update repository yet | `/usr/lib/homebase/homebase-filesync/bin/`, `/usr/bin/homebase-filesync` and `/usr/bin/hbsync` symlinks, `id.homebase.filesync.desktop` | our XDG autostart entry | `apt upgrade` from the signed repo in the site |

**Verified on fresh runners:**
- **Linux:** the deb installs in `ubuntu:24.04`, both programs run (`--version` with the commit),
  the apt source is signed by the site key, `apt-get update && apt-get install
  --only-upgrade` takes 0.1.0 to 0.1.1 from the served site, and removing leaves nothing in
  `/usr/bin`.
- **macOS:** the app is signed (self-signed in the spike) and passes `codesign --verify
  --deep --strict`, helpers included; Sparkle's framework and `libconveyor.dylib` are in
  `Frameworks`; both programs run.
- **Windows:** the MSIX installs (the self-signed certificate trusted first, as Conveyor's
  `install.ps1` does), the tray starts from the package, and its log and lock land in the real
  `%LOCALAPPDATA%\Homebase Filesync` (the folders are excluded from MSIX's file virtualization,
  so the zip build and the CLI see the same files); it uninstalls cleanly.

## What the spike taught

- **The programs' names.** Conveyor's Windows package needs the main exe named after the app
  (`homebase-filesync.exe`), and on macOS it launches whichever program it finds in
  `Contents/MacOS` (there's no setting; with three it picked the daemon). So the tray program is
  `homebase-filesync` everywhere, and in the Mac app the daemon and the CLI are helpers in
  `Contents/Helpers` (`hbsync_app::install::Install::program` finds them).
- **Mac binaries need header room** for Conveyor's Sparkle loader and its signing: linked with
  `-Wl,-headerpad,0xFF`, all three.
- **One execution alias per MSIX app.** Conveyor's is the tray's, so `hbsync` isn't on the
  Windows PATH from the MSIX; the command line on Windows comes with the zip.
- **Conveyor's Mac defaults** ask for the camera and the microphone and say "Proprietary":
  turned off, and the licence set.
- **Its Linux desktop entry** had invalid categories (`GTK;Gnome;`): set to
  `Network;FileTransfer;`.
- **A nested `.appex` (File Provider, for online-only files later) loses its own entitlements**
  when Conveyor signs the app, and macOS doesn't register it. That needs Conveyor's custom
  signing hook (`app.mac.sign.scripts`) or a separate signing step when File Provider comes
  (roadmap Layer 7); it doesn't affect v0.1.
- **MSIX updates follow the `.appinstaller`.** A package added from the `.msix` file alone
  isn't tied to its feed; users install through Conveyor's installer, which uses the feed.
  Installing from the feed doesn't work on the CI runners (Windows Server: `0x80D05011` fetching
  the package from a local server), so **the Windows update is still to confirm on a real PC**;
  it's the same mechanism chat-kmp ships with. The packages start through Conveyor's `updatecheck.exe`, which
  looks at the update site first: a draft's package, newer than what is published, tries to
  update from the feed there and doesn't start (found with v0.2.0's signed ARM package), so the
  install test blocks the update site. The macOS update (Sparkle) likewise waits for a
  real Mac and a properly signed app.

## The release workflow

How the workflow makes, tests, signs and publishes them: `RELEASING.md`. With the update site on
GitHub releases (a flat folder of files), Conveyor puts the apt repository (`InRelease`,
`Packages`) and the `.deb` at the site's top, not in a `debian/` folder. The `.rpm` is made from
the `.deb`'s files (`packaging/linux/homebase-filesync.spec`), so `conveyor.conf` alone decides
the layout; a test in `hbsync-app` checks that it names the app, the tray, its icon and the
MSIX's unvirtualized folders as the code does.

## Still to do

- Try `homebase-filesync.exe` (Conveyor's installer, the release notes' Windows download) on a
  real PC: it is signed (v0.1.0-rc.6), but no runner has run it.
- Confirm the Windows and Mac updates on real machines with signed releases.
- An update repository for the `.rpm` (dnf), which needs a signing key of its own.
