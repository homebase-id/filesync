Homebase Filesync keeps a folder on your computer in sync with your Homebase: a tray app (the
menu bar on macOS), a per-user daemon that syncs (`hbsyncd`) and a command line (`hbsync`).

## Install

| | Download | Updates |
|---|---|---|
| **Windows** 10/11 (x64) | `homebase-filesync-*.x64.msix` | by themselves (the package's update feed) |
| **macOS** 11+ | `Homebase-Filesync-*-mac-aarch64.dmg` (Apple Silicon) or `…-mac-amd64.dmg` (Intel): drag the app to Applications | by themselves (Sparkle) |
| **Ubuntu, Debian** (x86_64) | `homebase-filesync_*_amd64.deb`: `sudo apt install ./homebase-filesync_*_amd64.deb` | with `apt upgrade` (the package adds its repository) |
| **Fedora, openSUSE** (x86_64) | `homebase-filesync-*.x86_64.rpm`: `sudo dnf install ./homebase-filesync-*.rpm` | by hand, for now |
| **Other Linux, servers** | `homebase-filesync-*-linux-amd64.tar.gz` (static programs) | by hand |
| **Windows command line** | `homebase-filesync-*-windows-amd64.zip` | by hand |

Then start **Homebase Filesync** and choose **Log in to Homebase…**: a page opens in your browser
for your Homebase address and the folder (`~/Homebase` unless you change it), and your Homebase
asks you to approve this computer. Or from a terminal: `hbsync setup <identity>`.

On Linux the tray icon shows in KDE, Xfce, Cinnamon and most panels; GNOME needs the
AppIndicator extension (Ubuntu has it).

## Not signed yet

Until signing is in place (RELEASING.md):
- **Windows:** the package is self-signed: install `homebase-filesync.crt` into "Trusted People"
  first (an administrator PowerShell: `Import-Certificate -FilePath homebase-filesync.crt
  -CertStoreLocation Cert:\LocalMachine\TrustedPeople`).
- **macOS:** the first start is refused: open System Settings > Privacy & Security and choose
  "Open Anyway".

## Check a download

`SHA256SUMS` lists every file's checksum, and each file carries a build provenance attestation:
`gh attestation verify <file> --repo homebase-id/filesync`.

Packaged with [Conveyor](https://hydraulic.dev). How the daemon, its files and autostart work:
`docs/spec/daemon.md`; how files are encrypted: `docs/spec/encryption.md`.
