Homebase Drive keeps a folder on your computer in sync with your Homebase: a tray app (the
menu bar on macOS), a per-user daemon that syncs (`hbsyncd`) and a command line (`hbsync`).

## Install

| | Download | Updates |
|---|---|---|
| **Windows** 10/11 | `homebase-filesync.exe`: it installs the package for your PC (Intel/AMD or ARM) | by themselves (the package's update feed) |
| **Windows, by hand** | `homebase-filesync-*.x64.msix` (or `…arm64.msix`): double-click to install | none: installed this way, it doesn't update (use the `.exe`) |
| **macOS** 11+ | `Homebase-Filesync-*-mac-aarch64.dmg` (Apple Silicon) or `…-mac-amd64.dmg` (Intel): drag the app to Applications | by themselves (Sparkle) |
| **Ubuntu, Debian** | `homebase-filesync_*_amd64.deb` (or `_arm64`): `sudo apt install ./homebase-filesync_*.deb` | with `apt upgrade` (the package adds its repository) |
| **Fedora, openSUSE** | `homebase-filesync-*.x86_64.rpm` (or `.aarch64`): `sudo dnf install ./homebase-filesync-*.rpm` | by hand, for now |
| **Other Linux, servers** | `homebase-filesync-*-linux-amd64.tar.gz` (or `-aarch64`; static programs) | by hand |
| **Windows command line** | `homebase-filesync-*-windows-amd64.zip` (or `-aarch64`): the MSIX can't put `hbsync` on the PATH | by hand |

Then start **Homebase Drive** (on a Mac, **Homebase Filesync**) and choose **Log in to Homebase…**: a page opens in your browser
for your Homebase address and the folder (`~/Homebase` unless you change it), and your Homebase
asks you to approve this computer. Or from a terminal: `hbsync setup <identity>`.

On Linux the tray icon shows in KDE, Xfce, Cinnamon and most panels; GNOME needs the
AppIndicator extension (Ubuntu has it).

## Ubuntu, Debian: `apt update` refuses the repository once

The app was called Homebase Filesync before version 0.10.9, and so was its apt repository. On
a computer that installed it under that name, `apt update` stops with "changed its 'Origin'
value from 'Homebase Filesync' to 'Homebase Drive'", and the app isn't updated until the new
name is accepted, once:

```sh
sudo apt-get update --allow-releaseinfo-change-origin
```

If it then says "File has unexpected size … Mirror sync in progress?", apt kept the
repository's old index from the update it refused. Delete it and update again:

```sh
sudo sh -c 'rm -f /var/lib/apt/lists/partial/*filesync-release-production*'
sudo apt-get update --allow-releaseinfo-change-origin
```

## Check a download

`SHA256SUMS` lists every file's checksum, and each file carries a build provenance attestation:
`gh attestation verify <file> --repo homebase-id/filesync`.

Packaged with [Conveyor](https://hydraulic.dev). How the daemon, its files and autostart work:
`docs/spec/daemon.md`; how files are encrypted: `docs/spec/encryption.md`.
