# Tray assets

The Filesync logo, chosen 2026-10-14: a cloud with the Homebase dot (the one over the "i" in the
Homebase mark) in the upper right; where it is shown live, the dot tells the sync status. The
full set, its rules and the sketches are in the marketing folder
(`Dropbox/4-Projects/1-Odin/2-Marketing/Filesync/README.md`); these are the ones the app uses.

| File | What | Where |
|---|---|---|
| `app-icon.svg`, `app-icon-256.png` | the cloud on the Homebase tile, a white dot | the app icon (Conveyor: installers, Start menu, Dock), the Linux launcher, the tray's pages |
| `tray-{synced,syncing,paused,problem}.svg`, `-64.png` | the tile with the dot cut into the cloud, in the state's colour: green `#00966d`, cyan `#00ebeb`, grey `#9aa3b5`, magenta `#eb00eb` | the tray on Windows and Linux |
| `menu-bar.svg`, `menu-bar-36.png` | the cloud and dot in one colour | the macOS menu bar, a template image |
| `tray-problem-36.png` | the problem tile, small | the macOS menu bar when something needs the user |
| `folder.svg`, `folder-256.png` | the open cloud, no tile | the synced folder's icon on Linux (Dolphin, GNOME) and macOS |
| `folder-windows.svg`, `homebase.ico` | the tile with a white dot, 16 to 256 px | the synced folder on Windows: `desktop.ini` and Explorer's navigation pane |

The PNGs were rendered from the SVGs with resvg 0.48 (`resvg::render` scaled to the size); the
ICO holds 16, 24, 32, 48, 64, 128 and 256 px, made with Pillow from the 256 px PNG. Render them
again when an SVG changes.
