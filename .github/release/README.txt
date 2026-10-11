Homebase Filesync

Syncs a folder on this computer with your Homebase identity.

  homebase-filesync   the app in your tray (menu bar on macOS): start this one
  hbsyncd             the daemon that syncs
  hbsync              the command line

Keep the three in the same folder: each starts the hbsyncd next to it, and the entry made to
start at logon points there. Put them somewhere permanent first (on Linux and macOS,
~/.local/bin for example).

Start the tray app and choose "Log in to Homebase…": a page opens in your browser to enter your
Homebase address and choose the folder (~/Homebase unless you change it), then your Homebase
asks you to approve this computer. From then on Filesync starts at every logon, and its menu
shows what it does: recently synced files, pause, problems.

Or from a terminal:

  hbsync setup <identity>      for example: hbsync setup frodo.dotyou.cloud
  hbsync status                what it is doing
  hbsync help                  everything else

On Linux the tray icon shows in KDE, Xfce, Cinnamon and most panels; GNOME needs the
AppIndicator extension (Ubuntu has it).

These builds aren't signed yet:
- macOS: before the first run, clear the download quarantine:
    xattr -d com.apple.quarantine hbsync hbsyncd homebase-filesync
- Windows: SmartScreen may warn about an unknown publisher: "More info", then "Run anyway".
