Homebase Filesync

Syncs a folder on this computer with your Homebase identity.

  hbsync    the command line
  hbsyncd   the daemon that syncs

Keep the two in the same folder: hbsync starts the hbsyncd next to it, and so does the entry
it creates to start syncing at logon. Put them somewhere permanent before setting up (on
Linux and macOS, ~/.local/bin for example).

Start:

  hbsync setup <identity>      for example: hbsync setup frodo.dotyou.cloud

It opens your browser to approve this computer, makes ~/Homebase the synced folder (or the one
given with --folder), and starts syncing, also at every logon.

  hbsync status                what it is doing
  hbsync help                  everything else

These builds aren't signed yet:
- macOS: before the first run, clear the download quarantine:
    xattr -d com.apple.quarantine hbsync hbsyncd
- Windows: SmartScreen may warn about an unknown publisher: "More info", then "Run anyway".
