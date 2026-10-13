# The daemon, its config and its control socket

How the product runs on a computer: one daemon per OS user (`hbsyncd`), shown and controlled
by the tray app (crate `hbsync-tray`, program `homebase-filesync`) and the command line (`hbsync`). Code: `crates/hbsync-app`
(library), `crates/hbsync` (the daemon and the command line), `crates/hbsync-tray`.

## Scope

- One identity and one folder per OS user. The config file keeps a list, so more can come
  later without a migration; the daemon refuses more than one.
- The daemon runs as the user, never as a service: it needs the user's keychain and writes the
  user's files.

## Files

`HBSYNC_HOME` puts all of them in one folder (tests, a second instance while developing).
Otherwise:

| | Linux | macOS | Windows |
|---|---|---|---|
| config, credentials file | `$XDG_CONFIG_HOME/hbsync` (`~/.config/hbsync`) | `~/Library/Application Support/Homebase Filesync` | `%APPDATA%\Homebase Filesync` |
| locks, pids, pipe token, the tray's icon files | `$XDG_STATE_HOME/hbsync` (`~/.local/state/hbsync`) | same as config | `%LOCALAPPDATA%\Homebase Filesync` |
| logs | `<state>/logs` | `~/Library/Logs/Homebase Filesync` | `<state>\logs` |
| control socket | `$XDG_RUNTIME_DIR/hbsync`, else `<state>` | same as config | a named pipe |

The sync folder keeps its own state in `<folder>/.hbsync` (state DB, temp files, trash, ignore
rules), hidden on Windows too (the hidden attribute, set at every start). Its state DB records the identity it syncs with (`account.identity`): a folder synced
with one identity is never synced with another.

## `config.toml`

```toml
device_name = "Anna's laptop"   # optional; the computer's name otherwise
paused = false                  # hbsync pause / resume
keep_deleted_days = 30          # Recently deleted: removed items are deleted for good after this

[mass_delete]                   # sync semantics, the mass-delete guard
min_files = 50
fraction = 0.25

[[roots]]
identity = "anna.example.com"
folder = "/home/anna/Homebase"
```

Unknown keys are an error. Writes replace the file in one step.

## Credentials

In the OS keychain, service `Homebase Filesync`, account = identity, value = the credentials
JSON (identity, client auth token, shared secret, circle id):

- macOS: Keychain Services, the login keychain;
- Windows: Credential Manager;
- Linux: the Secret Service (GNOME Keyring, KWallet) over D-Bus.

When no keychain answers (a Linux without a Secret Service, say), the credentials go to
`<config>/credentials/<identity>.json`, mode 0600 in a 0700 folder (on Windows the profile
folder is the user's only), and `hbsync status` says so. Credentials found in the file while the
keychain works are moved into it. `HBSYNC_CREDENTIALS=file` uses the file only. The file is
replaced in one step (a crash leaves the old one or the new one).

A keychain that is there but can't be read (locked at logon, before the session unlocks it) is
not "not set up": the daemon waits for it (status `failing`, with the reason). `hbsync logout`
says so when it couldn't reach the keychain to remove the credentials there.

## The daemon

1. Takes the lock (`<state>/hbsyncd.lock`, an exclusive lock the OS drops when the process
   ends, with the pid in `hbsyncd.pid`): a second daemon says which process runs and exits.
2. Reads the config. Not set up: it says so and exits.
3. Answers on the control socket from now on.
4. Reads the credentials, waiting (backoff 5 s doubling to 5 min) while the keychain can't be
   read. None: it says so and exits.
5. Connects to the identity, retrying with the same backoff while it can't be reached
   (`offline`); a refusal (the device revoked) shows as `denied` and is asked again every 5 min.
   Meanwhile `pause` and `resume` work (kept in the config); other requests are refused.
6. Runs the sync loop (`Runner`) until a stop request, SIGTERM or SIGINT.

Exit status: 0 when stopped or when there is nothing to do (not set up, already running), 1 on
a failure (service managers restart it then).

### The sync loop

- Syncs when the folder changes (live watcher), when the identity announces a change on the
  drive (notification socket; once its announcements have stopped for 1 s, at most 10 s
  after the first, so a burst of deletes from another device reaches the mass-delete guard
  whole), at least once a minute, and on request. After a failure it
  waits 5 s, doubling to 5 min.
- **Pause** stops a running sync between operations and before the next transfer (transfers
  in flight finish); nothing syncs until resume. The watcher keeps recording changes. A pause
  survives a restart (`paused` in the config).
- **Sleep:** a wall-clock jump of more than 30 s across a 250 ms wait means the machine slept.
  The loop syncs at once, forgets its backoff and reconnects the notification socket.
- **Network changes:** the reconnect backoff (at most 30 s) and the poll; no OS hooks yet.
- **Ignore rules:** `.hbsync/ignore` is checked for an edit at every poll and on a sync
  request; an edit restarts the engine with the new rules (sync semantics §10).
- **The folder gone** (moved, deleted, a disk not mounted: its `.hbsync` missing): nothing
  syncs, the status says why, and the loop waits for it to come back. It never creates a new
  `.hbsync`: only `hbsync setup` does.
- **Held deletes:** the status shows how many (deleted here; deleted elsewhere), some paths,
  and a token naming exactly these deletes. A confirmation carries the token back and lets
  exactly them through; when the held deletes changed since, it is refused (sync semantics,
  the mass-delete guard).
- **Notes:** each conflict and failure is noted once; one that comes back sync after sync (a
  file that can't be read) isn't noted again until it has gone away. A row on the drive the
  client can't read (not encrypted, another key, no content hash) is noted too.
- **Progress** while a sync runs: its files (transfers, moves and deletes) and the bytes of its
  transfers, done and to do.
- **Pause for a while:** a pause can carry its end (Unix seconds), kept in the config with the
  pause; the runner resumes by itself then, also after a restart.
- **Recently synced:** each upload, download, move and delete, with its path, size and time;
  more than 10 of one kind in one folder in one sync become one entry for the folder. The
  newest 500 are kept in the folder's state DB (table `synced`), so the list survives a restart.

- **Recently deleted:** the folder's removed nodes (node schema, "Recently deleted"), kept in
  its state DB (table `removed`), listed newest first with their path. Restore and Empty go
  through the runner, at its next sync (kept in memory until then). Empty takes what the user
  was shown: the items named, or everything removed until the newest one listed. Once a day
  (the time is kept in the state DB) the runner deletes for good what was removed more than
  `keep_deleted_days` ago (a folder's delete as a whole), and empties `.hbsync/trash` of
  entries that old, from those made since the purge began (older ones, from before Recently
  deleted, may be the only copy and stay).

### States

`starting` (catching up), `syncing`, `idle`, `paused`, `offline` (can't reach the identity),
`failing` (another failure, or the folder gone), `denied` (the identity refused this device:
`hbsync setup` again), `stopped`.

## Control socket

One request line, one reply line (JSON), then the connection closes. Requests carry the protocol
version (now 3); a daemon refuses another one with the reason (after an update: restart it).

```json
{"protocol":3,"ask":"status"}
{"protocol":3,"ask":{"control":"sync_now"}}            // resume, stop
{"protocol":3,"ask":{"control":{"pause":{"until":1800000000}}}}   // "until":null: until resumed
{"protocol":3,"ask":{"control":{"confirm_deletes":{"token":"<held_deletes.token>"}}}}
{"protocol":3,"ask":{"control":{"restore":{"nodes":["<node id>"]}}}}
{"protocol":3,"ask":{"control":{"empty_deleted":{"nodes":["<node id>"]}}}}
{"protocol":3,"ask":{"control":{"empty_deleted_until":{"at_ms":1800000000000}}}}  // all removed until then
{"protocol":3,"ask":{"recent":{"limit":10}}}
{"protocol":3,"ask":{"deleted":{"limit":100}}}
```

Replies: `{"status":{...}}` (with the daemon's `version`, `progress`, `paused_until`, and the
counts `notes_added` and `synced_added` that tell a client what is new),
`{"recent":[...]}` and `{"deleted":[...]}` (newest first), `"done"`, `{"refused":"why"}`.

Only the user can connect:

- Unix: `hbsyncd.sock` in a 0700 folder. A path too long for a socket address (over 100 bytes,
  a deep `HBSYNC_HOME`) moves to `<temp>/hbsync-<hash of the folder>/`, which both sides check
  is a 0700 folder of this user.
- Windows: the named pipe `hbsync-<token>`, with a protected DACL granting only its owner and
  SYSTEM. The token is random, kept in `<state>/hbsyncd.pipe`, so another user can't squat the
  name. Named pipes have no read timeout.

## The tray app

`homebase-filesync` (the tray) is what people see, and what starts at logon when it is installed beside the
command line. It keeps the daemon running while a folder is set up (starting it when it
doesn't answer, at most every 30 s after a failed start, and once more when the daemon runs
another version), and **Quit stops the daemon too**. One tray per user (`homebase-filesync.lock`).

- **Linux:** a StatusNotifierItem over D-Bus (pure Rust, no GTK), as KDE, Xfce, Cinnamon and
  most panels show them; GNOME needs the AppIndicator extension. Windows and macOS: tao's event
  loop on the main thread; on macOS a menu bar item without a Dock icon, the glyph as a
  template image.
- **Icon:** the Homebase mark with a dot: green up to date, blue syncing, grey paused or
  offline or not set up, magenta when something needs the user (a failure, held deletes, a
  refused login).
- **Menu:** version, identity, a status line (with progress while syncing), Open Homebase
  folder, Recently synced (the last 10; clicking one shows it in the file manager), Problems
  (the status notes), Review N deletions…, Recently deleted…, Log in again… (refused), Pause syncing (1 h, 8 h,
  24 h, until resumed) or Resume, Sync now, Start at login, Open logs, Log out, Quit.
- **Pages:** logging in (the Homebase address, the folder, `~/Homebase` unless chosen with the
  OS's picker, merging a folder with files only when ticked), reviewing held deletes and
  Recently deleted (the topmost items: path, size, date, how many items their restore brings
  back, Restore; Empty now, after a confirmation) are
  pages the tray serves on `127.0.0.1`, on a random port, under a random path, and opens in the
  browser; the identity's approval returns there.
- **Notifications:** a new problem note, held deletes, a refused login; each kind at most once a
  minute.
- **The folder:** once per folder the tray gives it the Homebase icon (`desktop.ini` on
  Windows, Finder's custom icon on macOS, `.directory` and GIO metadata on Linux) and a
  shortcut (Quick access; GTK bookmarks and KDE places; Finder's sidebar has no public
  interface). Log out takes them off again.

## Autostart

What starts at logon is the tray when `homebase-filesync` is installed with the command line (beside it; in a Mac app the tray is in `Contents/MacOS` and `hbsyncd` and `hbsync` in `Contents/Helpers`, `hbsync_app::install`) (it starts the
daemon), else the daemon:

| | how | restarts |
|---|---|---|
| macOS | launchd agent `~/Library/LaunchAgents/id.homebase.filesync.plist` (the tray's: `id.homebase.filesync.tray.plist`), `launchctl bootstrap gui/<uid>` | after a crash (`KeepAlive`/`SuccessfulExit` = false) |
| Linux | the daemon: systemd user unit `~/.config/systemd/user/hbsync.service`, enabled; the tray: `~/.config/autostart/id.homebase.filesync.desktop` (a GUI needs the session) | the unit: `Restart=on-failure` |
| Windows | `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`, value `Homebase Filesync` | no |
| Windows, MSIX package | the package's startup task `HomebaseFilesync.StartupTask` (Conveyor's `start-on-login`); the user can also turn it off in Task Manager | no |

Windows uses the `Run` key rather than a logon task: creating an `ONLOGON` task needs an
administrator **[inferred, not tried]**. `hbsyncd.exe` has no console window.
`HBSYNC_HOME`, `HBSYNC_CREDENTIALS`, `XDG_CONFIG_HOME` and `XDG_STATE_HOME` are passed on in
the plist and the unit (not on Windows), so the daemon uses the same files as the command line
that set it up. The unit goes where the user's systemd looks by default, whatever the shell's
`XDG_CONFIG_HOME`. An install the service manager refuses leaves no file behind.

How it was installed (`hbsync_app::install`) changes three things: an MSIX package uses its
startup task (the `Run` key would be virtualized there, and the package's folder changes with
each version) and names its notifications by its AppUserModelID; a Mac app run from its disk
image or App Translocation doesn't turn on starting at logon (the path wouldn't last) and says
to move it to Applications; under a Linux package (`/usr`) the tray writes no launcher of its
own. On Linux a tray whose program was replaced on disk (`apt upgrade`, `cargo install`) offers
"Restart to finish the update", which runs the new program in the old one's place (`exec`;
the instance lock's file closes with it) and restarts a daemon of another build. A package's
programs are started by their links in `/usr/bin`, which last across package versions.

On Linux the tray also lists itself among the desktop's applications each time it starts
(unless `HBSYNC_HOME` is set): the same entry in `~/.local/share/applications/` and its icon in
`~/.local/share/icons/hicolor/256x256/apps/homebase-filesync.png`, rewritten only when they
changed; an autostart entry is brought up to date with it. Started from there or at logon, the session gives it a group of its own (System
Monitor shows Homebase Filesync, the daemon below it); started from a terminal, it belongs to
the terminal. The launcher stays after Log out, as the way to log in again. Elsewhere the
installer will do this (not built yet).

`hbsync setup` turns autostart on, `logout` and `unlink` turn it off, `hbsync autostart on|off`
by hand. With the daemon starting at logon, `hbsync start` goes through launchd or systemd so
that they supervise it. With `HBSYNC_HOME` set, only `hbsync autostart` touches the user's logon entry.

## Command line

| | |
|---|---|
| `hbsync setup <identity> [--folder P] [--existing] [--browser CMD] [--no-start] [--no-autostart]` | authorizes this computer (the owner approves in the browser) unless it already is and the identity still accepts it, saving the credentials at once, prepares the folder (default `~/Homebase`; it must be new, empty, already this identity's, or `--existing` to merge its files with the drive), saves the config, turns autostart on, (re)starts the daemon |
| `hbsync status [--json]` | what the daemon does; exit status 3 when it isn't running |
| `hbsync sync`, `pause [--for 1h]`, `resume` | a pause with `--for` ends by itself |
| `hbsync recent [-n N]` | what was synced lately, newest first |
| `hbsync confirm-deletes [--yes]` | shows the held deletes and lets them through |
| `hbsync start`, `stop` | |
| `hbsync autostart [on\|off]` | |
| `hbsync logout` | stops the daemon, forgets the credentials, autostart off; the folder stays |
| `hbsync unlink` | stops the daemon, removes the folder from the config, autostart off; its files and `.hbsync` stay, so setting it up again resumes |
| `hbsync deleted [-n N]` | Recently deleted, newest first, with node ids |
| `hbsync restore <path or node id>…` | restores removed items (a folder with what was deleted with it) |
| `hbsync empty-deleted [--yes]` | deletes everything in Recently deleted for good, after asking |
| `hbsync log` | the log folder (a daily file, a week kept) |

## Not yet

- several folders or identities;
- packaging, signing, auto-update;
- OS network-change notifications;
- revoking the device on the identity at logout (no endpoint known yet);
- structured per-item status and a diagnostics bundle (Observability).
