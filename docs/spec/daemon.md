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
| locks, pids, pipe token, the tray's icon files, the state DBs | `$XDG_STATE_HOME/hbsync` (`~/.local/state/hbsync`) | same as config | `%LOCALAPPDATA%\Homebase Filesync` |
| logs | `<state>/logs` | `~/Library/Logs/Homebase Filesync` | `<state>\logs` |
| control socket | `$XDG_RUNTIME_DIR/hbsync`, else `<state>` | same as config | a named pipe |

Each account has one state DB, `<state>/accounts/<identity>/state.db` (the identity in lower
case, a port's `:` as `_`), for every folder it syncs on this computer: the drive's side (its
mirror, Recently deleted) once, each folder's own rows (what it has, what was agreed) keyed by
the folder. It records its identity (`account.identity`).

A sync folder keeps the rest in `<folder>/.hbsync` (temp files and trash, on the folder's
disk for atomic renames; the ignore rules), hidden on Windows too (the hidden attribute, set
at every start), and its marker `.hbsync/folder`: the identity and the folder's id in that DB.
A folder is synced only through its marker: without one it is gone (or not mounted), with
another identity's it is refused, and one the DB doesn't know is set up again. A folder of an
older Filesync (its DB in `.hbsync`, no marker) is set up again as a folder with files: what it
has is adopted, nothing is copied twice.

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
# role = "documents"            # a role folder (sync semantics §12); none for the main folder
```

Unknown keys are an error. Writes replace the file in one step. The daemon writes it as it
adds and removes folders (and keeps a pause in it); `hbsync setup` writes it only when no
daemon runs.

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
4. Starts each account (identity) of the config on a thread of its own
   (`hbsync-app/src/daemon/account.rs`), which:
   1. reads the credentials, waiting (backoff 5 s doubling to 5 min) while the keychain can't
      be read. None: it says so and the daemon exits;
   2. connects to the identity, retrying with the same backoff while it can't be reached
      (`offline`); a refusal (the device revoked) shows as `denied` and is asked again every
      5 min. Meanwhile `pause` and `resume` work (kept in the config); other requests are
      refused;
   3. opens the account's `Drive` and the notification socket, and runs a sync loop
      (`Runner`) for each of its folders until a stop request, SIGTERM or SIGINT. It passes the
      notifications on to every folder's loop, reconnects the socket after a sleep, runs
      Recently deleted's requests and its daily purge (not while paused).

   One folder's loop failing is started again after a pause (from 5 s, doubling up to 5 min,
   reset after an hour without failing), the folder showing `failing` with its error
   meanwhile; the account's other folders sync on. A loop that can't start stops its account, and an account
   failing stops the daemon (it is restarted whole).

Exit status: 0 when stopped or when there is nothing to do (not set up, already running), 1 on
a failure (service managers restart it then).

### The sync loop

- Syncs when the folder changes (live watcher), when another folder's sync changed this one's
  part of the drive, when the identity announces a change on the
  drive (notification socket; once its announcements have stopped for 1 s, at most 10 s
  after the first, so a burst of deletes from another device reaches the mass-delete guard
  whole), when the socket connects (again), and on request. A sync that left work unfinished
  (the round limit, or operations skipped or failed; never deletes held for a confirmation)
  is tried again after 5 s, doubling to 5 min while the tries do nothing and starting over
  after one that did some, without holding back other syncs. After a failure it waits 5 s,
  doubling to 5 min. When the socket connects again, a folder waiting for either goes at
  once.
- **No poll:** nothing syncs on a timer while the notification socket is connected. The
  drive is read from the feed's cursor, so what wasn't announced (the socket was down, a
  notification got lost) arrives at the next (re)connect, when the account pulls as at start
  (which wakes only the folders it concerns), or with the next announced change. While the
  socket isn't connected (not yet, not any more, or never: a proxy blocking it, a refusal,
  its thread not starting), the account reads the drive every 5 min instead. Nothing is
  pulled while paused; the account's pulls run on a thread of their own. A dead socket is
  noticed within 75 s (no frame, not even a pong to the ping every 30 s) and reconnected
  (backoff 1 s, doubling to 30 s); a refused subscription the same way, but not sooner than
  every 5 min.
- **Pause** stops a running sync between operations and before the next transfer (transfers
  in flight finish); nothing syncs until resume. The watcher keeps recording changes. A pause
  survives a restart (`paused` in the config).
- **Sleep:** a wall-clock jump of more than 30 s across a 250 ms wait means the machine slept.
  The loop syncs at once and forgets its backoff; the account reconnects the notification
  socket.
- **Network changes:** the reconnect backoff (at most 30 s); no OS hooks yet.
- **Ignore rules:** `.hbsync/ignore` is checked for an edit once a minute and on a sync
  request; an edit restarts the engine with the new rules (sync semantics §10).
- **The folder gone** (moved, deleted, a disk not mounted: its marker missing): nothing
  syncs, the status says why, and the loop waits for it to come back. It never creates a
  marker: only `hbsync setup` does.
- **Held deletes:** the status shows how many (deleted here; deleted elsewhere), some paths,
  and a token naming exactly these deletes. A confirmation carries the token back and lets
  exactly them through; when the held deletes changed since, it is refused (sync semantics,
  the mass-delete guard).
- **Notes:** each conflict and failure is noted once; one that comes back sync after sync (a
  file that can't be read) isn't noted again until it has gone away. An operation left for the
  next plan is noted only when this device can't do it (a name or path it can't hold, access
  denied, the drive refusing it); one that went stale since the plan (the item changed or went)
  is planned again quietly, and told only when it keeps being skipped for ten minutes
  (something holds it back that the plan can't see: a file written all the time, one that can't
  be read). One waiting for another operation (a folder's base for its children's) isn't told:
  that one says why. A full drive (Homebase's 507) is one note, "Homebase is out of storage":
  every folder of the account holds its uploads for ten minutes (or until Sync now) and does
  the rest (downloads, moves, deletes); then one upload tries again. A row on the drive the
  client can't read (not encrypted, another key, no content hash) is noted too.
- **Progress** while a sync runs: its files (transfers, moves and deletes) and the bytes of its
  transfers, done and to do.
- **Pause for a while:** a pause can carry its end (Unix seconds), kept in the config with the
  pause; the runner resumes by itself then, also after a restart.
- **Recently synced:** each upload, download, move and delete, with its path, size and time;
  more than 10 of one kind in one folder in one sync become one entry for the folder. The
  newest 500 are kept in the folder's state DB (table `synced`), so the list survives a restart.

- **Recently deleted:** the account's removed nodes (node schema, "Recently deleted"), kept in
  its state DB (table `removed`), listed newest first with their path and the folder each comes
  back to. Restore and Empty are the account's: run on its thread when asked (once connected).
  Empty takes what the user was shown: the items named, or everything removed until the newest
  one listed, and the device's own removals shown "just now" (their time still open: not back
  from the server yet) by name. Once a day (the time is kept in the state DB) the account deletes for good what
  was removed more than
  `keep_deleted_days` ago (a folder's delete as a whole), and empties `.hbsync/trash` of
  entries that old, from those made since the purge began (older ones, from before Recently
  deleted, may be the only copy and stay).

### States

`starting` (catching up, then the first sync, with its progress), `syncing`, `idle`, `paused`, `offline` (can't
reach the identity), `failing` (another failure, or the folder gone), `denied` (the identity
refused this device: `hbsync setup` again), `stopped`.

## Control socket

One request line, one reply line (JSON), then the connection closes. Requests carry the protocol
version (now 5); a daemon refuses another one with the reason (after an update: restart it). The
wire types are `hbsync-app/src/protocol.rs`. A control goes to the daemon (every folder), to an
account (its Recently deleted) or to one folder (its held deletes):

```json
{"protocol":5,"ask":"status"}
{"protocol":5,"ask":"stop"}                                         // taken whatever the protocol
{"protocol":5,"ask":{"control":{"daemon":"sync_now"}}}            // resume
{"protocol":5,"ask":{"control":{"daemon":{"pause":{"until":1800000000}}}}}   // "until":null: until resumed
{"protocol":5,"ask":{"control":{"folder":{"path":"/home/anna/Homebase","control":{"confirm_deletes":{"token":"<held_deletes.token>"}}}}}}
{"protocol":5,"ask":{"control":{"account":{"identity":"anna.example.com","control":{"restore":{"nodes":["<node id>"]}}}}}}
{"protocol":5,"ask":{"control":{"account":{"identity":"anna.example.com","control":{"empty_deleted":{"nodes":["<node id>"]}}}}}}
{"protocol":5,"ask":{"control":{"account":{"identity":"anna.example.com","control":{"empty_listed":{"db":"<db id>","seq":4711}}}}}}  // all it held when listed
{"protocol":5,"ask":{"recent":{"limit":10}}}
{"protocol":5,"ask":{"deleted":{"limit":100}}}                    // the newest 100 deletes
{"protocol":5,"ask":{"find_deleted":{"items":["Photos/2024","<node id>"]}}}
{"protocol":5,"ask":{"add_folder":{"identity":"anna.example.com","path":"/home/anna/Homebase","role":null}}}
{"protocol":5,"ask":{"remove_folder":{"path":"/home/anna/Homebase"}}}
{"protocol":5,"ask":{"start_over":{"identity":"anna.example.com"}}}  // new credentials
```

A request of another protocol is refused (`this daemon speaks protocol N, …`), except `stop`,
which every daemon takes whatever protocol it speaks: an update stops the daemon of the build
before it. `start_over` has an account stop its folders and start again with its credentials
read anew (`hbsync setup` after logging in again).

`add_folder` sets the folder up in its account's state DB (its marker written; kept as it was if
it is already), adds it to the config and starts syncing it, in its account (a new one for an
identity not synced yet), without a restart: `hbsync setup` checks what the user wants first.
`remove_folder` stops syncing it and takes it off the config; its files and state stay.

Replies:
- `{"status":{"version":…,"accounts":[…],"folders":[…]}}`. An account has its `identity`, the
  notification connection (`hints`) and a `credentials_warning`. A folder has its `identity`,
  `path`, `role` (`null` for the main folder), `state`, `progress` (while `starting` or
  `syncing`, once the sync has a plan; its totals grow as each batch of up to 20,000 items is
  planned), `paused_until`, `held_deletes`, and the counts `notes_added` and `synced_added`
  that tell a client what is new.
- `{"recent":[…]}` (each entry with its `folder`), newest first.
- `{"deleted":{"items":[…],"accounts":[…]}}`: the newest `limit` deletes, newest first, each an
  item deleted on its own with its account's `identity` and `along`, how many items its delete
  took along: the removed ones deleted with it, which restoring it restores, whatever emptying
  can take now (not those items: a deleted folder of thousands of files is one entry); and, for
  each account holding anything, its `deletes`, its `items` (what emptying all of it deletes
  for good: removed nodes only, the deletes and what they took along, but a folder holding a
  live node or a removal that stays: 0 when nothing can be emptied yet, which clients say
  instead of offering to empty), and what the listing saw:
  `db`, the account's state DB (a random id made with it, so one made again is another), and
  `seq`, the counter of this device's order of recording removals (each version of a node
  removed gets the next when first recorded; recorded again, its server time confirmed, it keeps
  it; another version, after a restore or an edit, gets a new one). `empty_listed` with them
  deletes for good exactly the removals recorded up to `seq`, an open one too: nothing recorded
  since, whatever its server time, and nothing live; a listed folder holding either stays,
  emptied of what was listed in it. A listing of another DB, or a `seq` the account never gave,
  is refused in the reply (and again by the drive as it empties).
- `{"found":[…]}`: for `find_deleted`, the items named (a node id, or the place `hbsync deleted`
  shows: a delete there before an item a delete took along, then the newest; a name shaped like a node id that isn't one is tried as a place),
  deleted on their own or along with a folder; refused naming the first one that isn't there.
  Restore and empty pick through it, so no client reads Recently deleted whole.
- `"done"`, `{"refused":"why"}`.

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
  loop on the main thread; on macOS a menu bar item without a Dock icon, the cloud as a
  template image.
- **Icon:** the Filesync cloud on the Homebase tile, its dot in the state's colour: green up to
  date, cyan syncing, grey paused or offline or not set up, magenta when something needs the
  user (a failure, held deletes, a refused login). On macOS the cloud in one colour; a problem
  shows the tile with its magenta dot. The files: `crates/hbsync-tray/assets/README.md`.
- **Menu:** version, identity, a status line (with progress while syncing, the first sync after
  starting included), Open Homebase folder, Browse files online (chat's web app on the
  identity, opened on its Filesync mini-app: `https://<identity>/apps/chat/#filesync`),
  Recently synced (the last 10; clicking one shows it
  in the file manager), Problems (the status notes), Review N deletions…, Recently deleted…, Folders…, Log
  in again… (refused), Pause syncing (1 h, 8 h, 24 h, until resumed) or Resume, Sync now, Start
  at login, Open logs, Log out, Quit.
- **Pages:** logging in (the Homebase address, the folder, `~/Homebase` unless chosen with the
  OS's picker, merging a folder with files only when ticked), reviewing held deletes and
  Recently deleted (the topmost items: path, size, date, how many items their restore brings
  back, Restore; Empty now, after a confirmation), and the folders (each role where this
  computer keeps it, how many files and bytes it holds, counted for 2 s at most; Sync, or why
  it can't be: another app syncs it, a synced folder holds it; Stop syncing; Downloads marked
  not recommended; on macOS the folder is read once on Sync, so the system asks for access
  while the user is there) are
  pages the tray serves on `127.0.0.1`, on a random port, under a random path, and opens in the
  browser; the identity's approval returns there.
- **Notifications:** a new problem note, held deletes, a refused login; each kind at most once a
  minute.
- **The folder:** the tray gives the main folder (not a role folder: it is the user's own) the
  Filesync icon (`desktop.ini` on Windows, Finder's custom
  icon on macOS, `.directory` and GIO metadata on Linux) and, on Linux, a shortcut (GTK
  bookmarks and KDE places; Finder's sidebar has no public interface). The shortcut once per
  folder, so one the user removed stays removed; the icon again when a new version brings
  another (the note `folder-decorated` holds the folder and a hash of its icon; an icon that
  failed is tried again at the next start). On Windows and Linux the icon is a file in the state
  folder named after its content (`homebase-<hash>.ico`, `.png`), so file managers that cache
  icons by path show a new one; Explorer is told the folder changed. Unlinking
  (also the tray's Log out, which unlinks) takes them off again; `hbsync logout` keeps them.
- **Explorer's navigation pane (Windows):** at every start the tray registers the folder as a
  Cloud Files sync root named "Homebase" (`HomebaseFilesync!<user SID>!<identity>~<hash of the
  folder>`), at the top of the pane as OneDrive's and Dropbox's are. Only the registration, no
  placeholders: the files stay ordinary files.
  - Already registered, it is left alone; ours that are stale go (the same id at another
    folder, an earlier identity's at this one).
  - A Quick access pin (older versions made one) comes off when the root is registered.
    Where Explorer refuses a root (not NTFS, a share, inside another provider's root), the
    folder is pinned to Quick access instead.
  - Unlinking unregisters it, found by its id, also when the folder was moved or deleted.
    Uninstalling without unlinking leaves the entry (for the MSIX, untested).

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
changed; an autostart entry is brought up to date with it. Started from there or at logon, the
session gives it a systemd unit of its own (System Monitor shows Homebase Filesync, the daemon
below it). Started from a terminal on a desktop, the tray, and the daemon a terminal starts
(`hbsync setup`, `start`), move into a scope of the app's as the desktop's launchers do: the
user's service manager makes `app-id.homebase.filesync-<random>.scope` for the running process
(`StartTransientUnit` over D-Bus). The app is then listed as itself, not as the terminal. A
tray started that way still ends with its terminal (`setsid` keeps it). Not away from a desktop
(`XDG_SESSION_TYPE` neither `wayland` nor `x11`: over SSH a scope would end with the user's
service manager at logout), nor with `HBSYNC_HOME` set; a move that fails leaves the process
where it was. The launcher stays after Log out, as
the way to log in again. Elsewhere the installer will do this (not built yet).

`hbsync setup` turns autostart on, `logout` and `unlink` turn it off, `hbsync autostart on|off`
by hand. With the daemon starting at logon, `hbsync start` goes through launchd or systemd so
that they supervise it. With `HBSYNC_HOME` set, only `hbsync autostart` touches the user's logon entry.

## Command line

| | |
|---|---|
| `hbsync setup <identity> [--folder P] [--existing] [--browser CMD] [--no-start] [--no-autostart]` | authorizes this computer (the owner approves in the browser) unless it already is and the identity still accepts it, saving the credentials at once, prepares the folder (default `~/Homebase`; it must be new, empty, already this identity's, or `--existing` to merge its files with the drive), turns autostart on, and has the daemon add the folder (starting it when it isn't running; restarting it when it logged in again, so the new credentials are used) |
| `hbsync add <role> [--identity I] [--folder P]` | syncs one of the user's own folders (`documents`, `pictures`, `music`, `videos`, `downloads`) where the OS keeps it (Windows Known Folders, macOS home folders, XDG `user-dirs.dirs`), or at `--folder`, with the drive's role folder: joined if another device syncs it (identical files adopted), back from Recently deleted, or made. Refused where `setup` refuses a folder, inside or around another synced folder, when one of that role is synced already, and when OneDrive or iCloud syncs the folder. Without a running daemon it is set up for the next start |
| `hbsync remove <role> [--identity I]` | stops syncing that role folder; its files and state stay, so adding it again resumes |
| `hbsync status [--json]` | what the daemon does; exit status 3 when it isn't running |
| `hbsync sync`, `pause [--for 1h]`, `resume` | a pause with `--for` ends by itself |
| `hbsync recent [-n N]` | what was synced lately, newest first |
| `hbsync confirm-deletes [--yes]` | shows the held deletes and lets them through |
| `hbsync start`, `stop` | |
| `hbsync autostart [on\|off]` | |
| `hbsync logout` | stops the daemon, forgets the credentials, autostart off; the folder stays |
| `hbsync unlink` | stops the daemon, removes the folder from the config, autostart off; its files, `.hbsync` and the account's DB stay, so setting it up again resumes |
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
