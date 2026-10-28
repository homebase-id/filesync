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
| locks, pids, pipe token, the stopped mark, the tray's icon files, the state DBs | `$XDG_STATE_HOME/hbsync` (`~/.local/state/hbsync`) | same as config | `%LOCALAPPDATA%\Homebase Filesync` |
| logs | `<state>/logs` | `~/Library/Logs/Homebase Filesync` | `<state>\logs` |
| control socket | `$XDG_RUNTIME_DIR/hbsync`, else `<state>` | same as config | a named pipe |

Each account has one state DB, `<state>/accounts/<identity>/state.db` (the identity in lower
case, a port's `:` as `_`), for every folder it syncs on this computer: the drive's side (its
mirror, Recently deleted) once, each folder's own rows (what it has, what was agreed) keyed by
the folder. It records its identity (`account.identity`).

A sync folder keeps the rest in `<folder>/.hbsync` (temp files, and an item being deleted or
replaced for the moment it is checked, in `.hbsync/aside`, on the folder's disk for atomic
renames; the ignore rules), hidden on Windows too (the hidden attribute, set at every start),
and its marker `.hbsync/folder`: the identity and
the folder's id in that DB.
A folder is synced only through its marker: without one it is gone (or not mounted), with
another identity's it is refused, and one the DB doesn't know is set up again by `hbsync setup`
(merging its files, `--existing`), or as the account starts after its DB was set aside (below).

An account's DB that can't be used at all is set aside as the account starts (the daemon's
start, or `hbsync setup` or `hbsync add` while no daemon runs): one from before 0.7, which isn't
upgraded, or one whose file is bad (not a database; a damaged page, which SQLite's `quick_check`
finds, on a connection that only reads, before anything is upgraded or written: it reads the
whole file at each start, ~0.3 s and a read of 226 MB for 300,000 files; a table, column, index
or trigger missing, also where its upgrade fails for one). A page that can't be read for now (an
I/O error) isn't damage. One found bad while the account runs (SQLite answers a folder's runner
that it is malformed, or not a database, as it takes changes, scans or syncs) is looked at then:
the runner ends, the account stops the rest of its folders and closes every part holding the DB
(its feed, Recently deleted's keeper, the answers' connection), and checks it as at a start,
its folders showing *starting* meanwhile; damaged, it is set aside and the account syncs on in
a new one. A check that finds nothing (a passing fault is not worth every folder set up again:
their files hashed, the whole drive read) lets the folders start again; found bad again, it is
set aside unchecked (a check of its pages doesn't see an index's damage). Once set aside, not
again in that run of the account: found bad again, its folders back off as after any failed
sync, until the daemon starts again and checks the file. An error setting it aside is shown
and tried again after a pause, checked (not the daemon started again and again). A folder
added meanwhile waits for it (one DB's turn in the daemon: its setting up, and the config read
for the folders to set up again). It is renamed beside the new one,
`state.db.too-old-<Unix time>` or `state.db.corrupt-<Unix time>`, with its `-wal` and `-shm`
files named alike, never deleted: they are for a diagnosis, and old ones can be deleted by hand.
The account's folders that are there (their marker this identity's) are then set up again in the
new DB as folders with files, as `hbsync setup --existing` does: what they hold is adopted by
hash, nothing is copied twice, and a file that differs from the drive's becomes a conflict copy.
The tray and the log say so, once, and `hbsync setup` and `hbsync add` print it when they set
it aside (setup also when it then refuses the folder). The new DB records that it replaced one (`db.set_aside`) until it knows each of them:
a folder not there then (a disk not plugged in), or one that couldn't be set up, is set up again
at a later start of the account, once it is back. Meanwhile setup refuses it as it refuses any
folder synced here whose marker is gone (the empty mount point of a disk not plugged in), unless
asked to merge: the config lists it, though the new DB doesn't know it yet. Not set
aside: a DB of a newer version (a downgrade: setting it aside would throw a newer version's
records away; the account waits, saying why), and one that can't be read for now (busy or
locked, an I/O error, a full disk, access denied, a missing folder, a read-only file system:
told as an error, and tried again as the daemon starts again).

The recovery rule (data safety): setting a DB aside never deletes a file, here or on the drive:
the new DB has no base, and without one nothing is deleted. What is lost is the old DB's
pending state, and each part errs on the side of keeping: deletes held for review come back
from the drive; Recently deleted's bookkeeping is read from the drive again (a removal's time
is the server's: none goes for good sooner); a download placed but not recorded is adopted by
its hash; what a delete or a download left in `.hbsync/aside` goes only where its bytes are the
synced version its name says, else it goes back to its place, or stays there and is told; an
item parked under `.hbsync-move-<node>` keeps that name; an edit not sent yet becomes a
conflict copy beside the drive's version.

## `config.toml`

```toml
device_name = "Anna's laptop"   # optional; the computer's name otherwise
paused = false                  # hbsync pause / resume

[mass_delete]                   # sync semantics, the mass-delete guard
min_files = 50
fraction = 0.25

[[roots]]
identity = "anna.example.com"
folder = "/home/anna/Homebase"
# role = "documents"            # a role folder (sync semantics §12); none for the main folder
```

Unknown keys are an error. An identity is read as the client keeps it, here and on the
control socket: in lower case, without a trailing `/` (`Anna.Example.com/` is
`anna.example.com`). Writes replace the file in one step. While the daemon runs, it alone
writes it: as it adds and removes folders, and to keep a pause. The CLI, setup and the tray ask
it; when none runs, they write it themselves. Every change reads, changes and writes the file
holding `config.lock` beside it (one writer at a time, in any process). A program that finds no
daemon running keeps that lock from looking until it has written, and a daemon starting takes it
before its own lock, and reads the config and binds its socket under it: it starts after such a
change and reads it, and a program that finds it running can ask it.

Recently deleted keeps removed items 30 days, then deletes them for good: not a setting (the
purge is the account's, so every client keeps 30). A `keep_deleted_days` an older version
wrote is no longer used: the file loads, with a warning (once a run), and the next write
leaves it out.

A hand edit while the daemon runs isn't watched for; each setting takes effect as follows:

| Setting | Takes effect |
|---|---|
| `device_name`, `mass_delete`, a root's `role` | when the folder's sync (re)starts: the daemon's next start, a login, or a restart after a failure |
| `paused`, `paused_until` | at the daemon's next start (it holds the pause meanwhile); while it runs, `hbsync pause`/`resume` and the tray set them |
| `[[roots]]` added or removed | at the daemon's next start; while it runs, `hbsync add`/`remove` and the tray do it |

While the file doesn't read (being written, a typo), a folder's sync waits to (re)start, and the
daemon's own writes (a pause, a folder) are refused.

## Credentials

In the OS keychain, service `Homebase Filesync`, account = identity, value = the credentials
JSON (identity, client auth token, shared secret):

- macOS: Keychain Services, the login keychain, which Migration Assistant carries to a new Mac
  (keeping the item on this Mac, `AfterFirstUnlockThisDeviceOnly`, needs the data protection
  keychain, and that a provisioning profile and keychain-access-groups entitlement the app
  doesn't have);
- Windows: Credential Manager, with `CRED_PERSIST_LOCAL_MACHINE` persistence: a roaming
  profile doesn't carry it to another computer (an entry written before, roaming, is written
  again so when read);
- Linux: the Secret Service (GNOME Keyring, KWallet) over D-Bus, which has no notion of
  keeping an item on one computer: a copied keyring carries it.

When no keychain answers (a Linux without a Secret Service, say), the credentials go to
`<config>/credentials/<identity>.json` (named as the state DB's folder: a port's `:` as `_`,
which Windows refuses; a file of an earlier version named with the `:` is read and moved),
mode 0600 in a 0700 folder (on Windows the profile
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
   (`hbsync-app/src/daemon/account/`), which:
   1. opens its state DB, before any request: one only the user can set right (corrupt, from
      another version, another identity's) is shown on its folders (`failing`, with why) and
      looked at again hourly or at a wake-up, asking nothing; the other accounts sync on;
   2. reads the credentials, waiting (backoff 5 s doubling to 5 min) while the keychain can't
      be read. None: it says so and the account ends;
   3. connects to the identity, retrying with the same backoff while it can't be reached
      (`offline`, at least as long as its `Retry-After` says); a refusal (the device revoked)
      shows as `denied` and is asked again hourly (new credentials start it over at once). Meanwhile `pause` and `resume` work
      (kept in the config); other requests are refused;
   4. opens the account's `Drive` and the notification socket, and runs a sync loop
      (`Runner`) for each of its folders until a stop request, SIGTERM or SIGINT. It passes the
      notifications on to every folder's loop, reconnects the socket after a sleep, and has
      Recently deleted's requests and its daily purge (not while paused) run on a thread of
      their own, one at a time: the account's loop never waits for them (a purge waits on the
      state DB while the folders' first syncs hold it). What a purge found not there once (a
      delete for good this device missed) that thread asks again a minute on, once (sync
      semantics §8b).

   One folder's loop failing is started again after a pause (from 5 s, doubling up to 5 min,
   reset after an hour without failing), the folder showing `failing` with its error
   meanwhile; the account's other folders sync on. A loop that can't start stops its account.

   **Refused (401, 403).** Once the identity refuses this computer (a sync, a read of the
   feed, or the notifications' upgrade), the account is latched (`Drive::deny`): its folders
   show `denied` and ask nothing but one sync an hour each (a 403 for one write needn't hold
   the account for good), its puller reads nothing and wakes no folder, the daily purge waits. The notifications ask again
   after 5 min, doubling up to an hour: once they connect (the credentials work again), or the
   user asks (Sync now, resume), or new credentials start the account over, it syncs as before.

An account stopped by what only the user can fix ends alone; one failing otherwise stops the
daemon (it is restarted whole).

**Stopping** (a stop request, SIGTERM, SIGINT, an account failing) ends every account's work
between operations and waits for the requests in flight (a transfer, a batch of deletes for
good, a read of the feed) at most 15 s (`daemon::STOP_WITHIN`: under launchd's 20 s before it
kills a job; a batch of deletes for good takes about 18 s on a slow Homebase, a large transfer
minutes, a server that hangs up to a request's timeout). Past it, the daemon ends without them,
as a crash would, but with its state DBs whole (`hbsync_state::hold_for_exit`): it waits for
the write in progress, however long it takes (no transaction is left open), makes every
commit durable, holds every later write, and exits; the threads still waiting for an answer
end with the process. `hbsync stop` waits for the daemon to be gone, a minute at most (a daemon
of an earlier version, stopped by an update, waits for every request in flight).

What those threads did is found again at the next start, as after a crash (sync semantics
§8a, §8b): a transfer cut short goes again, a delete for good the server carried out without
its answer being written is found not there by the next emptying or purge and confirmed by its
header, an upload it took is found again (a re-create answers 4105). As at a crash, a hard
delete announced and not taken yet is lost (its file's row is stale until it is next written),
and a thread that did something on disk without committing it yet is cut there (each
operation's write-ahead records and guards take it from there, §8a).

Exit status: 0 when stopped, when there is nothing to do (not set up, already running), and on
an error only the user can fix (a state DB that is corrupt or from another version, a config
that doesn't parse, credentials that don't parse): a service manager starting it again every
few seconds can't help; 1 on any other failure, a full disk or a volume not mounted yet
included (service managers restart it then).

### The sync loop

- **No live watcher** (it couldn't start: inotify's instances, 128 a user, all taken by other
  programs, or its watches on a very large tree; or it stopped): the folder's changes are found
  by its look, a whole walk once a minute. The user is told in a note, once a run (and `hbsync
  status` says why while it lasts). A watcher is asked for again at the next look, then
  less often while it fails (doubling, up to 32 looks: a tree over the watch limit fails each
  time, and each try walks it); once one starts, the look's walk catches up on what changed
  meanwhile (again at each look till it has), and the status and the note say no more of it.
- Syncs when the folder changes (live watcher; not its own writes coming back), when another
  folder's sync changed this one's part of the drive, when the identity announces a change on
  the drive (notification socket; once its announcements have stopped for 1 s, at most 10 s
  after the first, so a burst of deletes from another device reaches the mass-delete guard
  whole), when the socket connects (again), and on request. A sync that left work unfinished
  (the round limit, or operations skipped or failed; never deletes held for a confirmation)
  is tried again after 5 s, doubling to 5 min while the tries do nothing and starting over
  after one that did some, without holding back other syncs. After a failure it waits 5 s,
  doubling to 5 min. When the socket connects again, a folder waiting for either goes at
  once.
- **Announced changes** name their file (a minimal row, odin-core #1970): a file deleted for
  good is gone from the account's picture within a second (a hard delete leaves no row in the
  feed), and versions the account has already are taken as a slim feed page's rows; the feed
  is read only when something announced isn't known, or an event didn't say which file. One
  ref per file counts (a delete for good ends it; else the newest), so a write made twice, or
  made then deleted, needs no read. A hard delete announced is taken within a second (what is
  announced within a second of it, up to 500 changes, is taken in one write, hard deletes
  before the rest), also while paused (it is in no feed; it writes only the account's DB, the
  folders sync it at the resume), kept and tried again with a growing pause when that fails,
  and its file id is never let back in by a later row. When the daemon stops, what was
  gathered is taken first; only a crash in that second loses a hard delete, and then the file's
  row is stale until it is next written (a file deleted for good answers not found). A read
  of the feed asked for (connecting, the timer while not connected) doesn't wait. Live changes
  beyond 2,000 are read from the feed instead.
  Announced changes are taken one at a time with the pulls, on the account's pull thread
  (started again if it ended), never on the account's loop. A timed pause runs out for the
  account as for its folders.
- **Own writes aren't announced** (`excludeOwnWrites`, odin-core #1971): the socket leaves
  out the writes made with this computer's own token (its client registration), so what is
  announced is another computer's (or the phone's) doing. Nothing waits for an echo, and no
  feed is read for them: the executor records each write from its answer (version, token,
  and `updated`, the server's time the feed shows for it: a version's time, an own
  removal's for Recently deleted and the purge). An answer without `updated` (null: only
  for a peer-only update, which this client never makes) leaves that time unknown, a
  removal's shown as just now and never purged, until a feed read the client makes anyway
  passes that row; it never asks for a read. The rows of its own writes stay in the feed
  ahead of the cursor and are read slim (versions it has) at its next read. The account's
  other folders hear of the writes through the account's drive, which they share, not the
  socket.
- **Known limitation: one login per computer.** The notifications leave out every write made
  with the computer's own login (its client registration), so two computers using the same
  login don't hear each other's changes: each gets the other's only when its notifications
  reconnect, at its start, or with a change from a third computer it doesn't know, which can
  be hours. Windows keeps the key on the computer; macOS Migration Assistant carries the
  login keychain to a new Mac; a Linux keyring copied to another computer carries it. So a
  migrated Mac, a copied keyring or a credentials file copied by hand can cause it. Nothing
  detects it: log in again on the second computer (`hbsync logout`, then `hbsync setup`),
  which registers it as a computer of its own.
- **When the feed is read**, and only then:
  - at a folder's start (and once more if its local catch-up, a walk, took long);
  - when the notifications connect (again): the account pulls, as changes made meanwhile
    weren't announced (connected while paused: at the resume). Only once that read worked
    are the drive's changes taken as announced (the account's picture *current*);
  - when an announced version isn't known, or an event didn't say which file. That one read
    brings it: the server announces a write once it is committed and its cached answers are
    cleared, and a row it holds back for a moment comes with more rows to follow, which the
    read reads on (`homebase-api-notes.md`, the change feed). Nothing is read for again;
  - every 5 min while the notifications aren't connected;
  - by a sync while the account's picture isn't current: the notifications aren't connected
    or their read after connecting hasn't worked, or the account is behind. It is behind
    when it can't be sure its picture is complete: a pull failed, the notifications were
    lost or refused, a write's answer was lost (it may have landed), or the drive refused a
    write for its state (a version moved on, a node id taken, a file gone: a change it
    wasn't told of, or an earlier try of its own write that landed unanswered). After a lost
    answer or such a refused write the sync reads at once; a read that works ends it;
  - by a sync the user asked for (sync now, a resume, answering held deletes) or the
    machine waking up; asked until a sync ended, so a failed one asks again;
  - before Recently deleted's restore, emptying and the daily purge, while not current;
  - when this device makes a role folder that another device made meanwhile.
  A sync woken by a local change or by the account's drive reads nothing while current,
  and none reads after its own writes. A read starts with a page of 500 rows; slim pages grow
  to 2,000 in a run of them (a slim page with something new is read again whole, as far as 500
  rows go, after its rows before the new one, read slim, when 500 wouldn't reach it).
- **No poll:** nothing syncs on a timer while the notification socket is connected. The
  drive is read from the feed's cursor, so what wasn't announced (the socket was down, a
  notification got lost) arrives at the next (re)connect, when the account pulls as at start
  (which wakes only the folders it concerns), or with the next change announced that the
  account doesn't know. While the
  socket isn't connected (not yet, not any more, or never: a proxy blocking it, a refusal,
  its thread not starting), the account reads the drive every 5 min instead. Nothing is
  pulled while paused; the account's pulls run on a thread of their own. A dead socket is
  noticed within 75 s (no frame, not even a pong to the ping every 30 s) and reconnected
  (backoff 1 s, doubling to 30 s); a refused subscription the same way, but not sooner than
  every 5 min.
- **No wake-up while idle:** the daemon's threads block until something happens or the next
  thing is due. A folder's loop waits on one channel (requests, the watcher, the drive's
  wakes) until a backoff, a retry, a pause's end or the look of a folder without a watcher
  (and 10 min at most); the watcher rings it, and the journal position is taken once per
  batch, not on a timer. An account's loop waits on one channel (requests, its
  notifications, its folders' ends) until the announcements quiet down, the read while not
  connected, a restart or the purge's look (10 min). The daemon's own loop waits for an
  account to end or a stop. The notification socket's thread blocks until a frame comes or the
  keep-alive is due (a stop or reconnect shuts the socket down). The tray is sent the status
  when it changes (`watch`, below). Left on a timer: the notification socket's ping (every
  30 s), the sleep poll below off Linux, the 10 min bounds on a lost wake-up, and the tray's
  minute (relative times).
- **Idle connections:** an account's HTTPS connections stay open for reuse while calls come;
  15 s after the last call ends (when they could no longer be reused anyway) they are closed,
  with their buffers, by a thread that waits without a timeout while nothing is open or calls
  run. The TLS sessions are kept: the next call resumes one instead of a full handshake
  (measured on frodo: ~1.3 KB on the wire against ~4 KB).
- **Pause** stops a running sync between operations and before the next transfer (transfers
  in flight finish); nothing syncs until resume. The watcher keeps recording changes. A pause
  survives a restart (`paused` in the config).
- **Sleep:** the daemon notices it on a thread of its own: on Linux the kernel tells it (a
  timer cancelled when the wall clock is set, as a resume does); elsewhere a 30 s wait that
  took over a minute by the wall clock means the machine slept (the OS's power notifications
  are a follow-up). Every account then reconnects its notification socket, and every folder
  syncs at once and forgets its backoff.
- **Network changes:** the reconnect backoff (at most 30 s); no OS hooks yet.
- **Ignore rules:** `.hbsync/ignore` is read again a second after the watcher saw it change
  (once a minute in a folder without a watcher) and on a sync request; an edit restarts the
  engine with the new rules (sync semantics §10). A line that isn't a valid rule is skipped,
  and the folder's notes say so once a version of the rules: ".hbsync/ignore line 3 isn't a
  valid rule and is skipped: …" (several: "lines 3, 5 and 9 aren't valid rules and are
  skipped (line 3: …)"); the note goes once the lines are fixed. A bad line that starts with
  `!` (a negation, indented or not: skipped, it would ignore what it keeps synced) stops the
  folder's sync, failing, till fixed, with one note naming every such line: ".hbsync/ignore
  line 2 isn't a valid rule (…), and keeps things synced: syncing stops till it is fixed (fix
  it to resume)"; it goes once fixed. A file that can't be read stops it too, failing, till
  it can.
- **The folder gone** (moved, deleted, a disk not mounted: its marker missing): nothing
  syncs, the status says why, and the loop waits for it to come back. It never creates a
  marker: only `hbsync setup` does.
- **Held deletes:** the status shows how many (deleted here, to go from the drive; removed
  elsewhere, every synced file), some paths, and a token naming exactly these deletes and
  which way each goes, until they are answered (nothing expires; a held removal of every file
  lasts across restarts). An answer carries the token back: `confirm_deletes` lets exactly
  them through, `put_back` puts exactly them back (sync semantics §7). The folder's runner
  checks it when it comes, against what it holds: when the held deletes changed since, or the
  folder is gone (nothing is held there meanwhile), it is refused; otherwise it is taken, and
  only then is the reply Done. One answer each time they are shown: another is refused until a
  sync shows them again. A taken answer goes with the next sync (after one in progress;
  while paused, once syncing resumes), for the deletes it named: any held since stay held,
  shown again. One dropped after it was taken (the folder went, or was set up again) is told
  in a note. A put back that fails runs once more a minute on, while the same deletes are held
  and nothing else answered them (what the identity said once isn't there is gone for good
  then, sync semantics §8b); the note says so. The tray says "812 items in Homebase were deleted on this computer (or newly
  ignored). Delete removes them from Homebase and your other devices too: Recently deleted
  keeps them. Put them back keeps them on Homebase and brings them back here. …" (or,
  everything removed elsewhere, "Everything here was removed on another device: 1,200 items.
  Delete removes them here too. Put them back keeps them here and restores them on Homebase,
  for every device."), with **Delete** and **Put them back**; each button names its answer,
  and a form without one does nothing. The CLI says the same with its commands in place of
  the buttons, and asks one question. The reply says when they go: "with the next sync", or
  "once you resume syncing".
- **Notes:** each conflict and failure is noted once; one that comes back sync after sync (a
  file that can't be read) isn't noted again until it has gone away. An operation left for the
  next plan is noted only when this device can't do it (a name or path it can't hold, access
  denied, the drive refusing it); one that went stale since the plan (the item changed or went)
  is planned again quietly, and told only when it keeps being skipped for ten minutes
  (something holds it back that the plan can't see: a file written all the time, one that can't
  be read). One waiting for another operation (a folder's base for its children's) isn't told:
  that one says why. A file another program holds locked, or one that can't be downloaded
  because the disk is full, is left for a later sync (no retry within the run) and is told
  like a stale one, when it keeps coming back for ten minutes. A full drive (Homebase's 507)
  is one note, "Homebase is out of storage": the uploads already queued in the run don't send
  their bytes, and every folder of the account holds its uploads for the time the 507's
  `Retry-After` says (between 30 s and a day; ten minutes without one), or until Sync now, and
  does the rest (downloads, moves, deletes); then one upload tries again.
- **Errors** are judged by one table (`hbsync_engine::policy`): the network or the server out
  of reach shows `offline`; that includes an answer that isn't the identity's (a captive
  portal's login page or a proxy's: no `X-Odin-Version`) and a redirect. A server that says
  when to ask again (429 or 503 with `Retry-After`: a paused identity says 10 min) stops the
  sync, and the folder or the account doesn't ask before then (up to an hour). A state DB
  error ends the sync, as a crash would: busy, the folder backs off; one only the user can fix
  (corrupt, from another version) is tried again every 5 min; the disk full, read-only or
  failing, after a pause from 5 s doubling to 5 min. A row on the drive the
  client can't read (not encrypted, another key, no content hash) is noted too. So is what a
  scan of the folder skips that the user may want to know: a folder it can't read (what is in
  it isn't synced), a name Homebase can't hold, a link, a device or socket; one note a reason
  with a few of its paths, each item once (again once a whole scan found it fixed). A
  sync that stops (refused, the drive out of reach) still tells the conflicts and failures it
  had come to. What is the account's is noted on the account (its status's `recent`), not on a
  folder: a restore or an emptying of Recently deleted that fails (it was answered before it
  ran), with what to do; a file on the drive this client can't read (another key, not a sync
  node), each version once while the daemon runs (a row that doesn't name its file, once a
  reason), and more than three new at once as one note. An operation the
  drive refused for good is noted once, and not sent again while it is the same (sync semantics
  §8a, refused): a change of the item, Sync now, or an hour sends it once more.
- **Progress** while a sync runs: its files (transfers, moves and deletes) and the bytes of its
  transfers, done and to do.
- **Pause for a while:** a pause can carry its end (Unix seconds), kept in the config with the
  pause. The daemon holds the pause and its one timer: at the end every account and folder
  syncs again (also one still connecting, and after a restart), as before the pause (unlike
  `hbsync resume`, what the identity refused isn't asked again); the accounts and folders hold
  it without a timer of their own.
- **Recently synced:** each upload, download, move and delete, with its path, size and time;
  more than 10 of one kind in one folder in one sync become one entry for the folder. The
  newest 500 are kept in the folder's state DB (table `synced`), so the list survives a restart.

- **Recently deleted:** the account's removed nodes (node schema, "Recently deleted"), kept in
  its state DB (table `removed`), listed newest first with their path and the folder each comes
  back to. Restore and Empty are the account's: run on its Recently deleted thread when asked
  (once connected), in turn with the purge. A restore the identity answers "not there" for
  (its first word on it, sync semantics §8b) is noted as such ("Restoring from Recently
  deleted: Homebase says one item isn't there; asking again in a minute") and run once more a
  minute on, with what of it is still there (while the daemon runs; one a second restore
  meets too joins it); what it then finds deleted for good, or another path found so meanwhile,
  is noted ("… was deleted for good meanwhile and can't come back"). Rows a restore leaves in
  Recently deleted with no error (a name taken where they go, a folder that didn't come back)
  get one note naming the first five ("… stayed there (…): try again, or rename what holds the
  name"); one the drive had changed meanwhile (another device's restore, likely) isn't named.
  Empty takes what the user was shown: the items named, each with what was in it when it was
  listed (a `deleted` or `find_deleted` answer carries its listing: a removal recorded in it
  since stays), or everything removed until the newest one listed, and the device's own removals shown "just now" (their time still open: not back
  from the server yet) by name. Once a day (the time is kept in the state DB) the account deletes for good what
  was removed more than
  30 days ago (a folder's delete as a whole, never a live node: a removed folder
  still holding one stays, restorable). Nothing is kept on the computers: Recently deleted on
  the drive is the only copy of what was deleted (a `.hbsync/trash` an older version left is
  no longer touched: the user deletes it by hand). What goes, and how: node schema, "The purge".

  Emptying and the purge run in the background (the CLI and the tray say so when asked) and
  delete in batches (250 files a request, a quarter of what Homebase takes, so a stop waits
  less), two at once, each batch written as it is answered: Recently deleted shrinks as it goes, and
  a crash or a kill loses nothing done (a SQLite commit a batch). The log says when one starts (how many items) and ends (how many
  went, or how far it got when it stopped); the account's status carries `emptying` while it
  runs, which `hbsync status` and the tray's menu show. Once the account ends (`hbsync stop`,
  Quit, SIGTERM, its last folder removed), it stops after the batches in flight, their answers
  written, and runs no later pass (what is in a folder goes before it): what it didn't reach
  stays in Recently deleted, for the next emptying or purge. An emptying asked but not started
  by then doesn't start; a purge stopped so isn't recorded as done, and runs again at the next
  start. On a Homebase that answers slowly (about 70 s for 1,000 files, so about 18 s for a
  batch), a daemon's stop doesn't wait for the batches in flight past its 15 s (Stopping,
  above): what such a batch deleted stays in Recently deleted here until the next emptying or
  purge finds it not there, confirmed by its header a minute on (sync semantics §8b). Two
  batches of a pass go at once (`FOR_GOOD_AT_ONCE`: two requests on the server, so its storage
  work overlaps, inferred: not shown on a local server on SQLite, where they take as long as one
  after the other), and a pass starts once the one before is answered.

### States

`starting` (catching up, then the first sync, with its progress), `syncing`, `idle`, `paused`, `offline` (can't
reach the identity), `failing` (another failure, or the folder gone), `denied` (the identity
refused this device: `hbsync setup` again), `stopped`.

## Control socket

One request line, one reply line (JSON), then the connection closes; but `watch`, answered with
the status now and again each time it changes (at most once a second while a folder syncs: its
progress changes without telling), on the open connection until either side closes it (what
the tray shows). Requests carry the protocol version (now 10); a daemon refuses another one
with the reason (after an update: restart it). The
wire types are `hbsync-app/src/protocol.rs`. A control goes to the daemon (every folder), to an
account (its Recently deleted) or to one folder (its held deletes):

```json
{"protocol":10,"ask":"status"}
{"protocol":10,"ask":"watch"}                                        // a status line now, then one per change
{"protocol":10,"ask":"stop"}                                         // taken whatever the protocol
{"protocol":10,"ask":{"control":{"daemon":"sync_now"}}}            // resume
{"protocol":10,"ask":{"control":{"daemon":{"pause":{"until":1800000000}}}}}   // "until":null: until resumed
{"protocol":10,"ask":{"control":{"folder":{"path":"/home/anna/Homebase","control":{"confirm_deletes":{"token":"<held_deletes.token>"}}}}}}
{"protocol":10,"ask":{"control":{"folder":{"path":"/home/anna/Homebase","control":{"put_back":{"token":"<held_deletes.token>"}}}}}}
{"protocol":10,"ask":{"control":{"account":{"identity":"anna.example.com","control":{"restore":{"nodes":["<node id>"]}}}}}}
{"protocol":10,"ask":{"control":{"account":{"identity":"anna.example.com","control":{"empty_deleted":{"nodes":["<node id>"],"db":"<db id>","seq":4711}}}}}}  // the items as listed
{"protocol":10,"ask":{"control":{"account":{"identity":"anna.example.com","control":{"empty_listed":{"db":"<db id>","seq":4711}}}}}}  // all it held when listed
{"protocol":10,"ask":{"recent":{"limit":10}}}
{"protocol":10,"ask":{"deleted":{"limit":100}}}                    // the newest 100 deletes
{"protocol":10,"ask":{"find_deleted":{"items":["Photos/2024","<node id>"]}}}
{"protocol":10,"ask":{"add_folder":{"identity":"anna.example.com","path":"/home/anna/Homebase","role":null}}}
{"protocol":10,"ask":{"remove_folder":{"path":"/home/anna/Homebase"}}}
{"protocol":10,"ask":{"start_over":{"identity":"anna.example.com"}}}  // new credentials
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
  notification connection (`hints`), a `credentials_warning`, its own notes (`recent`, with
  `notes_added`, as a folder's), and `emptying` while Recently deleted is being deleted for
  good (`done` of `total` items, `purge` when it is the daily purge; left out otherwise). A folder has its `identity`,
  `path`, `role` (`null` for the main folder), `state`, `progress` (while `starting` or
  `syncing`, once the sync has a plan; its totals grow as each batch of up to 20,000 items is
  planned), `paused_until`, `held_deletes`, and the counts `notes_added` and `synced_added`
  that tell a client what is new.
- `{"recent":[…]}` (each entry with its `folder`, and `main` when that is the main folder),
  newest first.
- `{"deleted":{"items":[…],"accounts":[…]}}`: the newest `limit` deletes, newest first, each an
  item deleted on its own with its account's `identity`, its `place` (where it was, as people
  name it and `hbsync restore` takes it: the folder's path before its path in another folder
  than the main one, `(on the drive) ` before it in a part of the drive no folder here syncs;
  left out in the main folder, where it is its `path`), `along`, how many items are in it:
  all of Recently deleted below it, through removed folders, what was deleted with it (which
  restoring it restores) and what was deleted from it before (listed on its own too), which
  emptying it takes with it (not those items: a deleted folder of thousands of files is one
  entry), and `stays` (left out when false) when emptying leaves it: it holds a live node, and
  only what else is in it goes (clients say so, never "deleting" alone); and, for
  each account holding anything, its `deletes`, its `items` (what emptying all of it deletes
  for good: removed nodes only, the deletes and what was deleted with them, but a folder holding a
  live node or a removal that stays: 0 when nothing can be emptied yet, which clients say
  instead of offering to empty), its `stays` (the items deleted on their own that emptying
  leaves: they hold a live node), and what the listing saw:
  `db`, the account's state DB (a random id made with it, so one made again is another), and
  `seq`, the counter of this device's order of recording removals (each version of a node
  removed gets the next when first recorded; recorded again, its server time confirmed, it keeps
  it; another version, after a restore or an edit, gets a new one). `empty_listed` with them
  deletes for good exactly the removals recorded up to `seq`, an open one too: nothing recorded
  since, whatever its server time, and nothing live; a listed folder holding either stays,
  emptied of what was listed in it. A listing of another DB, or a `seq` the account never gave,
  is refused in the reply (and again by the drive as it empties).
- `{"found":[…]}`: for `find_deleted`, the items named (a node id, or the place `hbsync deleted`
  shows: a delete there before an item deleted with one, then the newest; a name shaped like a node id that isn't one is tried as a place),
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
another version), unless syncing was stopped on purpose. One tray per user (`homebase-filesync.lock`).
It watches the daemon's status (`watch` on the control socket: sent when it changes, read on a
thread of its own), asks once more after an action, shows the status again once a minute (the
menu's relative times; and looks whether its program was replaced on disk), and while the daemon
isn't running looks every second (every ten while nothing is set up or syncing is stopped).

**Starting and stopping.** *Quit Homebase Filesync* asks, as a submenu, whether to **keep
syncing in the background** (the daemon goes on without the tray) or **stop syncing too**; with
nothing running (not set up, stopped, failing to start) it just quits. Stopping syncing (that
choice, or `hbsync stop`) leaves a mark, `<state>/stopped`, before it asks the daemon to stop
(a request that doesn't reach it takes the mark off again). Starting it on purpose ends the
stop (`hbsync start`, *Start syncing*, `setup`: `control::start` takes the mark off, and
starts the daemon unless it runs);
a daemon started otherwise while the mark is there (by the service manager at logon) ends at
once, as with nothing to sync. While stopped, the tray shows *Syncing stopped* (the grey dot)
with **Start syncing**, and looks once a minute (`hbsync start` shows within a minute). A tray
started while syncing is stopped says so once, in a notification: that is its question whether
to start. It asks nothing otherwise, as nothing was stopped on purpose:
- at login, the tray starts the daemon as before (it isn't running yet then);
- after an update, or a crash, the daemon was stopped without the mark, and is started again;
- a tray restarting into its update (*Restart to finish the update*, `--restarted`) leaves the
  daemon as it was and doesn't tell a stop again.

A stop is this computer's, for this user (the mark is in the local state folder): other
computers, and another user's Filesync on this one, keep syncing. It lasts until syncing is
started again, also across logins, as *Pause until I resume* does: starting at login starts the
tray, which then shows that syncing is stopped.

A quit runs on the thread that starts the daemon (the watcher's): after a start in flight (at
most its 15 s; after an update, the restart of a daemon of another version, its stop included),
and with none after it. The daemon that start brings up is the one stopped, or left running. Before, a quit could find
no daemon to stop yet and leave the one just started running, or watch the tray start the
daemon again as the stopped one went (the stop then gave up after a minute, "still finishing a
transfer"). On Linux a daemon left running moves out of the tray's autostart service into a
scope of its own (`unit::outlive_service`): systemd before 250 stops a service, and all in it,
when its main process ends.

How the tray asks is its menu, not a dialog: it has no window, and a native message box on
Linux (rfd's, without GTK) needs `zenity`, which KDE doesn't install.

- **Linux:** a StatusNotifierItem over D-Bus (pure Rust, no GTK), as KDE, Xfce, Cinnamon and
  most panels show them; GNOME needs the AppIndicator extension. Windows and macOS: tao's event
  loop on the main thread; on macOS a menu bar item without a Dock icon, the cloud as a
  template image.
- **Icon:** the Filesync cloud on the Homebase tile, its dot in the state's colour: green up to
  date, cyan syncing, grey paused or offline or not set up, magenta when something needs the
  user (a failure, held deletes, a refused login). On macOS the cloud in one colour; a problem
  shows the tile with its magenta dot. The files: `crates/hbsync-tray/assets/README.md`.
- **Menu:** version, identity, a status line (with progress while syncing, the first sync after
  starting included), how far emptying Recently deleted (or the daily purge) has got while it
  runs ("Emptying Recently deleted: 3,000 of 22,367"), Open Homebase folder, Browse files online (chat's web app on the
  identity, opened on its Filesync mini-app: `https://<identity>/apps/chat/#filesync`),
  Recently synced (the last 10; clicking one shows it
  in the file manager), Problems (the main folder's notes and its account's, newest first), Review N deletions…, Recently deleted…, Folders…, Log
  in again… (refused), Pause syncing (1 h, 8 h, 24 h, until resumed) or Resume, Sync now, Start
  at login, Open logs, Log out, Quit (keep syncing in the background, or stop syncing too).
  Syncing stopped: Start syncing, Open Homebase folder, Start at login, Open logs, Quit.
- **Pages:** logging in (the Homebase address, the folder, `~/Homebase` unless chosen with the
  OS's picker, merging a folder with files only when ticked; while the picker is open the page
  says a window opened, and on Windows, where the tray can't take the foreground from the
  browser, a hidden topmost window owns the picker to keep it in front), reviewing held deletes and
  Recently deleted (the topmost items: path, size, date, how many items their restore brings
  back, Restore; Empty now, after a confirmation), and the folders (each role where this
  computer keeps it, how many files and bytes it holds, counted for 2 s at most; Sync, or why
  it can't be: another app syncs it, a synced folder holds it; Stop syncing; Downloads marked
  not recommended; on macOS the folder is read once on Sync, so the system asks for access
  while the user is there) are
  pages the tray serves on `127.0.0.1`, on a random port, under a random path, and opens in the
  browser; the identity's approval returns there.
- **Notifications:** a new problem note, deletions to review (held deletes), a refused login;
  each kind at most once a
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
A stop takes 15 s at most, and a write in progress (Stopping, above): within launchd's default
of 20 s before it kills a job, and systemd's 90 s (`DefaultTimeoutStopSec`). The daemon's
launchd agent still sets `ExitTimeOut` to 60 s, room for a long write (a kill then is as a
crash). The tray's agent needs none: the tray quits at once, and the daemon it starts runs in
its own process group (so the daemon's agent is only there without the tray, as with the
command line alone).
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
| `hbsync setup <identity> [--folder P] [--existing] [--browser CMD] [--no-start] [--no-autostart]` | authorizes this computer (the owner approves in the browser) unless it already is and the identity still accepts it, saving the credentials at once, prepares the folder (default `~/Homebase`; it must be new, empty, already this identity's, or `--existing` to merge its files with the drive), turns autostart on, and has the daemon add the folder (starting it when it isn't running; restarting it when it logged in again, so the new credentials are used). When the app is registered but its drive is gone (deleted in the owner app), the owner is asked in the browser to give it back, only on a computer that never synced with the identity |
| `hbsync add <role> [--identity I] [--folder P]` | syncs one of the user's own folders (`documents`, `pictures`, `music`, `videos`, `downloads`) where the OS keeps it (Windows Known Folders, macOS home folders, XDG `user-dirs.dirs`), or at `--folder`, with the drive's role folder: joined if another device syncs it (identical files adopted), back from Recently deleted, or made. Refused where `setup` refuses a folder, inside or around another synced folder, when one of that role is synced already, and when OneDrive or iCloud syncs the folder. Without a running daemon it is set up for the next start |
| `hbsync remove <role> [--identity I]` | stops syncing that role folder; its files and state stay, so adding it again resumes |
| `hbsync status [--json]` | what the daemon does; exit status 3 when it isn't running |
| `hbsync sync`, `pause [--for 1h]`, `resume` | a pause with `--for` ends by itself |
| `hbsync recent [-n N]` | what was synced lately, newest first |
| `hbsync confirm-deletes [--yes]` | shows the deletions to review (held deletes) and lets them through |
| `hbsync put-back [--yes]` | shows the deletions to review and puts them back (sync semantics §7) |
| `hbsync start`, `stop` | `stop` stops syncing on this computer until `start` (or the tray's Start syncing), also across logins: neither the tray nor the service manager starts the daemon meanwhile |
| `hbsync autostart [on\|off]` | |
| `hbsync logout` | stops the daemon, forgets the credentials, autostart off; the folder stays |
| `hbsync unlink` | stops the daemon, removes the folder from the config, autostart off; its files, `.hbsync` and the account's DB stay, so setting it up again resumes |
| `hbsync deleted [-n N]` | Recently deleted, newest first, with node ids |
| `hbsync restore <path or node id>…` | restores removed items (a folder with what was deleted with it) |
| `hbsync empty-deleted [<path or node id>…] [--yes]` | deletes everything in Recently deleted for good, or the items named as it shows them (nothing removed in them since), after asking |
| `hbsync log` | the log folder (a daily file, a week kept) |

## Not yet

- several folders or identities;
- packaging, signing, auto-update;
- OS network-change notifications;
- revoking the device on the identity at logout (no endpoint known yet);
- structured per-item status and a diagnostics bundle (Observability).
