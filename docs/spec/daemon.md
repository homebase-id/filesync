# The daemon, its config and its control socket

How the product runs on a computer: one daemon per OS user (`hbsyncd`), set up and controlled
with the command line (`hbsync`). Code: `crates/hbsync-app` (library), `crates/hbsync`
(the two programs).

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
| lock, pid, pipe token | `$XDG_STATE_HOME/hbsync` (`~/.local/state/hbsync`) | same as config | `%LOCALAPPDATA%\Homebase Filesync` |
| logs | `<state>/logs` | `~/Library/Logs/Homebase Filesync` | `<state>\logs` |
| control socket | `$XDG_RUNTIME_DIR/hbsync`, else `<state>` | same as config | a named pipe |

The sync folder keeps its own state in `<folder>/.hbsync` (state DB, temp files, trash, ignore
rules). Its state DB records the identity it syncs with (`account.identity`): a folder synced
with one identity is never synced with another.

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
keychain works are moved into it. `HBSYNC_CREDENTIALS=file` uses the file only.

## The daemon

1. Takes the lock (`<state>/hbsyncd.lock`, an exclusive lock the OS drops when the process
   ends, with the pid in `hbsyncd.pid`): a second daemon says which process runs and exits.
2. Reads the config and the credentials. Not set up: it says so and exits.
3. Answers on the control socket from now on.
4. Connects to the identity, retrying with backoff (5 s doubling to 5 min) while it can't be
   reached; meanwhile the status says `offline` and why.
5. Runs the sync loop (`Runner`) until a stop request, SIGTERM or SIGINT.

Exit status: 0 when stopped or when there is nothing to do (not set up, already running), 1 on
a failure (service managers restart it then).

### The sync loop

- Syncs when the folder changes (live watcher), when the identity announces a change on the
  drive (notification socket), at least once a minute, and on request. After a failure it
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
- **Held deletes:** the status shows how many (deleted here; deleted elsewhere) and some paths;
  a confirmation covers at most as many as were shown (sync semantics, the mass-delete guard).

### States

`starting` (catching up), `syncing`, `idle`, `paused`, `offline` (can't reach the identity),
`failing` (another failure, or the folder gone), `denied` (the identity refused this device:
`hbsync setup` again), `stopped`.

## Control socket

One request line, one reply line (JSON), then the connection closes. Requests carry the protocol
version (now 1); a daemon refuses another one with the reason (after an update: restart it).

```json
{"protocol":1,"ask":"status"}
{"protocol":1,"ask":{"control":"sync_now"}}            // pause, resume, stop
{"protocol":1,"ask":{"control":{"confirm_deletes":{"here":60,"remotely":0}}}}
```

Replies: `{"status":{...}}`, `"done"`, `{"refused":"why"}`.

Only the user can connect:

- Unix: `hbsyncd.sock` in a 0700 folder. A path too long for a socket address (over 100 bytes,
  a deep `HBSYNC_HOME`) moves to `<temp>/hbsync-<hash of the folder>/`, which both sides check
  is a 0700 folder of this user.
- Windows: the named pipe `hbsync-<token>`, with a protected DACL granting only its owner and
  SYSTEM. The token is random, kept in `<state>/hbsyncd.pipe`, so another user can't squat the
  name. Named pipes have no read timeout.

## Autostart

| | how | restarts |
|---|---|---|
| macOS | launchd agent `~/Library/LaunchAgents/cloud.homebase.filesync.plist`, `launchctl bootstrap gui/<uid>` | after a crash (`KeepAlive`/`SuccessfulExit` = false) |
| Linux | systemd user unit `~/.config/systemd/user/hbsync.service`, enabled | `Restart=on-failure` |
| Windows | `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`, value `Homebase Filesync` | no |

Windows uses the `Run` key rather than a logon task: creating an `ONLOGON` task needs an
administrator **[inferred, not tried]**. `hbsyncd.exe` has no console window. `HBSYNC_HOME` and
`HBSYNC_CREDENTIALS` are passed on in the plist and the unit (not on Windows).

`hbsync setup` turns autostart on, `logout` and `unlink` turn it off, `hbsync autostart on|off`
by hand. With autostart on, `hbsync start` goes through launchd or systemd so that they supervise
the daemon. With `HBSYNC_HOME` set, only `hbsync autostart` touches the user's logon entry.

## Command line

| | |
|---|---|
| `hbsync setup <identity> [--folder P] [--existing] [--browser CMD] [--no-start] [--no-autostart]` | authorizes this computer (the owner approves in the browser) unless it already is, prepares the folder (default `~/Homebase`; it must be new, empty, already this identity's, or `--existing` to merge its files with the drive), saves the config, turns autostart on, (re)starts the daemon |
| `hbsync status [--json]` | what the daemon does; exit status 3 when it isn't running |
| `hbsync sync`, `pause`, `resume` | |
| `hbsync confirm-deletes [--yes]` | shows the held deletes and lets them through |
| `hbsync start`, `stop` | |
| `hbsync autostart [on\|off]` | |
| `hbsync logout` | stops the daemon, forgets the credentials, autostart off; the folder stays |
| `hbsync unlink` | stops the daemon, removes the folder from the config, autostart off; its files and `.hbsync` stay, so setting it up again resumes |
| `hbsync log` | the log folder (a daily file, a week kept) |

## Not yet

- several folders or identities;
- a tray or menubar app, packaging, signing, auto-update;
- OS network-change notifications;
- revoking the device on the identity at logout (no endpoint known yet);
- structured per-item status and a diagnostics bundle (Observability).
