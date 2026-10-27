# notify 8.2.0, patched for Homebase Filesync

The crates.io release of notify 8.2.0 (CC0-1.0, `LICENSE-CC0`), used through `[patch.crates-io]`
in the workspace's `Cargo.toml`. One change, in `src/windows.rs`, marked `hbsync:`:

- The Windows watcher thread waits on its wake-up semaphore without a timeout (`INFINITE`)
  instead of 100 ms, so it sleeps while nothing changes instead of waking ten times a second
  (measured on Windows 10: 9.7 of the daemon's 10.2 wakeups a second idle). The directory
  change completions still run: the wait is alertable.
- `configure` wakes the thread, as every other action already did; with no timeout it would
  otherwise wait for the next file event.

Drop the patch once upstream notify waits without a timeout.
