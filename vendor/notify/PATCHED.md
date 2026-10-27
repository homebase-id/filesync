# notify 8.2.0, patched for Homebase Filesync

The crates.io release of notify 8.2.0 (CC0-1.0, `LICENSE-CC0`), used through `[patch.crates-io]`
in the workspace's `Cargo.toml` (`hbsync-local` asks for exactly `=8.2.0`, so a newer
release can't leave the patch unused). Changes in `src/windows.rs`, marked `hbsync:`:

- The Windows watcher thread waits on its wake-up semaphore without a timeout (`INFINITE`)
  instead of 100 ms, so it sleeps while nothing changes instead of waking ten times a second
  (measured on Windows 10: 9.7 of the daemon's 10.2 wakeups a second idle). The directory
  change completions still run: the wait is alertable.
- `configure` wakes the thread, as every other action already did; with no timeout it would
  otherwise wait for the next file event. (An unwatch sent from a completion routine needs no
  release: running the routine ends the alertable wait.)
- A failed wait (neither the semaphore nor a completion) is logged and retried after 100 ms
  instead of spinning.
- The semaphore is shared by the watcher and its thread (`WakeupSem`) and closed by the last
  one: upstream's thread closed it when it stopped, while `Drop` could still release it.

`hbsync-local`'s `an_idle_watcher_answers_every_call` checks that every call returns with nothing
happening on disk (CI runs it on Windows).

A path crate isn't covered by cargo-deny's advisory and yank checks: look at upstream notify's
releases and advisories when updating dependencies. Drop the patch once upstream waits without
a timeout.
