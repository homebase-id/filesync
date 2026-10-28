# tray-icon 0.26.1, patched for Homebase Filesync

The crates.io release of tray-icon 0.26.1 (MIT OR Apache-2.0, `LICENSE-MIT`, `LICENSE-APACHE`;
upstream commit `17e3196a15aa6174a21588ac4c5a52e322878479`, `.cargo_vcs_info.json`), byte for byte
apart from this file and `src/platform_impl/windows/mod.rs` (`diff -r` against the registry copy
shows the patch; cargo's `.cargo-ok` and the crate's own `Cargo.lock` left out: the workspace's
resolves it), used through `[patch.crates-io]` in the
workspace's `Cargo.toml` (`hbsync-tray` asks for exactly `=0.26.1`, so a newer release can't
leave the patch unused). The change in `src/platform_impl/windows/mod.rs`, marked `hbsync:`:

- The timer that finds the cursor leaving the icon (15 ms, set as the cursor moves over it)
  reads the last position instead of taking it. Upstream took it on the first tick, so with the
  cursor still on the icon then (a hover, a click), every later tick found none and never
  stopped the timer: the tray's main thread woke every 10–15 ms for good while idle (measured
  on Windows 10 with 0.10.2: 89–94 a second, quiet at 0.09 without it), until the cursor
  happened to cross the icon again. Now the timer stops once the cursor is off the icon, at
  once if there is no position to track, and when the cursor or the icon can't be found (a
  locked screen, the icon in a closed overflow area, explorer restarting): those count as left.
  While the cursor rests on the icon, the timer still fires (as upstream's does).

Proposed upstream as the same change: https://github.com/tauri-apps/tray-icon/pull/393 (branch
`Seifert69:fix-windows-leave-timer`).

A path crate isn't covered by cargo-deny's advisory and yank checks: look at upstream
tray-icon's releases and advisories when updating dependencies. Drop the patch once upstream
stops the timer (`tauri-apps/tray-icon`, `WM_USER_LEAVE_TIMER_ID`).
