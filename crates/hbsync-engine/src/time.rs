//! The engine's time rules (recovery's hold of a placement it can't judge yet stays with it:
//! `exec::UNHASHED_HOLD`). Times of the drive are the server's clock (Unix ms); the local clock
//! names only this device's own things (its purge's day).

use std::time::Duration;

pub(crate) const DAY_S: u64 = 24 * 60 * 60;
pub(crate) const DAY_MS: i64 = DAY_S as i64 * 1000;

/// How long the feed keeps a trace of a delete for good, Unix ms: what was removed (or became
/// a tombstone) this long ago may be deleted hard (node schema, Recently deleted), and a device
/// away this long reads the whole feed again.
pub const TRACE_MS: i64 = 30 * DAY_MS;

/// A device away this long reads the whole feed again: a day before [`TRACE_MS`], for a
/// server answer's time and its clock that differ.
pub(crate) const REREAD_MS: i64 = TRACE_MS - DAY_MS;

/// The days Recently deleted keeps an item (the same 30 days as [`TRACE_MS`], by the node
/// schema's two rules): the purge is the whole account's, and every client keeps 30 (node
/// schema, The purge). Not a setting: one device can't keep longer or shorter.
pub const KEEP_DAYS: u32 = 30;

/// How long uploads are held after the drive said it is full: then one is tried again.
pub(crate) const FULL_HOLD: Duration = Duration::from_secs(600);
/// The bounds of a hold the drive asked for (its 507's `Retry-After`).
const FULL_HOLD_MIN: Duration = Duration::from_secs(30);
const FULL_HOLD_MAX: Duration = Duration::from_secs(24 * 3600);

/// How long uploads are held after a 507 that said to try again `after` (if it said).
pub(crate) fn full_hold(after: Option<Duration>) -> Duration {
    after.map_or(FULL_HOLD, |after| after.clamp(FULL_HOLD_MIN, FULL_HOLD_MAX))
}
