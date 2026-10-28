//! Every setting the state DB keeps, in one place: the account's (`kv`, one per DB) and each
//! folder's (`folder_kv`, gone with the folder). A key says whose it is, so it can only be
//! read and written where it belongs.

/// A setting of the account ([`crate::ReadTx::account_kv`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountKey(pub(crate) &'static str);

/// A setting of a folder ([`crate::ReadTx::get_kv`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FolderKey(pub(crate) &'static str);

/// The identity the account's DB belongs to.
pub const IDENTITY: AccountKey = AccountKey("account.identity");
/// The remote change feed's cursor.
pub const REMOTE_CURSOR: AccountKey = AccountKey("remote.cursor");
/// Until when the feed was read to its end, at the latest (Unix ms, the server's clock; kept
/// a day at a time): with the cursor's time, how long this device was away.
pub const READ_AT: AccountKey = AccountKey("remote.read_at");
/// The whole feed is being read again ([`crate::AccountTx::start_reread`]).
pub(crate) const REREADING: AccountKey = AccountKey("remote.rereading");
/// When Recently deleted was last purged (Unix s).
pub const PURGED_AT: AccountKey = AccountKey("deleted.purged_at");
/// The last removal's place in this device's order of recording them ([`crate::Recorded::seq`]).
pub(crate) const REMOVED_SEQ: AccountKey = AccountKey("deleted.seq");
/// This DB's own id (random, set when it is made): a listing of its Recently deleted names it.
pub(crate) const DB_ID: AccountKey = AccountKey("db.id");
/// Which login the stored file tokens are sealed under (a fingerprint of its key).
pub const TOKENS_SEALED_BY: AccountKey = AccountKey("remote.tokens_sealed_by");
/// When the drive this DB syncs with was made (Unix ms, the server's clock): a drive found with
/// another time is another drive, made again, and the DB is set aside
/// ([`crate::Unusable::OtherDrive`]).
pub const DRIVE_MADE: AccountKey = AccountKey("remote.drive_made");
/// Why the DB this one replaced was set aside (`too-old`, `corrupt`, `other-drive`): one made so sets the
/// account's folders up again as it starts, whichever it didn't have yet.
pub const SET_ASIDE: AccountKey = AccountKey("db.set_aside");
/// How often the DB was flushed to the disk on purpose (the write such a commit syncs).
pub(crate) const FLUSHES: AccountKey = AccountKey("db.flushes");

/// The local change journal's cursor (USN, FSEvents).
pub const JOURNAL_CURSOR: FolderKey = FolderKey("local.journal_cursor");
/// The ignore rules the last scan applied.
pub const FILTER: FolderKey = FolderKey("filter.fingerprint");
/// What the last run left out of the drive (sync semantics §12).
pub const SCOPE: FolderKey = FolderKey("scope.fingerprint");
/// Nodes whose removal elsewhere the mass-delete guard held as every synced file, until the
/// user answers (sync semantics §7): their ids, comma-separated.
pub const WIPED: FolderKey = FolderKey("guard.wiped");
