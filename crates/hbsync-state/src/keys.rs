//! Every setting the state DB keeps, in one place: the account's (`kv`, one per DB) and each
//! folder's (`folder_kv`, gone with the folder). A key says whose it is, so it can only be
//! read and written where it belongs.

/// A setting of the account ([`crate::Store::account_kv`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountKey(pub(crate) &'static str);

/// A setting of a folder ([`crate::Store::get_kv`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FolderKey(pub(crate) &'static str);

/// The identity the account's DB belongs to.
pub const IDENTITY: AccountKey = AccountKey("account.identity");
/// The remote change feed's cursor.
pub const REMOTE_CURSOR: AccountKey = AccountKey("remote.cursor");
/// When Recently deleted was last purged (Unix s).
pub const PURGED_AT: AccountKey = AccountKey("deleted.purged_at");
/// The last removal's place in this device's order of recording them ([`crate::RemovedRow::seq`]).
pub(crate) const REMOVED_SEQ: AccountKey = AccountKey("deleted.seq");
/// Which login the stored file tokens are sealed under (a fingerprint of its key).
pub const TOKENS_SEALED_BY: AccountKey = AccountKey("remote.tokens_sealed_by");
/// How often the DB was flushed to the disk on purpose (the write such a commit syncs).
pub(crate) const FLUSHES: AccountKey = AccountKey("db.flushes");

/// The local change journal's cursor (USN, FSEvents).
pub const JOURNAL_CURSOR: FolderKey = FolderKey("local.journal_cursor");
/// The ignore rules the last scan applied.
pub const FILTER: FolderKey = FolderKey("filter.fingerprint");
/// What the last run left out of the drive (sync semantics §12).
pub const SCOPE: FolderKey = FolderKey("scope.fingerprint");
/// When the purge started emptying the folder's trash (Unix s).
pub const TRASH_PURGED_SINCE: FolderKey = FolderKey("trash.purged_since");
