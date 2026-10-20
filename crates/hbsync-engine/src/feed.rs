//! Pulling the remote change feed into the mirror.
//!
//! Pages are applied in feed order, each in one transaction together with the cursor, so a
//! crash replays at most one page (and applying a page twice is harmless). Every node a page
//! changes is marked dirty; a version the mirror has already (this device's own write coming
//! back) changes nothing. The feed is drained completely before planning, which is what
//! makes the mirror complete enough to call an unknown parent truly gone.

use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;

use hbsync_remote::{
    ChangeRef, ChangesPage, RefsPage, Remote, RemoteBody, RemoteError, RemoteHeader,
};
use hbsync_state::{MirrorRow, RemovedRow, StateDb, StateError, Store, keys};
use hbsync_types::activity::removal_time;
use hbsync_types::{FileToken, NodeId, NodeKind, RemoteFileId, RemoteRef};

/// How long the feed keeps a trace of a delete for good, Unix ms: what was removed (or became
/// a tombstone) this long ago may be deleted hard (node schema, Recently deleted), and a device
/// away this long reads the whole feed again.
pub const TRACE_MS: i64 = 30 * DAY_MS;
/// A device away this long reads the whole feed again: a day before [`TRACE_MS`], for a
/// server answer's time and its clock that differ.
const REREAD_MS: i64 = TRACE_MS - DAY_MS;
const DAY_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error(transparent)]
    Remote(#[from] RemoteError),
    #[error(transparent)]
    State(#[from] StateError),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FeedReport {
    pub pages: usize,
    pub live: usize,
    pub deleted: usize,
    /// Rows the client can't read (not encrypted, another key, not a sync node), with why.
    pub skipped: Vec<String>,
    /// The server's clock at the last page read, Unix ms.
    pub server_time: Option<i64>,
}

impl std::ops::AddAssign for FeedReport {
    fn add_assign(&mut self, other: FeedReport) {
        self.pages += other.pages;
        self.live += other.live;
        self.deleted += other.deleted;
        self.skipped.extend(other.skipped);
        self.server_time = self.server_time.max(other.server_time);
    }
}

/// Reads the feed from the stored cursor until it has no more rows. The DB is taken from `db`
/// for each step, and let go while a page is on its way: others use it meanwhile.
///
/// A page is read slim first ([`Remote::change_refs`]: which version of which file changed).
/// If the store knows every row (a version it has: this device's own writes coming back; a
/// tombstone), the page is applied from them alone (this device's own writes are known, with
/// their times, from their answers; a time an answer didn't give comes from the row). A page with anything new is
/// read again whole from the same cursor, and the feed continues from that page's cursor (a
/// file changed in between has moved further along the feed). Changes from elsewhere come in
/// runs, so after a page with something new the next is read whole at once, as is a first read
/// (no cursor: all of it is new), until a whole page holds nothing new.
///
/// A delete for good leaves nothing in the feed once the feed had it for [`TRACE_MS`] (node
/// schema, Recently deleted): a device that hasn't read the feed for nearly that long (by the
/// server's clock: the cursor's time, or [`keys::READ_AT`], against the first page's; decided
/// before that page is applied) reads all of it again ([`Reread`]).
///
/// Slim pages may be longer than whole ones ([`Pages`]): after a whole page they are as long,
/// and each slim page applied doubles them. A slim page read again whole is read as far as a
/// whole page goes, and the feed continues from there; its rows before the first new one, when
/// a whole page wouldn't reach it, are read again slim first.
pub fn pull<G: DerefMut<Target = StateDb>>(
    db: impl Fn() -> G,
    remote: &dyn Remote,
    pages: impl Into<Pages>,
    reread: &mut Option<Reread>,
) -> Result<FeedReport, FeedError> {
    let mut report = read_pages(&db, remote, pages.into(), reread)?;
    // A re-read open: what is left of it asked about (once nothing is, it ends).
    if let Some(asking) = reread.as_ref().map(Reread::to_ask) {
        report.deleted += settle(&db, reread, ask(remote, &asking))?;
    }
    Ok(report)
}

/// Rows of the feed read per request, at most: a slim page's rows are a fraction of a whole
/// one's, in bytes and in the work they take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pages {
    pub whole: usize,
    pub slim: usize,
}

impl From<usize> for Pages {
    /// Slim and whole pages alike.
    fn from(rows: usize) -> Pages {
        Pages {
            whole: rows,
            slim: rows,
        }
    }
}

/// The pages of [`pull`], without the re-read's questions ([`Reread::to_ask`], [`ask`],
/// [`settle`]), which come after them, in the same pull: a header read and its write in one
/// pull, so no other pull or announcement comes between them.
fn read_pages<G: DerefMut<Target = StateDb>>(
    db: impl Fn() -> G,
    remote: &dyn Remote,
    pages: Pages,
    reread: &mut Option<Reread>,
) -> Result<FeedReport, FeedError> {
    let mut report = FeedReport::default();
    let (rereading, cursor, read_at) = db().read(|s| {
        Ok((
            s.rereading()?,
            s.account_kv(keys::REMOTE_CURSOR)?,
            s.account_kv(keys::READ_AT)?
                .and_then(|v| v.parse::<i64>().ok()),
        ))
    })?;
    if !rereading {
        *reread = None;
    } else if reread.is_none() {
        // Asked (an upgrade), or one a new process doesn't know how far it got: from the start.
        *reread = Some(db().write(Reread::begin)?);
    }
    // Read until then, by the server's clock: what the cursor and the last day's mark say, or
    // when the re-read on its way began.
    let mut away_from = match reread {
        Some(r) => r.since,
        None => cursor
            .as_deref()
            .and_then(|c| remote.cursor_time(c))
            .max(read_at),
    };
    let mut slim = None;
    // Slim pages start as long as whole ones after a whole page, and grow in a run of them.
    let whole_rows = pages.whole.max(1);
    let most_slim = pages.slim.max(whole_rows);
    let mut slim_rows = whole_rows;
    // The rows before the first new one of a slim page, read again to apply them slim. Counted
    // as the server returned them (a row filtered out for this client may make the read stop
    // short of the new one: the whole page that follows then starts on known rows).
    let mut known_rows: Option<usize> = None;
    loop {
        let cursor = db().read(|s| s.account_kv(keys::REMOTE_CURSOR))?;
        let cursor = cursor.as_deref();
        let page = if *slim.get_or_insert(cursor.is_some()) {
            let rows = known_rows.unwrap_or(slim_rows);
            Page::Slim(with_retries(|| remote.change_refs(cursor, rows))?)
        } else {
            Page::Whole(with_retries(|| remote.changes(cursor, whole_rows))?)
        };
        let now = page.server_time();
        report.server_time = Some(now);
        if let Some(r) = reread.as_mut() {
            r.since.get_or_insert(now);
        }
        if let Some(from) = away_from.take()
            && now.saturating_sub(from) > REREAD_MS
        {
            *reread = Some(Reread {
                since: Some(now),
                ..db().write(Reread::begin)?
            });
            slim = None;
            continue;
        }
        // A slim page applies on its own, or else is read whole. Its rows before the first new
        // one, if a whole page wouldn't reach it, are read again slim first.
        let slim_applied = match &page {
            Page::Slim(refs) => match apply_slim(&db, refs, cursor, &mut report)? {
                Slim::Applied(has_more) => {
                    // Shown only once applied: read again, it may go less far.
                    if let Some(r) = reread.as_mut() {
                        r.shown(refs.refs.iter().map(|r| r.file_id));
                    }
                    Some(has_more)
                }
                // Each such read asks for fewer rows: it ends.
                Slim::NewAt(i) if i >= whole_rows && known_rows.is_none_or(|k| i < k) => {
                    known_rows = Some(i);
                    continue;
                }
                Slim::NewAt(_) => None,
            },
            Page::Whole(_) => None,
        };
        let before_new = known_rows.take().is_some();
        let (has_more, nothing_new) = match slim_applied {
            // The rows before a new one: it comes next.
            Some(has_more) if before_new => (has_more, false),
            Some(has_more) => {
                slim_rows = slim_rows.saturating_mul(2).min(most_slim);
                (has_more, true)
            }
            None => {
                slim_rows = whole_rows;
                let whole = match page {
                    Page::Whole(whole) => whole,
                    Page::Slim(_) => with_retries(|| remote.changes(cursor, whole_rows))?,
                };
                if let Some(r) = reread.as_mut() {
                    r.shown(whole.entries.iter().map(|h| h.file_id));
                }
                apply_whole(&db, whole, cursor, &mut report)?
            }
        };
        slim = Some(nothing_new);
        report.pages += 1;
        if !has_more {
            break;
        }
    }
    let mark = report
        .server_time
        .filter(|now| read_at.is_none_or(|at| now.saturating_sub(at) > DAY_MS));
    if let Some(now) = mark {
        db().write(|s| s.set_account_kv(keys::READ_AT, &now.to_string()))?;
    }
    Ok(report)
}

/// Files a re-read asks the server about per pull, at most ([`Reread`]): one pull at a time
/// holds the drive, so it doesn't for long.
const CONFIRM_PER_PULL: usize = 100;

/// A re-read of the whole feed ([`Store::start_reread`]) on its way: the files the store had
/// when it began that the feed hasn't shown yet, and when it began (the server's clock). The
/// caller keeps it between pulls, so a pull an error cut short goes on where it stopped (unless
/// the re-read is as old as an absence that starts one again); a new process starts it over.
#[derive(Debug)]
pub struct Reread {
    missing: HashSet<RemoteFileId>,
    since: Option<i64>,
}

impl Reread {
    /// Starts one: the cursor goes, and what the store has now is what the feed must show. Its
    /// time comes with the first page.
    fn begin(s: &Store<'_>) -> Result<Reread, StateError> {
        s.start_reread()?;
        Ok(Reread {
            missing: s.drive_file_ids()?.into_iter().collect(),
            since: None,
        })
    }

    fn shown(&mut self, files: impl Iterator<Item = RemoteFileId>) {
        for f in files {
            self.missing.remove(&f);
        }
    }

    /// Its pages read, the files the store had that the feed didn't show: to ask the server
    /// about ([`ask`]), [`CONFIRM_PER_PULL`] at most.
    fn to_ask(&self) -> Vec<RemoteFileId> {
        self.missing
            .iter()
            .take(CONFIRM_PER_PULL)
            .copied()
            .collect()
    }
}

/// What the server said of the files a re-read asked about ([`ask`]).
#[derive(Debug, Default)]
struct Asked {
    gone: Vec<RemoteFileId>,
    found: Vec<RemoteHeader>,
    answered: Vec<RemoteFileId>,
}

/// Asks the server about `files`, which the feed didn't show (it leaves out rows it has: one
/// changed in its last second, homebase-api-notes): gone only if it says not found (for this
/// app, the file isn't there); one it has is applied as it is ([`settle`]). One it can't
/// tell about now (a broken connection) stops the asking: it and the rest are asked again
/// later. What it found is written by [`settle`], in the same pull.
fn ask(remote: &dyn Remote, files: &[RemoteFileId]) -> Asked {
    let mut asked = Asked::default();
    for &file_id in files {
        match remote.header(file_id) {
            Err(RemoteError::NotFound) => asked.gone.push(file_id),
            Ok(header) => asked.found.push(header),
            Err(RemoteError::Fatal(why)) => {
                tracing::warn!("{file_id:?} isn't in the feed and doesn't read: {why}");
            }
            Err(e) => {
                tracing::info!("{file_id:?} isn't in the feed; asked again later: {e}");
                break;
            }
        }
        asked.answered.push(file_id);
    }
    asked
}

/// Writes what [`ask`] found: a file gone is gone here too, one there is applied as it is
/// (one that isn't a node this client reads stays as it is; one deleted for good since stays
/// gone, [`Store::mark_file_gone`]). Each file answered is the re-read's no more once that is
/// written (a write that fails leaves them to ask again): once none is left, it ends. Returns
/// how many went.
fn settle<G: DerefMut<Target = StateDb>>(
    db: &impl Fn() -> G,
    reread: &mut Option<Reread>,
    asked: Asked,
) -> Result<usize, FeedError> {
    let done = reread.as_ref().is_some_and(|r| {
        let answered = asked
            .answered
            .iter()
            .filter(|f| r.missing.contains(f))
            .count();
        answered == r.missing.len()
    });
    db().write(|s| {
        for &file_id in &asked.gone {
            apply_tombstone(s, file_id)?;
        }
        for header in &asked.found {
            apply(s, header)?;
        }
        if done {
            s.end_reread()?;
        }
        Ok(())
    })?;
    if let Some(r) = reread.as_mut() {
        for file_id in &asked.answered {
            r.missing.remove(file_id);
        }
    }
    if done {
        *reread = None;
    }
    Ok(asked.gone.len())
}

/// A page of the feed as read: slim, or whole.
enum Page {
    Slim(RefsPage),
    Whole(ChangesPage),
}

impl Page {
    fn server_time(&self) -> i64 {
        match self {
            Page::Slim(p) => p.server_time,
            Page::Whole(p) => p.server_time,
        }
    }
}

/// What became of a slim page ([`apply_slim`]).
enum Slim {
    /// It held nothing new, and was applied: whether more follow.
    Applied(bool),
    /// Its first new row (0 when one didn't read): nothing applied.
    NewAt(usize),
}

/// A slim page, applied if it holds nothing new.
fn apply_slim<G: DerefMut<Target = StateDb>>(
    db: &impl Fn() -> G,
    page: &RefsPage,
    cursor: Option<&str>,
    report: &mut FeedReport,
) -> Result<Slim, FeedError> {
    // Checked where it is applied: a commit in between may have changed what is known.
    Ok(db().write(|s| {
        if page.unread > 0 {
            return Ok(Slim::NewAt(0));
        }
        if let Some(i) = first_unknown(s, &page.refs)? {
            return Ok(Slim::NewAt(i));
        }
        for r in &page.refs {
            report.count(apply_ref(s, r)?);
        }
        save_cursor(s, cursor, !page.refs.is_empty(), &page.cursor)?;
        Ok(Slim::Applied(page.has_more))
    })?)
}

/// A whole page, applied: whether more follow, and whether it held nothing new.
fn apply_whole<G: DerefMut<Target = StateDb>>(
    db: &impl Fn() -> G,
    mut page: ChangesPage,
    cursor: Option<&str>,
    report: &mut FeedReport,
) -> Result<(bool, bool), FeedError> {
    report.skipped.append(&mut page.skipped);
    let nothing_new = db().write(|s| {
        let refs: Vec<ChangeRef> = page.entries.iter().map(ChangeRef::of).collect();
        let nothing_new = known(s, &refs)?;
        for header in &page.entries {
            report.count(apply(s, header)?);
        }
        save_cursor(s, cursor, !page.entries.is_empty(), &page.cursor)?;
        Ok(nothing_new)
    })?;
    Ok((page.has_more, nothing_new))
}

impl FeedReport {
    fn count(&mut self, live: bool) {
        if live {
            self.live += 1;
        } else {
            self.deleted += 1;
        }
    }
}

/// Whether the store knows every one of `refs` ([`first_unknown`]).
fn known(s: &Store<'_>, refs: &[ChangeRef]) -> Result<bool, StateError> {
    Ok(first_unknown(s, refs)?.is_none())
}

/// The first of `refs` the store doesn't know: a tombstone needs nothing, the rest are
/// versions it has, in Recently deleted or else in the mirror.
fn first_unknown(s: &Store<'_>, refs: &[ChangeRef]) -> Result<Option<usize>, StateError> {
    let (at, versions): (Vec<usize>, Vec<(RemoteRef, bool)>) = refs
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.deleted)
        .map(|(i, r)| (i, (r.at(), r.removed_at.is_some())))
        .unzip();
    Ok(s.first_unknown(&versions)?.map(|i| at[i]))
}

/// Stores the cursor after a page; unchanged on an idle pull: no write, so nothing to sync.
fn save_cursor(s: &Store<'_>, old: Option<&str>, rows: bool, new: &str) -> Result<(), StateError> {
    if rows || old != Some(new) {
        s.set_account_kv(keys::REMOTE_CURSOR, new)?;
    }
    Ok(())
}

/// Changes the notifications announced (rows as a slim page has them): a file deleted for good
/// is gone at once (a hard delete leaves no row in the feed), and when the store knows every
/// other version (this device's own writes coming back) they are applied as a slim page's,
/// without reading the feed. Whether that was all; else the feed has to be read.
pub(crate) fn announced(s: &Store<'_>, refs: &[ChangeRef]) -> Result<bool, StateError> {
    let (refs, sure) = latest(refs);
    for r in refs.iter().filter(|r| r.deleted) {
        match r.changed_at {
            // A tombstone on the drive (a soft delete), recorded as the feed's would be.
            Some(at) => tombstone(s, r.at(), Some(at))?,
            // Deleted for good, nothing left (a hard delete names only the file as it was): a
            // tombstone this device holds at that version has nothing left to delete.
            None => {
                s.delete_tombstone_at(r.at())?;
                apply_tombstone(s, r.file_id)?;
            }
        }
    }
    if !sure || !known(s, &refs)? {
        return Ok(false);
    }
    for r in refs.iter().filter(|r| !r.deleted) {
        apply_ref(s, r)?;
    }
    Ok(true)
}

/// One ref per file: the last a file had. A tombstone ends a file (its id never comes back,
/// `homebase-api-notes.md`); of the others the newest by the server's time, the later
/// announced on a tie. An own write made twice, or made then deleted, needs no read. Whether
/// that is sure: two writes of a file, one of them without a time (a removal whose row didn't
/// say when, `i64::MAX`), can't be told apart: in doubt, the feed tells.
fn latest(refs: &[ChangeRef]) -> (Vec<ChangeRef>, bool) {
    let time = |r: &ChangeRef| {
        [r.changed_at, r.removed_at]
            .into_iter()
            .flatten()
            .filter(|t| *t != i64::MAX)
            .max()
    };
    let mut sure = true;
    let mut last: HashMap<RemoteFileId, ChangeRef> = HashMap::new();
    for r in refs {
        match last.get(&r.file_id) {
            Some(had) if had.deleted => {}
            Some(had) if !r.deleted && (time(had).is_none() || time(r).is_none()) => {
                sure = false;
                last.insert(r.file_id, *r);
            }
            Some(had) if !r.deleted && time(had) > time(r) => {}
            _ => {
                last.insert(r.file_id, *r);
            }
        }
    }
    (last.into_values().collect(), sure)
}

/// Applies a known ref ([`known`]) as [`apply`] would its header: only what the server says
/// better, its times. Returns `true` for a node in the tree.
fn apply_ref(s: &Store<'_>, r: &ChangeRef) -> Result<bool, StateError> {
    if r.deleted {
        tombstone(s, r.at(), r.changed_at)?;
        return Ok(false);
    }
    apply_known(s, r, None)
}

/// A version the store has already (this device's own write coming back, or a page applied
/// again) changes nothing but what the server says better: the file's `token` where the
/// header gives one (an earlier login's may not open), a removal's time, and the time of a
/// version in a removed folder, where it decides whether it went with it (node schema,
/// Recently deleted). No change of the node, so nothing to reconcile. Returns `true` for a node
/// in the tree.
fn apply_known(
    s: &Store<'_>,
    r: &ChangeRef,
    token: Option<&FileToken>,
) -> Result<bool, StateError> {
    if let Some(token) = token {
        s.set_token(r.file_id, r.removed_at.is_some(), token)?;
    }
    if let Some(removed_at) = r.removed_at {
        if let Some(at) = known_time(removed_at) {
            s.confirm_removed(r.at(), at)?;
        }
        return Ok(false);
    }
    if let (Some(changed_at), Some(parent)) = (r.changed_at, r.parent) {
        s.refresh_time(r.file_id, parent, changed_at)?;
    }
    Ok(true)
}

/// Retries transient errors a few times with exponential backoff (50, 100, 200 ms).
pub fn with_retries<T>(mut f: impl FnMut() -> Result<T, RemoteError>) -> Result<T, RemoteError> {
    let mut delay = std::time::Duration::from_millis(50);
    for _ in 0..3 {
        match f() {
            Err(e) if e.is_transient() => {
                std::thread::sleep(delay);
                delay *= 2;
            }
            other => return other,
        }
    }
    f()
}

/// A removal's time, unless the remote didn't say ([`removal_time`]`(None)`: the newest, open
/// in the store until a version that says).
fn known_time(removed_at: i64) -> Option<i64> {
    (removed_at != removal_time(None)).then_some(removed_at)
}

/// Applies one header to the mirror. Returns `true` for a node in the tree, `false` for one
/// that left it (deleted, or removed to Recently deleted).
pub fn apply(s: &Store<'_>, header: &RemoteHeader) -> Result<bool, StateError> {
    if header.is_deleted() {
        tombstone(s, header.at(), header.changed_at)?;
        return Ok(false);
    }
    // A live file without its payload changes nothing: it must never read as a delete.
    let Some(row) = mirror_row(header) else {
        return Ok(true);
    };
    // The store marks the nodes that changed dirty where that concerns.
    let removed_at = header.removed_at;
    if s.has_version(row.remote_ref(), removed_at.is_some())? {
        return apply_known(s, &ChangeRef::of(header), header.token.as_ref());
    }
    if let Some(removed_at) = removed_at {
        // Out of the tree for the reconciler, as a tombstone is; restorable meanwhile.
        gone(s, header.file_id)?;
        s.put_removed(&RemovedRow {
            row,
            removed_at: known_time(removed_at),
        })?;
        return Ok(false);
    }
    s.delete_removed(row.node)?;
    s.put_mirror(&row)?;
    Ok(true)
}

/// A tombstone in the feed, written at `at` (the server's time): the file is deleted for good,
/// and the tombstone itself goes hard once old enough ([`crate::deleted::purge`]).
fn tombstone(s: &Store<'_>, tombstone: RemoteRef, at: Option<i64>) -> Result<(), StateError> {
    if let Some(at) = at {
        s.put_tombstone(tombstone, at)?;
    }
    apply_tombstone(s, tombstone.file_id)
}

/// Whether a file a write was refused with `e` (not found, or a tombstone) is deleted for good:
/// a tombstone's answer says so, else its header is read ([`gone_for_good`]).
pub(crate) fn gone_after(
    remote: &dyn Remote,
    e: &RemoteError,
    file_id: RemoteFileId,
) -> Result<bool, RemoteError> {
    match e {
        RemoteError::Deleted => Ok(true),
        _ => gone_for_good(remote, file_id),
    }
}

/// Whether the drive has file `file_id` no more (not found, or only its tombstone), by its
/// header: a write or read answered not found may have other reasons.
pub(crate) fn gone_for_good(
    remote: &dyn Remote,
    file_id: RemoteFileId,
) -> Result<bool, RemoteError> {
    match remote.header(file_id) {
        Ok(header) => Ok(header.is_deleted()),
        Err(RemoteError::NotFound) => Ok(true),
        Err(e) => Err(e),
    }
}

/// A file deleted for good: out of Recently deleted and the mirror, and never back in them
/// ([`Store::mark_file_gone`]).
pub(crate) fn apply_tombstone(s: &Store<'_>, file_id: RemoteFileId) -> Result<(), StateError> {
    s.mark_file_gone(file_id)?;
    s.delete_removed_file(file_id)?;
    gone(s, file_id)
}

/// The mirror without file `file_id`, its node marked to plan again where it was
/// held. A tombstone has no node id: the node is found by file id.
fn gone(s: &Store<'_>, file_id: RemoteFileId) -> Result<(), StateError> {
    if s.delete_mirror(file_id)?.is_none()
        && let Some(node) = s.node_of_file(file_id)?
    {
        // Its place went with its row: the folders holding it are the ones concerned.
        s.fan_out(node, NodeId::ROOT)?;
    }
    Ok(())
}

/// The mirror row of a live header (removed or not); `None` for tombstones and for files
/// without a payload.
pub fn mirror_row(header: &RemoteHeader) -> Option<MirrorRow> {
    let RemoteBody::Live {
        node,
        content,
        payload,
    } = &header.body
    else {
        return None;
    };
    let payload = match header.kind {
        NodeKind::Folder => None,
        // `Remote`s refuse such rows.
        NodeKind::File => Some(payload.as_ref()?),
    };
    let at = header.at();
    let hash = payload.map(|p| p.hash);
    let mut row = MirrorRow {
        token: header.token.clone(),
        changed_at: header.changed_at,
        ..MirrorRow::from_content(at, *node, header.parent, header.kind, content, hash)
    };
    // A content without its size (another client's) has the payload's.
    if let (Some(file), Some(p), None) = (&mut row.file, payload, content.size) {
        file.size = p.bytes;
    }
    Some(row)
}

#[cfg(test)]
mod tests {
    use hbsync_fileio::{HashAlgorithm, hash_bytes};
    use hbsync_remote::PayloadInfo;
    use hbsync_types::Uuid;
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeId, RemoteFileId, VersionTag};

    use super::*;

    /// An announced soft delete records its tombstone, as the feed's row would; only a delete
    /// for good (a ref without a time: a hard delete) of that version clears it.
    #[test]
    fn announced_deletes_keep_tombstones_as_the_feed_does() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut db = StateDb::open_in_memory()?;
        let at = RemoteRef {
            file_id: RemoteFileId(Uuid::now_v7()),
            version: VersionTag(Uuid::now_v7()),
        };
        let deleted = |changed_at| ChangeRef {
            file_id: at.file_id,
            version: at.version,
            deleted: true,
            parent: None,
            removed_at: None,
            changed_at,
        };
        db.write(|s| announced(s, &[deleted(Some(7))]).map(drop))?;
        assert_eq!(
            db.read(|s| s.tombstones_before(8))?,
            [at],
            "a tombstone on the drive"
        );
        db.write(|s| announced(s, &[deleted(None)]).map(drop))?;
        assert!(
            db.read(|s| s.tombstones_before(8))?.is_empty(),
            "deleted hard"
        );
        assert!(db.read(|s| s.file_gone(at.file_id))?);
        Ok(())
    }

    /// A write of a file the store has, then its removal without a time (`i64::MAX`), announced
    /// together: which came last can't be told, so the feed is read (the removal isn't lost to
    /// the write the store already has).
    #[test]
    fn a_write_and_a_removal_without_a_time_are_doubt() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let header = RemoteHeader {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            body: RemoteBody::Live {
                node: NodeId::new(),
                content: NodeContent::folder("d"),
                payload: None,
            },
            removed_at: None,
            changed_at: Some(5),
            token: None,
        };
        db.write(|s| apply(s, &header).map(drop))?;
        let write = ChangeRef::of(&header);
        let removal = ChangeRef {
            version: VersionTag(Uuid::new_v4()),
            removed_at: Some(i64::MAX),
            changed_at: None,
            ..write
        };
        for refs in [[write, removal], [removal, write]] {
            assert!(!db.write(|s| announced(s, &refs))?, "the feed is read");
        }
        assert!(
            db.write(|s| announced(s, &[write]))?,
            "the write alone is known"
        );
        Ok(())
    }

    /// A first read (no cursor) is read whole; a page of nothing new costs one slim read; a run
    /// of changes from elsewhere is read slim once, then whole page by page.
    #[test]
    fn news_is_read_whole_and_the_rest_slim() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_remote::NodeMeta;
        use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
        let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
        let folder = |name: String| NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            content: NodeContent::folder(&name),
            removed: false,
        };
        for i in 0..4 {
            remote.create(&folder(format!("a{i}")), None)?;
        }
        let db = std::sync::Mutex::new(StateDb::open_in_memory()?);
        let lock = || db.lock().unwrap_or_else(|e| e.into_inner());
        let reads = |r: &FakeRemote| {
            let f = r.faults();
            (f.ref_pages, f.full_pages)
        };
        pull(lock, &remote, 3, &mut None)?;
        assert_eq!(reads(&remote), (0, 2), "a first read: whole");
        pull(lock, &remote, 3, &mut None)?;
        assert_eq!(reads(&remote), (1, 2), "nothing new: slim");
        for i in 0..7 {
            remote.create(&folder(format!("b{i}")), None)?;
        }
        pull(lock, &remote, 3, &mut None)?;
        assert_eq!(reads(&remote), (2, 5), "news: slim once, then whole");
        assert_eq!(lock().read(|s| s.mirror_count())?, 11);
        // A slim row that can't be read: the page is read whole, the cursor goes on.
        remote.faults().unread_refs = true;
        pull(lock, &remote, 3, &mut None)?;
        assert_eq!(reads(&remote), (3, 6), "an unread row: whole");
        Ok(())
    }

    #[test]
    fn a_live_file_without_its_payload_is_no_delete() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let mut header = RemoteHeader {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            body: RemoteBody::Live {
                node: NodeId::new(),
                content: NodeContent::file("f", 0, 1, false),
                payload: Some(PayloadInfo {
                    hash: hash_bytes(b"x", HashAlgorithm::Blake3),
                    bytes: 1,
                }),
            },
            removed_at: None,
            changed_at: None,
            token: None,
        };
        assert!(db.write(|s| apply(s, &header))?);
        if let RemoteBody::Live { payload, .. } = &mut header.body {
            *payload = None;
        }
        assert!(db.write(|s| apply(s, &header))?);
        assert_eq!(
            db.read(|s| s.mirror_count())?,
            1,
            "the mirror keeps the file"
        );
        Ok(())
    }

    /// Another client's file without `size` in its content has its payload's.
    #[test]
    fn a_file_without_a_size_has_its_payloads() {
        let mut content = NodeContent::file("f", 0, 1, false);
        content.size = None;
        let header = RemoteHeader {
            file_id: RemoteFileId(Uuid::new_v4()),
            version: VersionTag(Uuid::new_v4()),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            body: RemoteBody::Live {
                node: NodeId::new(),
                content,
                payload: Some(PayloadInfo {
                    hash: hash_bytes(b"xyz", HashAlgorithm::Blake3),
                    bytes: 3,
                }),
            },
            removed_at: None,
            changed_at: None,
            token: None,
        };
        let size = mirror_row(&header).and_then(|r| r.file).map(|f| f.size);
        assert_eq!(size, Some(3));
    }

    /// The feed brings this device's own writes back: a version the mirror has is no change.
    #[test]
    fn a_known_version_marks_nothing() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let header = |version: u128, removed_at: Option<i64>| RemoteHeader {
            removed_at,
            ..folder(1, version, node, NodeId::ROOT)
        };
        let dirty_after = |db: &mut StateDb, h: &RemoteHeader| {
            db.write(|s| {
                s.clear_dirty(node)?;
                apply(s, h)?;
                s.dirty_count()
            })
        };
        for (version, removed_at) in [(1, None), (2, Some(9))] {
            let h = header(version, removed_at);
            assert_eq!(dirty_after(&mut db, &h)?, 1, "version {version} is news");
            assert_eq!(
                dirty_after(&mut db, &h)?,
                0,
                "version {version} again isn't"
            );
        }
        Ok(())
    }

    #[test]
    fn a_removal_leaves_the_tree_and_a_restore_returns() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let live = |removed_at: Option<i64>| RemoteHeader {
            version: VersionTag(Uuid::new_v4()),
            removed_at,
            ..folder(1, 0, node, NodeId::ROOT)
        };
        assert!(db.write(|s| apply(s, &live(None)))?);
        assert!(!db.write(|s| apply(s, &live(Some(9))))?);
        db.read(|s| {
            assert_eq!(s.get_mirror(node)?, None);
            let at = s.removed_by_node(node)?.and_then(|r| r.removed_at);
            assert_eq!(at, Some(9));
            Ok(())
        })?;
        assert!(db.write(|s| apply(s, &live(None)))?);
        db.read(|s| {
            assert!(s.get_mirror(node)?.is_some() && s.removed_by_node(node)?.is_none());
            Ok(())
        })
    }

    /// A folder's header at `version` (no token, no time): edit the fields a test needs.
    fn folder(file_id: u128, version: u128, node: NodeId, parent: NodeId) -> RemoteHeader {
        RemoteHeader {
            file_id: RemoteFileId(Uuid::from_u128(file_id)),
            version: VersionTag(Uuid::from_u128(version)),
            parent,
            kind: NodeKind::Folder,
            body: RemoteBody::Live {
                node,
                content: NodeContent::folder("d"),
                payload: None,
            },
            removed_at: None,
            changed_at: None,
            token: None,
        }
    }

    /// A version the mirror has already, again, changes nothing; its echo takes the token (an
    /// earlier login's may not open), and nothing needs reconciling. In Recently deleted too.
    #[test]
    fn a_known_version_takes_only_its_token() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let node = NodeId::new();
        let without = folder(1, 1, node, NodeId::ROOT);
        let token = |b: u8| Some(FileToken(vec![b; 16].into()));
        let with = |b: u8| RemoteHeader {
            token: token(b),
            ..without.clone()
        };
        db.write(|s| {
            apply(s, &without)?;
            s.clear_dirty(node)
        })?;
        let held =
            |db: &mut StateDb| db.read(|s| s.get_mirror(node).map(|m| m.and_then(|m| m.token)));
        db.write(|s| apply(s, &with(0xAA)))?;
        assert_eq!(held(&mut db)?, token(0xAA), "the token taken");
        assert!(!db.read(|s| s.is_dirty(node))?);
        // An earlier login's is replaced; none keeps the one there.
        db.write(|s| apply(s, &with(0xEE)))?;
        db.write(|s| apply(s, &without))?;
        assert_eq!(held(&mut db)?, token(0xEE));

        // In Recently deleted, the same; its time stays the one it has.
        let removed = RemoteHeader {
            version: VersionTag(Uuid::from_u128(2)),
            removed_at: Some(5),
            ..without.clone()
        };
        db.write(|s| {
            apply(s, &removed)?;
            s.clear_dirty(node)
        })?;
        let again = RemoteHeader {
            removed_at: Some(9),
            token: token(0xAA),
            ..removed
        };
        db.write(|s| apply(s, &again))?;
        let back = db.read(|s| s.removed_by_node(node))?.expect("removed");
        assert_eq!((back.row.token, back.removed_at), (token(0xAA), Some(5)));
        assert!(!db.read(|s| s.is_dirty(node))?);
        Ok(())
    }

    /// This device's own write has no server time until the feed brings it back, and the echo
    /// records it only where it decides something: in a folder in Recently deleted.
    #[test]
    fn an_own_writes_time_is_kept_in_a_removed_folder_only() -> Result<(), StateError> {
        let mut db = StateDb::open_in_memory()?;
        let top = folder(1, 1, NodeId::new(), NodeId::ROOT);
        let inner = folder(2, 2, NodeId::new(), top.node().expect("live"));
        let leaf = folder(3, 3, NodeId::new(), inner.node().expect("live"));
        let node = leaf.node().expect("live");
        db.write(|s| {
            [&top, &inner, &leaf]
                .into_iter()
                .try_for_each(|h| apply(s, h).map(drop))
        })?;
        let echo = RemoteHeader {
            changed_at: Some(7),
            ..leaf.clone()
        };
        let time = |db: &mut StateDb| {
            db.read(|s| s.get_mirror(node).map(|m| m.and_then(|m| m.changed_at)))
        };
        db.write(|s| apply(s, &echo))?;
        assert_eq!(time(&mut db)?, None, "nothing it decides");
        let removed = RemoteHeader {
            version: VersionTag(Uuid::from_u128(4)),
            removed_at: Some(5),
            ..top
        };
        db.write(|s| apply(s, &removed))?;
        db.write(|s| apply(s, &echo))?;
        assert_eq!(
            time(&mut db)?,
            Some(7),
            "in a removed folder, two levels up"
        );
        Ok(())
    }
}
