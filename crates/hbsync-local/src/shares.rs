//! Which folder at a sync folder's top is its folder of shares (sync semantics §10): the one
//! that holds what other identities share with the account, this device's own.
//!
//! The marker in it ([`crate::layout::SHARED_MARKER`]) is a file anyone can copy, so it says
//! nothing alone. The sync folder's [`crate::STATE_DIR`] has the folder on record
//! ([`Record`]): the token its marker holds, and the folder's file id. The folder of shares is
//! the folder with that id holding a marker with that token. A rename, or a move within the
//! volume, keeps both; a marker copied elsewhere is in a folder with another id; a folder
//! that got the id of one deleted holds no such marker. A copy of the folder (a restore from a
//! backup) has another id: [`adopt`] takes it as the sync folder opens.
//!
//! Where the volume's file ids aren't stable (FAT, exFAT, a network or FUSE volume:
//! [`hbsync_platform::FsFamily::has_stable_file_ids`]) no id is recorded, and the token alone
//! tells: there a copied marker makes its folder one too (a known limit).

use std::io;
use std::path::Path;

use hbsync_platform::{EntryKind, FileId};

use crate::filter::{holds_marker, is_state_dir, top_key};
use crate::layout::{shared_marker, shares_file, tmp_dir};

/// The folder of shares on record for a sync folder, or none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record(Option<Known>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Known {
    /// What the folder's marker holds.
    token: String,
    /// The folder's file id on the sync folder's volume, and before it those of the folders
    /// that were on record earlier (the one a copy was taken for, [`adopt`]: it may still be
    /// somewhere in the sync folder, and stays the folder of shares where it is); none
    /// where ids aren't stable. Kept without the volume: on Unix a volume's number may
    /// change with a restart, and the folders they are compared with are in the sync folder.
    ids: Vec<u128>,
}

/// How many earlier file ids a record keeps beside the one of the folder on record now.
const EARLIER: usize = 7;

impl Known {
    /// Whether the folder at `dir` holds this record's marker (its token in it).
    fn marks(&self, dir: &Path) -> io::Result<bool> {
        Ok(token_in(dir)?.is_some_and(|token| token == self.token))
    }
}

impl Record {
    /// The file ids on record, the one of the folder on record now first: none where
    /// nothing is on record, or the volume's file ids aren't stable.
    pub fn ids(&self) -> &[u128] {
        self.0.as_ref().map_or(&[], |known| &known.ids)
    }

    /// The record of the sync folder at `root`: none where nothing was recorded. One that
    /// can't be read, or isn't one, is an error: a folder of shares taken for a plain one
    /// would be sent.
    pub fn read(root: &Path) -> io::Result<Record> {
        let path = shares_file(root);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Record(None)),
            Err(e) => return Err(e),
        };
        let field = |name: &str| {
            let value = text.lines().find_map(|l| l.strip_prefix(name));
            value.map(str::trim).filter(|v| !v.is_empty())
        };
        // A line that starts as an id's must hold one: never taken for a record without.
        let ids = text.lines().filter(|l| l.starts_with("folder"));
        let ids = ids.map(|l| l.strip_prefix("folder ").unwrap_or("").trim().parse());
        match (field("token "), ids.collect::<Result<Vec<u128>, _>>()) {
            (Some(token), Ok(ids)) => Ok(Record(Some(Known {
                token: token.to_owned(),
                ids,
            }))),
            _ => {
                let what = format!("{}: not a record of a folder of shares", path.display());
                Err(io::Error::new(io::ErrorKind::InvalidData, what))
            }
        }
    }

    /// Records the folder at `dir`, whose marker holds `token`, as the folder of shares of the
    /// sync folder at `root`, in one step: with its file id where the volume's are stable,
    /// and after it the ids on record `before` for this token (the last few: the folder a
    /// copy is taken for may still be in the sync folder, and is never sent either). A
    /// volume whose kind can't be told is an error: a record without the id would let any
    /// folder holding the marker pass for the folder of shares, for good.
    fn write(root: &Path, token: &str, dir: &Path, before: &[u128]) -> io::Result<()> {
        let stable = hbsync_platform::fskind::detect(root)?.has_stable_file_ids();
        let mut text = format!("token {token}\n");
        if stable {
            let id = hbsync_platform::stat(dir)?.file_id.file;
            let earlier = before.iter().filter(|&&was| was != id).take(EARLIER);
            for id in std::iter::once(&id).chain(earlier) {
                text += &format!("folder {id}\n");
            }
        }
        // (The state folder in the sync folder that is there, never that one with it.)
        hbsync_fileio::make_dir_in_place(&crate::layout::state_dir(root))?;
        hbsync_fileio::write_atomic(&shares_file(root), text.as_bytes())
    }

    /// Whether the folder at `dir` with `id`, at the sync folder's top, is its folder of
    /// shares: the one on record (by its id, where one is recorded), holding the record's
    /// marker. Nothing is looked at where nothing is on record, or the id is another's.
    pub fn holds(&self, id: FileId, dir: &Path) -> io::Result<bool> {
        match &self.0 {
            Some(known) if known.ids.is_empty() || known.ids.contains(&id.file) => known.marks(dir),
            _ => Ok(false),
        }
    }

    /// [`Record::holds`] for a folder whose id isn't known yet. Nothing is looked at where
    /// nothing is on record; a folder that isn't there is none.
    pub fn is(&self, dir: &Path) -> io::Result<bool> {
        if self.0.is_none() {
            return Ok(false);
        }
        match hbsync_platform::stat(dir) {
            Ok(meta) => self.holds(meta.file_id, dir),
            Err(e) if hbsync_platform::meta::nothing_there(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// What the marker of the folder at `dir` holds; `None` for a folder holding none
/// ([`holds_marker`]), or one gone as it was read.
fn token_in(dir: &Path) -> io::Result<Option<String>> {
    if !holds_marker(dir)? {
        return Ok(None);
    }
    match std::fs::read(shared_marker(dir)) {
        Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).trim().to_owned())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// What is at the top of `root`, the state folder left out: one listing, as the walk's. The
/// real folders (no links) with their file ids, and the names of all else. Nothing in a root
/// that isn't there. An item whose name isn't Unicode is never synced anyway
/// ([`crate::SkipReason`]).
fn list_top(root: &Path) -> io::Result<(Folders, Vec<String>)> {
    let items = match hbsync_platform::listdir::list_dir(root) {
        Ok(items) => items,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(e) => return Err(e),
    };
    let (mut folders, mut rest) = (Vec::new(), Vec::new());
    for item in items {
        match item.name.into_string() {
            Ok(name) if is_state_dir(&name) => {}
            Ok(name) if item.meta.kind == EntryKind::Dir => folders.push((name, item.meta.file_id)),
            Ok(name) => rest.push(name),
            Err(_) => {}
        }
    }
    Ok((folders, rest))
}

/// Folders by name, each with its file id.
type Folders = Vec<(String, FileId)>;

/// The top of a sync folder, as its folder of shares is told there ([`Top::read`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Top {
    /// The folder of shares, by name, sorted: the folder on record ([`Record::holds`]). Found
    /// by the record, never by a name: the user may rename it. More than one where the
    /// volume's file ids aren't stable (the module's words), or where a folder that was on
    /// record before is at the top beside the one on record now.
    pub shares: Vec<String>,
    /// The one of [`Top::shares`] on record now (by the record's first id), where more than
    /// one is there and ids are on record: where what is shared is synced.
    pub now: Option<String>,
    /// What else is there, by name (the state folder left out).
    pub others: Vec<String>,
    /// The file ids on record for the folder of shares (the one on record now, and those
    /// before it), wherever those folders are now: none where none is on record, or the
    /// volume's file ids aren't stable.
    pub ids: Vec<u128>,
    /// The folders here that hold the record's marker while the folder on record isn't here,
    /// each with its file id: one may be it, back as a copy (moved to another volume and
    /// back, restored), which has another file id, for [`adopt`] to judge as the sync folder
    /// opens. None while the folder on record is here (a marker copied elsewhere says
    /// nothing then), nor where no id is on record (the marker alone tells there).
    pub returned: Vec<(String, FileId)>,
}

impl Top {
    /// The top of `root`. A marker that can't be read is an error, as a root that can't be
    /// listed.
    ///
    /// Nothing but the record's read where none is on record (and nothing is told of the top
    /// then); else one listing of the root and one read of a marker (where no id is recorded:
    /// a look for one in each folder there).
    pub fn read(root: &Path) -> io::Result<Top> {
        let record = Record::read(root)?;
        if record.0.is_none() {
            return Ok(Top::default());
        }
        Top::told(root, &record)
    }

    /// [`Top::read`] by `record`, the top listed also where nothing is on record.
    fn told(root: &Path, record: &Record) -> io::Result<Top> {
        let (folders, mut others) = list_top(root)?;
        let (mut shares, mut plain, mut now) = (Vec::new(), Vec::new(), None);
        let first = record.0.as_ref().and_then(|known| known.ids.first());
        for (name, id) in folders {
            match record.holds(id, &root.join(&name))? {
                true => {
                    if first == Some(&id.file) {
                        now = Some(name.clone());
                    }
                    shares.push(name);
                }
                false => plain.push((name, id)),
            }
        }
        shares.sort_unstable();
        let ids = record
            .0
            .as_ref()
            .map_or_else(Vec::new, |known| known.ids.clone());
        // Looked for only while the folder on record isn't here (a marker a folder).
        let mut returned = Vec::new();
        if let (true, Some(known)) = (shares.is_empty(), &record.0)
            && !known.ids.is_empty()
        {
            for (name, id) in &plain {
                if known.marks(&root.join(name))? {
                    returned.push((name.clone(), *id));
                }
            }
        }
        others.extend(plain.into_iter().map(|(name, _)| name));
        Ok(Top {
            shares,
            now,
            others,
            ids,
            returned,
        })
    }
}

/// The folder of shares at the top of `root`, by name ([`Top::shares`]).
pub fn folders(root: &Path) -> io::Result<Vec<String>> {
    Ok(Top::read(root)?.shares)
}

/// What [`make`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Made {
    /// The folder of shares is there under the name asked for, on record.
    Made,
    /// Something else has the name: nothing is at the top that wasn't. Asked again with
    /// another name.
    Taken,
    /// The folder of shares is at the top already, under this name: nothing was made.
    Kept(String),
}

/// Makes the folder of shares of the sync folder at `root`, named `name` at its top. Never
/// there without its marker, nor before it is on record: prepared in the sync folder's tmp
/// (on its volume) with a marker holding a new token, recorded, then renamed into place,
/// never over anything. A crash in between leaves a prepared folder: one that is on record
/// (its marker in it) is put into place by the next call as it is, any other is made anew.
/// Never taken apart while it is the one on record: stopped there, the record would name
/// nothing, and read as a folder of shares that is gone ([`Placed::missing`]).
///
/// A folder on record that lost its marker is a plain folder (the user took the marker away:
/// theirs now): another is made, and goes on record in its place.
pub fn make(root: &Path, name: &str) -> io::Result<Made> {
    make_with(root, name, false)
}

/// [`make`]; `again`: in place of one on record that isn't at the top ([`make_again`]), with
/// the record's token and its ids kept beside the new folder's.
fn make_with(root: &Path, name: &str, again: bool) -> io::Result<Made> {
    let record = Record::read(root)?;
    if record.0.is_some()
        && let Some(kept) = Top::told(root, &record)?.shares.into_iter().next()
    {
        return Ok(Made::Kept(kept));
    }
    // (Each in the folder it is in: the sync folder isn't made again where it is gone.)
    let tmp = tmp_dir(root);
    hbsync_fileio::make_dir_in_place(&crate::layout::state_dir(root))?;
    hbsync_fileio::make_dir_in_place(&tmp)?;
    let prepared = tmp.join(PREPARED);
    if !record.is(&prepared)? {
        let marker = shared_marker(&prepared);
        // One left by a crash before it was on record: this program's own, holding its
        // marker and nothing else.
        for gone in [
            std::fs::remove_file(&marker),
            std::fs::remove_dir(&prepared),
        ] {
            match gone {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        std::fs::create_dir(&prepared)?;
        let (token, before) = match (&record.0, again) {
            (Some(known), true) => (known.token.clone(), known.ids.as_slice()),
            _ => (new_token(), &[][..]),
        };
        mark(&prepared, &token)?;
        Record::write(root, &token, &prepared, before)?;
    }
    match hbsync_platform::ops::rename_noreplace(&prepared, &root.join(name)) {
        Ok(()) => Ok(Made::Made),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(Made::Taken),
        Err(e) => Err(e),
    }
}

/// A token no other record has.
fn new_token() -> String {
    hbsync_types::Uuid::new_v4().simple().to_string()
}

/// Writes the marker of the folder at `dir`, holding `token`, over one that is there. On disk
/// before a record names it (the record is): a power cut must not leave a record whose folder
/// holds an empty marker, which reads as a folder of shares that is gone, for good. Written
/// in place (no temporary file beside it: one left by a crash would keep a prepared folder
/// from being cleared).
fn mark(dir: &Path, token: &str) -> io::Result<()> {
    let marker = shared_marker(dir);
    let mut file = std::fs::File::create(&marker)?;
    std::io::Write::write_all(&mut file, token.as_bytes())?;
    file.sync_all()?;
    // Only looks (a leading dot hides it elsewhere): a marker that can't be hidden still
    // works.
    let _ = hbsync_platform::ops::set_hidden(&marker);
    Ok(())
}

/// Makes the folder of shares again, on the user's word, where the one on record isn't at
/// the top of the sync folder at `root` (which [`place`] never does by itself: that one may
/// be back): under `name`, or the first of `<name>-2`, … that nothing at the top or on the
/// drive (`on_drive`) has. Its name. The record's token and ids stay beside the new folder's:
/// the one that was on record, still somewhere in the sync folder, is the folder of shares
/// where it is as before, and never sent. Refused where one is at the top.
pub fn make_again<E: From<io::Error>>(
    root: &Path,
    name: &str,
    case_insensitive: bool,
    mut on_drive: impl FnMut(&str) -> Result<bool, E>,
) -> Result<String, E> {
    let record = Record::read(root)?;
    let top = Top::told(root, &record)?;
    if let Some(at) = top.shares.first() {
        return Err(there(at).into());
    }
    let key = |name: &String| top_key(name, case_insensitive).into_owned();
    let mut names = Names {
        here: top.others.iter().map(key).collect(),
        case_insensitive,
        on_drive: &mut on_drive,
    };
    let mut kept = None;
    let made = names.free(name, 1, |to| {
        Ok(match make_with(root, to, true)? {
            Made::Made => true,
            Made::Taken => false,
            Made::Kept(name) => {
                kept = Some(name);
                true
            }
        })
    })?;
    Ok(match kept {
        Some(kept) => kept,
        None => made?,
    })
}

/// Why [`make_again`] and [`take`] refuse while a folder of shares is at the top.
fn there(at: &str) -> io::Error {
    let what = format!("the folder of shares is there: {at}");
    io::Error::new(io::ErrorKind::AlreadyExists, what)
}

/// Takes the folder `name` at the top of the sync folder at `root` as its folder of shares,
/// on the user's word: one that came back in a way [`adopt`] doesn't take by itself (synced
/// before it could be told: what was sent of it leaves the drive, as anything left out
/// does), or any folder they want what is shared in. Its marker is written (over one it
/// holds), then it goes on record, the record's token and ids kept. Refused where a folder of
/// shares is at the top, and for anything but a folder there (a path, a link, the state
/// folder).
pub fn take(root: &Path, name: &str) -> io::Result<()> {
    let record = Record::read(root)?;
    if let Some(at) = Top::told(root, &record)?.shares.first() {
        return Err(there(at));
    }
    let refuse = |what: String| io::Error::new(io::ErrorKind::InvalidInput, what);
    let mut parts = Path::new(name).components();
    let plain = matches!(
        (parts.next(), parts.next()),
        (Some(std::path::Component::Normal(_)), None)
    );
    let dir = root.join(name);
    let state = top_key(name, true) == top_key(crate::STATE_DIR, true);
    if !plain || state || !std::fs::symlink_metadata(&dir).is_ok_and(|m| m.is_dir()) {
        return Err(refuse(format!(
            "{name} isn't a folder at the top of {}",
            root.display()
        )));
    }
    let (token, before) = match &record.0 {
        Some(known) => (known.token.clone(), known.ids.as_slice()),
        None => (new_token(), &[][..]),
    };
    mark(&dir, &token)?;
    Record::write(root, &token, &dir, before)
}

/// The name of the folder [`make`] prepares in the sync folder's tmp.
const PREPARED: &str = "shares";

/// What [`place`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Placed {
    /// The name of the folder of shares it made.
    pub made: Option<String>,
    /// The folders of shares that gave way: the name each had, and the one it has now.
    pub moved: Vec<(String, String)>,
    /// A folder of shares couldn't give way to the drive's item of its name (in use, no name
    /// free), with why: the item stays masked, which loses nothing, and the next open tries
    /// again.
    pub stuck: Option<String>,
    /// The folder of shares couldn't be made (no name free, something in the way of its
    /// making), with why: there is none, which keeps nothing of the user's own from syncing,
    /// and the next open tries again.
    pub unmade: Option<String>,
    /// One is on record and isn't at the top (moved away, deleted, its marker gone): none
    /// was made in its place.
    pub missing: bool,
    /// The folders of shares at the top now, by name: where they were, gave way to, or were
    /// made. The one on record now first (where what is shared is synced), then any that
    /// was on record before it, by name.
    pub at: Vec<String>,
}

/// How many names [`place`] tries for one folder: `Shared`, `Shared-2`, … `Shared-99`.
const NAMES: usize = 99;

/// `name` as [`place`] numbers it: what it is without its number, and the number (1 for a
/// name without). `Shared-2` is `Shared`'s second, so it gives way to `Shared-3`; a number
/// [`place`] wouldn't give (`From 2024-2025`) is part of the name. A name the user gave that
/// ends as one of ours (`Team-7`) is numbered on too (`Team-8`): which it is isn't known.
fn numbered(name: &str) -> (&str, usize) {
    let ours = name.rsplit_once('-').and_then(|(base, n)| {
        let plain =
            !base.is_empty() && !n.starts_with('0') && n.bytes().all(|b| b.is_ascii_digit());
        let n = n.parse().ok().filter(|n| plain && (2..NAMES).contains(n))?;
        Some((base, n))
    });
    ours.unwrap_or((name, 1))
}

/// The names taken at a sync folder's top, for [`place`].
struct Names<'a, E> {
    /// Those on disk, as they compare ([`top_key`]).
    here: Vec<String>,
    case_insensitive: bool,
    /// Whether the drive has an item of this name at its top.
    on_drive: &'a mut dyn FnMut(&str) -> Result<bool, E>,
}

impl<E: From<io::Error>> Names<'_, E> {
    /// The first of `name` (as number 1), `<name>-2`, `<name>-3`, … from number `first` on
    /// that nothing has and `put` takes (`false`: taken on disk since the listing). Within:
    /// why `put` failed, or that no name is free.
    fn free(
        &mut self,
        name: &str,
        first: usize,
        mut put: impl FnMut(&str) -> io::Result<bool>,
    ) -> Result<io::Result<String>, E> {
        for n in first..=NAMES {
            let to = match n {
                1 => name.to_owned(),
                n => format!("{name}-{n}"),
            };
            let key = top_key(&to, self.case_insensitive).into_owned();
            if self.here.contains(&key) || (self.on_drive)(&to)? {
                continue;
            }
            match put(&to) {
                Ok(true) => {
                    self.here.push(key);
                    return Ok(Ok(to));
                }
                Ok(false) => {}
                Err(e) => return Ok(Err(e)),
            }
        }
        let what = format!("no name is free up to {name}-{NAMES}");
        Ok(Err(io::Error::new(io::ErrorKind::AlreadyExists, what)))
    }
}

/// Puts the folder of shares of the sync folder at `root` where it holds no name against
/// anything, as the sync folder opens and before its filter is read:
///
/// - With `make_as` and none on record, one is made under that name ([`make`]), or the
///   first of `<name>-2`, `<name>-3`, … that is free. Never where one is on record, at the
///   top or not ([`Placed::missing`]).
/// - One whose name something else has gives way: renamed (its marker and its file id go with
///   it, so it stays the one on record) to the next free number ([`numbered`]). Where that
///   fails for an item beside it, an error (that item would leave the drive); for the drive's
///   item, [`Placed::stuck`].
///
/// A name is taken by another item at the top, and by an item at the top of the drive
/// (`on_drive`, asked by name), as names compare there ([`top_key`]; `case_insensitive`: the
/// volume's). Without this the folder of shares would mask the drive's item of its name for
/// good, and an item of the user's own that the volume tells apart from it and the client
/// doesn't would be left out with it, and leave the drive (sync semantics §10).
///
/// Costs the record's read; where one is on record or one is to be made, one listing of the
/// root, a marker's read, and `on_drive` once a folder of shares (more only for a name taken).
pub fn place<E: From<io::Error>>(
    root: &Path,
    make_as: Option<&str>,
    case_insensitive: bool,
    mut on_drive: impl FnMut(&str) -> Result<bool, E>,
) -> Result<Placed, E> {
    let record = Record::read(root)?;
    let mut placed = Placed::default();
    if record.0.is_none() && make_as.is_none() {
        return Ok(placed);
    }
    let top = Top::told(root, &record)?;
    let key = |name: &str| top_key(name, case_insensitive).into_owned();
    let others: Vec<String> = top.others.iter().map(|n| key(n)).collect();
    // Every name at the top, as it compares: the folders of shares' too (none takes another's).
    let mut names = Names {
        here: others
            .iter()
            .cloned()
            .chain(top.shares.iter().map(|n| key(n)))
            .collect(),
        case_insensitive,
        on_drive: &mut on_drive,
    };
    placed.at.clone_from(&top.shares);
    // The one on record now, under the name it has when this is done.
    let mut now = top.now.clone();
    for name in &top.shares {
        let here = others.contains(&key(name));
        if !here && !(names.on_drive)(name)? {
            continue;
        }
        let from = root.join(name);
        let (base, number) = numbered(name);
        let moved = names.free(
            base,
            number + 1,
            |to| match hbsync_platform::ops::rename_noreplace(&from, &root.join(to)) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(e),
            },
        )?;
        match moved {
            Ok(to) => {
                placed.at.retain(|at| at != name);
                placed.at.push(to.clone());
                if now.as_ref() == Some(name) {
                    now = Some(to.clone());
                }
                placed.moved.push((name.clone(), to));
            }
            // An item of the user's own would be left out with it, and leave the drive.
            Err(e) if here => {
                let what = format!(
                    "the folder of shares {name} can't give way to the item beside it that \
                     compares as the same name: {e}"
                );
                return Err(io::Error::new(e.kind(), what).into());
            }
            Err(e) => {
                placed.stuck = Some(format!(
                    "the folder of shares {name} can't give way to the drive's item of its \
                     name: {e}"
                ));
            }
        }
    }
    // Only where nothing is on record: one on record that isn't at the top (moved into
    // another folder for a moment, its marker unreadable just now) stays the one on record,
    // or it would be a plain folder for good when it is back, and sent.
    // But one on record that never got to the top ([`make`] stopped before its rename: a
    // crash, a rename refused) is no folder of shares yet: [`make`] puts it in place.
    let unplaced = top.shares.is_empty() && record.is(&tmp_dir(root).join(PREPARED))?;
    placed.missing = record.0.is_some() && top.shares.is_empty() && !unplaced;
    if let Some(name) = make_as.filter(|_| record.0.is_none() || unplaced) {
        let mut kept = None;
        let made = names.free(name, 1, |to| {
            Ok(match make(root, to)? {
                Made::Made => true,
                Made::Taken => false,
                // Made by another since the listing: there, which is all that was asked.
                Made::Kept(name) => {
                    kept = Some(name);
                    true
                }
            })
        })?;
        match (made, kept) {
            (Ok(_), Some(kept)) => placed.at.push(kept),
            (Ok(made), None) => {
                placed.at.push(made.clone());
                placed.made = Some(made);
            }
            (Err(e), _) => {
                let why = format!("the folder of shares {name} can't be made: {e}");
                placed.unmade = Some(why);
            }
        }
    }
    placed.at.sort_unstable();
    // More than one (the one on record now, and one that was before, back at the top): the
    // one on record now first, which is where what is shared is synced.
    if let Some(at) = now.and_then(|now| placed.at.iter().position(|name| *name == now)) {
        placed.at[..=at].rotate_right(1);
    }
    Ok(placed)
}

/// Takes a folder of shares whose record no longer finds it (a sync folder restored from a
/// backup: every folder in it has a new id), as the sync folder opens: where one is on record
/// and isn't at the top of `root`, and exactly one folder there that isn't synced (`synced`,
/// by its name and file id) holds a marker with the record's token, that one goes on record.
/// Its name, if one was taken.
///
/// Never a synced folder: a marker copied into one does nothing. With two to choose from,
/// neither. With nothing on record, nothing is looked at, and nothing taken.
///
/// Costs the record's read; where one is on record, one listing of the root and a marker's
/// read, and where the folder on record isn't there a look for a marker in each folder.
pub fn adopt<E: From<io::Error>>(
    root: &Path,
    mut synced: impl FnMut(&str, FileId) -> Result<bool, E>,
) -> Result<Option<String>, E> {
    let record = Record::read(root)?;
    let Some(known) = &record.0 else {
        return Ok(None);
    };
    // (None where the one on record is there: no other folder is looked into then.)
    let top = Top::told(root, &record)?;
    let mut found = None;
    for (name, id) in &top.returned {
        if synced(name, *id)? {
            continue;
        }
        if found.is_some() {
            return Ok(None);
        }
        found = Some(name);
    }
    let Some(name) = found else {
        return Ok(None);
    };
    Record::write(root, &known.token, &root.join(name), &known.ids)?;
    Ok(Some(name.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn never(_: &str, _: FileId) -> io::Result<bool> {
        Ok(false)
    }

    /// Whether this volume's file ids are stable: a test of what the id tells can't run
    /// where they aren't.
    fn stable(root: &Path) -> bool {
        Record::read(root).is_ok_and(|r| r.0.is_some_and(|k| !k.ids.is_empty()))
    }

    /// The folder made is the folder of shares under any name it is given later; a marker
    /// copied into another folder makes that one nothing, and a copy of the folder itself
    /// neither.
    #[test]
    fn the_folder_made_is_the_one_and_a_copied_marker_makes_none() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert!(folders(root)?.is_empty());
        assert!(folders(&root.join("missing"))?.is_empty());
        fs::create_dir_all(root.join("Projects"))?;
        assert_eq!(make(root, "Projects")?, Made::Taken);
        assert!(folders(root)?.is_empty(), "nothing is there that wasn't");
        assert_eq!(make(root, "Shared")?, Made::Made);
        assert_eq!(folders(root)?, ["Shared"]);
        assert_eq!(make(root, "Shared-2")?, Made::Kept("Shared".into()));
        assert!(!root.join("Shared-2").exists());
        if !stable(root) {
            return Ok(());
        }

        let marker = shared_marker(&root.join("Shared"));
        fs::copy(&marker, shared_marker(&root.join("Projects")))?;
        fs::create_dir(root.join("Copy"))?;
        fs::copy(&marker, shared_marker(&root.join("Copy")))?;
        assert_eq!(folders(root)?, ["Shared"]);
        let record = Record::read(root)?;
        assert!(record.is(&root.join("Shared"))?);
        assert!(!record.is(&root.join("Projects"))?);
        assert!(!record.is(&root.join("missing"))?);

        fs::rename(root.join("Shared"), root.join("Renamed by hand"))?;
        assert_eq!(folders(root)?, ["Renamed by hand"]);
        assert_eq!(adopt(root, never)?, None, "the folder on record is there");
        // Another marker in it (an empty file, another folder of shares' own) isn't its own.
        let marker = shared_marker(&root.join("Renamed by hand"));
        let token = fs::read(&marker)?;
        fs::remove_file(&marker)?;
        fs::write(&marker, "")?;
        assert!(folders(root)?.is_empty());
        fs::remove_file(&marker)?;
        fs::write(&marker, token)?;
        assert_eq!(folders(root)?, ["Renamed by hand"]);
        Ok(())
    }

    /// Without its marker the folder on record is a plain folder: the next one made takes its
    /// place on record. A folder left prepared by a crash, not on record, is made anew.
    #[test]
    fn another_is_made_once_the_one_on_record_is_plain() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(make(root, "Shared")?, Made::Made);
        let old = fs::read(shared_marker(&root.join("Shared")))?;
        fs::remove_file(shared_marker(&root.join("Shared")))?;
        assert!(folders(root)?.is_empty());
        assert_eq!(make(root, "Shared")?, Made::Taken);
        assert!(tmp_dir(root).join(PREPARED).is_dir());
        assert_eq!(make(root, "Shared-2")?, Made::Made);
        assert_eq!(folders(root)?, ["Shared-2"]);
        assert!(!tmp_dir(root).join(PREPARED).exists());
        // Its old marker back makes the plain one nothing: another's token.
        fs::write(shared_marker(&root.join("Shared")), old)?;
        assert_eq!(folders(root)?, ["Shared-2"]);
        Ok(())
    }

    /// A folder of shares the record doesn't find (restored: a new id) is taken as the folder
    /// opens, if it is the only one: never a synced folder holding the marker, a folder
    /// holding another marker, or one of two. With nothing on record, none.
    #[test]
    fn a_restored_folder_of_shares_is_taken_when_it_is_the_only_one() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = &dir.path().join("sync");
        fs::create_dir(root)?;
        fs::create_dir(root.join("Loose"))?;
        fs::write(shared_marker(&root.join("Loose")), "")?;
        assert_eq!(adopt(root, never)?, None, "nothing is on record");
        assert_eq!(make(root, "Shared")?, Made::Made);
        if !stable(root) {
            return Ok(());
        }
        let token = fs::read(shared_marker(&root.join("Shared")))?;
        // Out of the sync folder, not deleted: a folder made next could get its id (ext4 and
        // XFS give a freed one out again), and would be the one on record, by id and token.
        fs::rename(root.join("Shared"), dir.path().join("gone"))?;
        for name in ["Restored", "Projects", "plain"] {
            fs::create_dir(root.join(name))?;
        }
        fs::write(shared_marker(&root.join("Restored")), &token)?;
        fs::write(shared_marker(&root.join("Projects")), &token)?;
        let projects = hbsync_platform::stat(&root.join("Projects"))?.file_id;
        let by_name = |name: &str, _: FileId| io::Result::Ok(name == "Projects");
        let by_id = |_: &str, id: FileId| io::Result::Ok(id == projects);
        assert!(folders(root)?.is_empty());
        assert_eq!(adopt(root, never)?, None, "two to choose from");
        assert_eq!(adopt(root, by_id)?.as_deref(), Some("Restored"));
        assert_eq!(folders(root)?, ["Restored"]);
        assert_eq!(adopt(root, never)?, None);

        fs::rename(root.join("Restored"), dir.path().join("gone too"))?;
        fs::create_dir(root.join("Again"))?;
        fs::write(shared_marker(&root.join("Again")), &token)?;
        assert_eq!(adopt(root, |_, _| io::Result::Ok(true))?, None);
        assert_eq!(adopt(root, by_name)?.as_deref(), Some("Again"));
        Ok(())
    }

    /// A record that isn't one is an error, never "none"; one without an id (a volume whose
    /// ids aren't stable) goes by the token alone.
    #[test]
    fn a_record_that_cant_be_read_isnt_none_and_one_without_an_id_goes_by_the_token()
    -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::create_dir_all(root.join("Shared"))?;
        fs::create_dir_all(crate::layout::state_dir(root))?;
        for junk in [
            "junk",
            "folder 12\n",
            "token t\nfolder x\n",
            "token t\nfolder \n",
            "token t\nfolder\n",
        ] {
            fs::write(shares_file(root), junk)?;
            assert!(Record::read(root).is_err(), "{junk:?}");
            assert!(folders(root).is_err());
        }
        fs::write(shares_file(root), "token t\n")?;
        assert!(folders(root)?.is_empty());
        fs::write(shared_marker(&root.join("Shared")), "t\n")?;
        assert_eq!(folders(root)?, ["Shared"]);
        assert_eq!(make(root, "Shared-2")?, Made::Kept("Shared".into()));
        Ok(())
    }

    /// A sync folder that is gone isn't made again for its folder of shares: neither as one
    /// is made, nor as one is put on record.
    #[test]
    fn a_sync_folder_that_is_gone_isnt_made_again() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let gone = dir.path().join("gone");
        assert!(make(&gone, "Shared").is_err());
        assert!(Record::write(&gone, "token", &gone.join("Shared"), &[]).is_err());
        assert!(!gone.exists());
        Ok(())
    }

    /// A folder prepared and on record whose making stopped before it was in place (a name
    /// taken, a crash) is put in place as it is by the next try: never taken apart first,
    /// which would leave the record naming nothing if that stopped too.
    #[test]
    fn a_folder_prepared_and_on_record_is_put_in_place_as_it_is() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::write(root.join("Shared"), "in the way")?;
        assert_eq!(make(root, "Shared")?, Made::Taken);
        let prepared = tmp_dir(root).join(PREPARED);
        let token = fs::read(shared_marker(&prepared))?;
        assert!(Record::read(root)?.is(&prepared)?);
        assert_eq!(make(root, "Shared-2")?, Made::Made);
        assert_eq!(fs::read(shared_marker(&root.join("Shared-2")))?, token);
        assert_eq!(folders(root)?, ["Shared-2"]);
        assert!(!prepared.exists());
        Ok(())
    }

    /// A folder of shares that can't be made is none, said so: not an error that stops the
    /// sync folder, whose own items don't wait for it.
    #[test]
    fn a_folder_of_shares_that_cant_be_made_is_told_not_an_error() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        // Something in the way of its making that isn't this program's to clear.
        fs::create_dir_all(tmp_dir(root).join(PREPARED).join("stray"))?;
        let placed = place(root, Some("Shared"), false, nothing)?;
        assert!(placed.unmade.is_some_and(|why| why.contains("Shared")));
        assert!(placed.made.is_none() && placed.at.is_empty() && !placed.missing);
        assert!(folders(root)?.is_empty());
        Ok(())
    }

    /// A copy taken as the folder of shares while the one that was on record is still in the
    /// sync folder (moved into another folder): both are the folder of shares from then on,
    /// each by its id. Neither is sent.
    #[test]
    fn a_copy_taken_leaves_the_one_before_it_on_record_too() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(make(root, "Shared")?, Made::Made);
        if !stable(root) {
            return Ok(());
        }
        let token = fs::read(shared_marker(&root.join("Shared")))?;
        fs::create_dir(root.join("Work"))?;
        fs::rename(root.join("Shared"), root.join("Work/Shared"))?;
        fs::create_dir(root.join("Copy"))?;
        fs::write(shared_marker(&root.join("Copy")), &token)?;
        assert_eq!(adopt(root, never)?.as_deref(), Some("Copy"));
        let record = Record::read(root)?;
        assert!(record.is(&root.join("Copy"))? && record.is(&root.join("Work/Shared"))?);
        assert_eq!(folders(root)?, ["Copy"]);
        // Back at the top beside it, both are left out; the one on record now is told first.
        fs::rename(root.join("Work/Shared"), root.join("A earlier"))?;
        assert_eq!(folders(root)?, ["A earlier", "Copy"]);
        assert_eq!(place(root, None, false, nothing)?.at, ["Copy", "A earlier"]);
        Ok(())
    }

    /// On the user's word another is made where the one on record isn't at the top, which
    /// is never done by itself: under the next name that is free, with the record's token and
    /// ids kept, so the one that was on record stays the folder of shares where it still is.
    /// Refused while one is at the top.
    #[test]
    fn made_again_on_request_the_one_before_it_stays_on_record() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(make(root, "Shared")?, Made::Made);
        let refused = make_again(root, "Shared", false, nothing);
        assert!(refused.is_err_and(|e| e.kind() == io::ErrorKind::AlreadyExists));
        fs::create_dir(root.join("Work"))?;
        fs::rename(root.join("Shared"), root.join("Work/Shared"))?;
        let placed = place(root, Some("Shared"), false, nothing)?;
        assert!(placed.missing && placed.made.is_none());

        fs::write(root.join("Shared"), "something else has the name meanwhile")?;
        assert_eq!(make_again(root, "Shared", false, nothing)?, "Shared-2");
        assert_eq!(folders(root)?, ["Shared-2"]);
        let record = Record::read(root)?;
        assert!(record.is(&root.join("Shared-2"))? && record.is(&root.join("Work/Shared"))?);
        assert_eq!(
            place(root, Some("Shared"), false, nothing)?.at,
            ["Shared-2"]
        );
        Ok(())
    }

    /// A folder at the top is taken as the folder of shares on the user's word: its marker
    /// written, on record, the record's token and ids kept. Refused while one is at the top,
    /// and for anything but a folder there.
    #[test]
    fn a_named_folder_is_taken_on_request() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::create_dir_all(root.join("Theirs/tom"))?;
        fs::create_dir(root.join(crate::STATE_DIR))?;
        fs::write(root.join("notes.txt"), "n")?;
        for not_one in ["notes.txt", "Theirs/tom", "none", "..", crate::STATE_DIR] {
            let refused = take(root, not_one);
            let invalid = refused.is_err_and(|e| e.kind() == io::ErrorKind::InvalidInput);
            assert!(invalid, "{not_one}");
        }
        assert!(folders(root)?.is_empty());

        // Nothing on record: with a token of its own.
        take(root, "Theirs")?;
        assert_eq!(folders(root)?, ["Theirs"]);
        let refused = take(root, "Theirs");
        assert!(refused.is_err_and(|e| e.kind() == io::ErrorKind::AlreadyExists));

        // The one on record away, another is taken: both are on record.
        fs::create_dir(root.join("Work"))?;
        fs::rename(root.join("Theirs"), root.join("Work/Theirs"))?;
        fs::create_dir(root.join("Back"))?;
        take(root, "Back")?;
        assert_eq!(folders(root)?, ["Back"]);
        let record = Record::read(root)?;
        assert!(record.is(&root.join("Back"))? && record.is(&root.join("Work/Theirs"))?);
        Ok(())
    }

    fn nothing(_: &str) -> io::Result<bool> {
        Ok(false)
    }

    /// Made under the first name nothing has, here or on the drive; there, it gives way to an
    /// item that takes its name, keeping its number's turn, and stays the one on record.
    #[test]
    fn it_is_made_under_a_free_name_and_gives_way_to_what_takes_its_name() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(place(root, None, false, nothing)?, Placed::default());
        assert!(fs::read_dir(root)?.next().is_none(), "nothing asked for");

        fs::write(root.join("Shared"), "mine")?;
        let placed = place(root, Some("Shared"), false, |name| {
            Ok::<_, io::Error>(name == "Shared-2")
        })?;
        assert_eq!(placed.made.as_deref(), Some("Shared-3"));
        assert!(placed.moved.is_empty());
        assert_eq!(folders(root)?, ["Shared-3"]);
        assert_eq!(placed.at, ["Shared-3"]);
        let there = |name: &str| Placed {
            at: vec![name.to_owned()],
            ..Placed::default()
        };
        assert_eq!(
            place(root, Some("Shared"), false, nothing)?,
            there("Shared-3")
        );

        fs::create_dir(root.join("Shared-4"))?;
        let mut asked = Vec::new();
        let placed = place(root, Some("Shared"), false, |name| {
            asked.push(name.to_owned());
            Ok::<_, io::Error>(name == "Shared-3")
        })?;
        assert_eq!(placed.made, None);
        assert_eq!(placed.moved, [("Shared-3".into(), "Shared-5".into())]);
        assert_eq!(placed.at, ["Shared-5"]);
        assert_eq!(
            asked,
            ["Shared-3", "Shared-5"],
            "one on disk isn't asked of the drive"
        );
        assert_eq!(folders(root)?, ["Shared-5"]);
        assert_eq!(fs::read_to_string(root.join("Shared"))?, "mine");
        assert!(root.join("Shared-4").is_dir());

        // A name of the user's own that ends in no number of ours.
        fs::rename(root.join("Shared-5"), root.join("From 2024-2025"))?;
        let placed = place(root, None, false, |name| {
            Ok::<_, io::Error>(name == "From 2024-2025")
        })?;
        let moved = [("From 2024-2025".into(), "From 2024-2025-2".into())];
        assert_eq!(placed.moved, moved);

        // One on record that isn't at the top stays the one on record: none is made in its
        // place, and back at the top it is the folder of shares as before.
        fs::create_dir(root.join("Work"))?;
        fs::rename(root.join("From 2024-2025-2"), root.join("Work/Shared"))?;
        let placed = place(root, Some("Shared"), false, nothing)?;
        assert!(placed.missing && placed.made.is_none() && placed.at.is_empty());
        assert!(folders(root)?.is_empty());
        fs::rename(root.join("Work/Shared"), root.join("Back"))?;
        assert_eq!(place(root, Some("Shared"), false, nothing)?, there("Back"));
        assert_eq!(folders(root)?, ["Back"]);

        // No name free: for the drive's item it stays where it is, and says so.
        let placed = place(root, None, false, |_| Ok::<_, io::Error>(true))?;
        assert!(placed.moved.is_empty());
        assert!(placed.stuck.is_some_and(|why| why.contains("Back")));
        assert_eq!(folders(root)?, ["Back"]);

        // One whose making stopped before it was in place (on record, still prepared) is
        // put in place.
        let other = tempfile::tempdir()?;
        let root = other.path();
        assert_eq!(make(root, "Shared")?, Made::Made);
        fs::create_dir_all(tmp_dir(root))?;
        fs::rename(root.join("Shared"), tmp_dir(root).join(PREPARED))?;
        let placed = place(root, Some("Shared"), false, nothing)?;
        assert_eq!(placed.made.as_deref(), Some("Shared"));
        assert!(!placed.missing);
        assert_eq!(folders(root)?, ["Shared"]);
        Ok(())
    }

    /// What the volume tells apart and the client doesn't (`Straße` beside `STRASSE`, where
    /// the volume ignores case) is in the way: the folder of shares gives way to it, and is
    /// never made under such a name.
    #[test]
    fn it_gives_way_to_a_name_the_client_cant_tell_from_its_own() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        assert_eq!(make(root, "Straße")?, Made::Made);
        if fs::create_dir(root.join("STRASSE")).is_err() {
            // A volume that doesn't tell the two apart: nothing to give way to.
            return Ok(());
        }
        let in_the_way = |caseless| crate::Filter::load(root, caseless).map(|f| f.in_the_way());
        assert!(!in_the_way(false).expect("read"));
        assert!(in_the_way(true).expect("read"));
        assert_eq!(place(root, None, false, nothing)?.at, ["Straße"]);
        let placed = place(root, None, true, nothing)?;
        assert_eq!(placed.at, ["Straße-2"]);
        assert_eq!(placed.moved, [("Straße".into(), "Straße-2".into())]);
        assert_eq!(folders(root)?, ["Straße-2"]);
        assert!(!in_the_way(true).expect("read"));
        assert!(root.join("STRASSE").is_dir());

        let other = tempfile::tempdir()?;
        let root = other.path();
        fs::create_dir(root.join("shared"))?;
        let placed = place(root, Some("Shared"), true, nothing)?;
        assert_eq!(placed.made.as_deref(), Some("Shared-2"));
        Ok(())
    }
}
