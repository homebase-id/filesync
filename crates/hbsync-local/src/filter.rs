//! What is never synced: the client's own folders, built-in junk, and the user's ignore rules.
//!
//! Junk is disposable (OS metadata, lock and swap files, partial downloads): a junk file goes
//! with its folder. Ignored items are the user's data kept on this device only: they
//! keep their folder. Both are invisible to sync in either direction (sync semantics §10), as
//! is the folder at the top that holds what other identities share ([`crate::shares`]).

use std::borrow::Cow;
use std::io;
use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use hbsync_platform::RelPath;
use hbsync_platform::name::{ends_caseless, name_key, starts_caseless};

/// Bumped whenever what the built-in names leave out changes (the junk list, where the state
/// folder's name counts), so that counts as a rule change.
const JUNK_VERSION: u32 = 5;

/// How the built-in list matches a name (ASCII case-insensitively).
#[derive(Debug, Clone, Copy)]
enum Pat {
    Name(&'static str),
    Prefix(&'static str),
    Suffix(&'static str),
    Both(&'static str, &'static str),
}

/// Built-in junk: names the OS or an application creates on its own and that make no sense on
/// another device. Anything a user might want kept is left to their rules (`*.tmp`, `*~`).
const JUNK: &[Pat] = &[
    // macOS: Finder view settings, AppleDouble resource forks on non-Apple volumes, custom
    // folder icons, and volume-level indexes and trash.
    Pat::Name(".DS_Store"),
    Pat::Prefix("._"),
    Pat::Name("Icon\r"),
    Pat::Name(".Spotlight-V100"),
    Pat::Name(".Trashes"),
    Pat::Name(".fseventsd"),
    Pat::Name(".TemporaryItems"),
    Pat::Name(".DocumentRevisions-V100"),
    Pat::Name(".apdisk"),
    // KDE: Dolphin's folder settings (and the folder icon the tray sets).
    Pat::Name(".directory"),
    // Windows: thumbnail caches, folder view settings, the recycle bin and restore points.
    Pat::Name("Thumbs.db"),
    Pat::Name("ehthumbs.db"),
    Pat::Name("desktop.ini"),
    Pat::Name("$RECYCLE.BIN"),
    Pat::Name("System Volume Information"),
    // Microsoft Office: owner (lock) files and temporary save files.
    Pat::Prefix("~$"),
    Pat::Both("~", ".tmp"),
    // LibreOffice lock files.
    Pat::Both(".~lock.", "#"),
    // vim swap files.
    Pat::Both(".", ".swp"),
    Pat::Both(".", ".swx"),
    // Other sync apps' own files, left behind in a folder moved over from them: Dropbox's
    // folder marker and cache, OneDrive's folder marker, Google Drive's transfers in progress.
    Pat::Name(".dropbox"),
    Pat::Name(".dropbox.cache"),
    Pat::Name(".849C9593-D756-4E56-8D6E-42412F2A707B"),
    Pat::Name(".tmp.drivedownload"),
    Pat::Name(".tmp.driveupload"),
    // Downloads in progress (Chrome and Edge, Firefox, older browsers, Safari's folder): they
    // get their real name when done.
    Pat::Suffix(".crdownload"),
    Pat::Suffix(".part"),
    Pat::Suffix(".partial"),
    Pat::Suffix(".download"),
    // The client's own: the marker of the folder that holds what other identities share. A
    // marker from the drive would take the folder it lands in off the drive.
    Pat::Name(crate::layout::SHARED_MARKER),
];

/// The state folder's ([`crate::STATE_DIR`]) name key (`name_key`): its name in any case or
/// spelling.
pub fn state_dir_key() -> &'static str {
    use std::sync::LazyLock;
    static KEY: LazyLock<String> = LazyLock::new(|| name_key(crate::STATE_DIR));
    &KEY
}

/// Whether `name` is the state folder's ([`crate::STATE_DIR`]) in any case or spelling: caseless
/// and normalization-insensitive (`name_key`), as a volume that ignores case compares names.
///
/// Asked of every name a walk, the watcher and the mask meet, so without building a key where
/// it can't match: an ASCII name's key is the name in lower case, and only a name that starts
/// with a `.` has a key that does.
pub fn is_state_dir(name: &str) -> bool {
    if name.is_ascii() {
        return name.eq_ignore_ascii_case(crate::STATE_DIR);
    }
    name.starts_with('.')
        && name.len() <= crate::STATE_DIR.len() * 4
        && name_key(name) == state_dir_key()
}

/// Whether the folder at `dir` holds a [`crate::layout::SHARED_MARKER`], a file: in the folder
/// on record ([`crate::shares`]), what makes it the one that holds what other identities
/// share. A folder that isn't there, or that this user may not look into (the walk can't read
/// it either), holds none; any other error is one: the marker may be there.
pub(crate) fn holds_marker(dir: &Path) -> io::Result<bool> {
    use io::ErrorKind::{NotADirectory, NotFound, PermissionDenied};
    match std::fs::symlink_metadata(crate::layout::shared_marker(dir)) {
        Ok(meta) => Ok(meta.is_file()),
        Err(e) if matches!(e.kind(), NotFound | NotADirectory | PermissionDenied) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Whether `name` is on the built-in junk list (at any depth, in any case).
pub fn is_junk(name: &str) -> bool {
    JUNK.iter().any(|pat| match *pat {
        Pat::Name(n) => name.eq_ignore_ascii_case(n),
        Pat::Prefix(p) => starts_caseless(name, p),
        Pat::Suffix(s) => ends_caseless(name, s),
        Pat::Both(p, s) => {
            name.len() >= p.len() + s.len() && starts_caseless(name, p) && ends_caseless(name, s)
        }
    })
}

/// What the filter says about an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Sync,
    /// The client's own: a state folder ([`crate::STATE_DIR`], this sync root's at its top or
    /// one below it, of a sync root moved or copied in), and at the top a folder that holds
    /// what other identities share ([`crate::shares`]).
    Excluded,
    Junk,
    Ignored,
}

impl Verdict {
    pub fn is_sync(self) -> bool {
        self == Verdict::Sync
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("can't read {}: {source}", path.display())]
    Read { path: PathBuf, source: io::Error },
    /// The rules as a whole couldn't be built (a line that doesn't parse is skipped instead,
    /// [`Filter::skipped`]).
    #[error("{}/{}: {message}", crate::STATE_DIR, crate::layout::IGNORE_FILE)]
    Rules { message: String },
    /// Negations (`!…`) that don't parse, by line: skipped, what they keep synced would be
    /// ignored, and leave the drive and the other devices (sync semantics §10, rule 1). Never
    /// skipped: the folder doesn't sync till they are fixed. `message`: why the first doesn't.
    #[error(
        "{}/{} {} ({message}), and keeps things synced: syncing stops till it is fixed \
         (fix it to resume)",
        crate::STATE_DIR,
        crate::layout::IGNORE_FILE,
        negations(lines)
    )]
    Negation { lines: Vec<usize>, message: String },
}

/// "line 2 isn't a valid rule", or "lines 2 and 7 aren't valid rules".
fn negations(lines: &[usize]) -> String {
    match lines {
        [one] => format!("line {one} isn't a valid rule"),
        [rest @ .., last] => {
            let rest: Vec<String> = rest.iter().map(|l| l.to_string()).collect();
            format!("lines {} and {last} aren't valid rules", rest.join(", "))
        }
        [] => "a line isn't a valid rule".into(),
    }
}

/// A line of `.hbsync/ignore` that isn't a valid rule: skipped, matching nothing (as Git does).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedRule {
    /// 1-based.
    pub line: usize,
    /// Why it doesn't parse.
    pub message: String,
}

/// The filter of one sync root.
#[derive(Debug, Clone)]
pub struct Filter {
    rules: Gitignore,
    /// The rules as written, for the fingerprint.
    text: String,
    case_insensitive: bool,
    /// Lines that aren't valid rules, in order.
    skipped: Vec<SkippedRule>,
    /// The folders at the top that hold what other identities share ([`crate::shares`]), as
    /// their names compare ([`top_key`]), in order.
    shared: Vec<String>,
    /// Something else at the top had one's name when the filter was read
    /// ([`Filter::in_the_way`]).
    in_the_way: bool,
    /// The file ids on record for the folder of shares when the filter was read
    /// ([`Filter::is_shares_id`]).
    shares_ids: Vec<u128>,
    /// How many folders at the top held the record's marker when the filter was read, the
    /// folder on record not there ([`Filter::returned`]), by name, in order.
    returned: Vec<String>,
}

/// `name` as names at the top compare with a folder's that holds what other identities
/// share: caseless and normalization-insensitive (`name_key`) where the volume ignores case
/// (`case_insensitive`), as it is elsewhere. macOS takes a name in any normalization for the
/// same one on a volume that tells case apart too: in NFC there.
pub fn top_key(name: &str, case_insensitive: bool) -> Cow<'_, str> {
    if case_insensitive {
        Cow::Owned(name_key(name))
    } else if cfg!(target_os = "macos") && !name.is_ascii() {
        Cow::Owned(hbsync_platform::name::nfc(name))
    } else {
        Cow::Borrowed(name)
    }
}

/// The names of `top`'s folders that may be the folder of shares, back
/// ([`crate::shares::Top::returned`]), in order.
fn returned(top: &crate::shares::Top) -> Vec<String> {
    let mut names: Vec<String> = top.returned.iter().map(|(name, _)| name.clone()).collect();
    names.sort_unstable();
    names
}

impl Default for Filter {
    fn default() -> Self {
        Filter {
            rules: Gitignore::empty(),
            text: String::new(),
            case_insensitive: false,
            skipped: Vec::new(),
            shared: Vec::new(),
            in_the_way: false,
            shares_ids: Vec::new(),
            returned: Vec::new(),
        }
    }
}

impl Filter {
    /// The user's rules from `.hbsync/ignore` under `root`, if the file exists; built-ins only
    /// otherwise. A line that isn't a valid rule is skipped ([`Filter::skipped`]); a file that
    /// can't be read is an error: its rules are unknown, not mistyped. With the folder at the
    /// top of `root` that holds what other identities share ([`crate::shares::Top`]) left
    /// out; one that can't be told is an error too.
    pub fn load(root: &Path, case_insensitive: bool) -> Result<Self, FilterError> {
        let path = crate::layout::ignore_file(root);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
            Err(source) => return Err(FilterError::Read { path, source }),
        };
        let top = crate::shares::Top::read(root).map_err(|source| FilterError::Read {
            path: root.to_owned(),
            source,
        })?;
        let mut filter = Self::from_rules(&text, case_insensitive)?.with_shared(&top.shares);
        filter.in_the_way = filter.left_out_with(&top);
        filter.returned = returned(&top);
        filter.shares_ids = top.ids;
        Ok(filter)
    }

    /// The built-ins plus `text` (gitignore syntax, paths relative to the sync root). A line
    /// that doesn't parse matches nothing, as in Git, and is listed in [`Filter::skipped`]; but
    /// those meant as negations (starting with `!`, after any indent) are an error, naming
    /// them all ([`FilterError::Negation`]): skipped, they would ignore what they keep synced.
    pub fn from_rules(text: &str, case_insensitive: bool) -> Result<Self, FilterError> {
        let failed = |e: ignore::Error| FilterError::Rules {
            message: e.to_string(),
        };
        let mut builder = GitignoreBuilder::new("");
        // Never fails (its `Result` is a leftover of the `ignore` crate's).
        let _ = builder.case_insensitive(case_insensitive);
        let mut skipped = Vec::new();
        // Negations that don't parse (indented too: meant as ones), with why the first doesn't.
        let mut negations: Option<(Vec<usize>, String)> = None;
        for (i, line) in text.lines().enumerate() {
            if let Err(e) = builder.add_line(None, line) {
                if line.trim_start().starts_with('!') {
                    negations
                        .get_or_insert_with(|| (Vec::new(), e.to_string()))
                        .0
                        .push(i + 1);
                    continue;
                }
                skipped.push(SkippedRule {
                    line: i + 1,
                    message: e.to_string(),
                });
            }
        }
        if let Some((lines, message)) = negations {
            return Err(FilterError::Negation { lines, message });
        }
        let rules = builder.build().map_err(failed)?;
        Ok(Filter {
            rules,
            text: text.to_owned(),
            case_insensitive,
            skipped,
            shared: Vec::new(),
            in_the_way: false,
            shares_ids: Vec::new(),
            returned: Vec::new(),
        })
    }

    /// The filter leaving out the folders named `names` at the top too: those that hold what
    /// other identities share ([`crate::shares`]), instead of any it left out so before.
    pub fn with_shared<S: AsRef<str>>(mut self, names: impl IntoIterator<Item = S>) -> Self {
        self.shared = self.top_keys(names);
        self
    }

    /// Whether the folder at the top of `root` that holds what other identities share isn't
    /// what this filter leaves out, or is to give way to something else there of its name
    /// ([`Filter::in_the_way`]), or can't be told: [`crate::shares::Top`] alone, the rules not
    /// read again.
    pub fn shared_changed(&self, root: &Path) -> bool {
        crate::shares::Top::read(root).map_or(true, |top| {
            let shared = self.top_keys(&top.shares);
            self.top_differs(&shared, self.left_out_with(&top), &returned(&top))
        })
    }

    /// Whether the top, as found now, isn't the one this filter was read at, or has the
    /// folder of shares give way: other folders of shares (`shared`, as their names
    /// compare), something of its name beside it (`in_the_way`), or other folders that may
    /// be it, back (`returned`: by name, so one gone and another come count).
    fn top_differs(&self, shared: &[String], in_the_way: bool, returned: &[String]) -> bool {
        shared != self.shared || in_the_way || returned != self.returned
    }

    /// Whether something else at `top` has the name of a folder of shares this filter leaves
    /// out, as names compare there: what the volume tells apart and the client doesn't
    /// (`STRASSE` beside `Straße`).
    fn left_out_with(&self, top: &crate::shares::Top) -> bool {
        top.others.iter().any(|other| self.is_shared(other))
    }

    /// Whether, when the filter was read ([`Filter::load`]), something else at the top had the
    /// name of a folder that holds what other identities share, as names compare there: what
    /// the volume tells apart and the client doesn't. This filter leaves it out too, until the
    /// folder of shares gives way ([`crate::shares::place`], as the sync folder opens).
    pub fn in_the_way(&self) -> bool {
        self.in_the_way
    }

    /// Whether this filter, read a while ago, is to be read again given `fresh`, read now:
    /// it decides otherwise ([`Filter::fingerprint`]), or the folder of shares is to give
    /// way ([`Filter::in_the_way`]) or may be back ([`Filter::returned`]).
    pub fn changed_from(&self, fresh: &Filter) -> bool {
        fresh.fingerprint() != self.fingerprint()
            || self.top_differs(&fresh.shared, fresh.in_the_way, &fresh.returned)
    }

    /// The folders at the top that held the marker of the folder of shares on record when
    /// the filter was read, that folder not being there ([`crate::shares::Top::returned`]),
    /// by name: each may be it, back as a copy. Others now mean the sync folder is to be
    /// opened again, for one to be taken ([`crate::shares::adopt`]); the same, that none was
    /// (it is synced as the user's own, or there are several).
    pub fn returned(&self) -> &[String] {
        &self.returned
    }

    /// Whether `file` is a file id on record for the folder of shares when the filter was
    /// read ([`Filter::load`]), wherever that folder is: at the top it is left out by its
    /// name ([`Filter::shared`]); moved into another folder (a slip of the hand) it has no
    /// name to go by, and a walk knows it by its id, to look for its marker
    /// ([`crate::shares::Record::holds`]) before it reports or enters it. Never where none is
    /// on record, or the volume's file ids aren't stable ([`Filter::knows_shares_by_id`]).
    pub fn is_shares_id(&self, file: u128) -> bool {
        self.shares_ids.contains(&file)
    }

    /// Whether a file id is on record for the folder of shares ([`Filter::is_shares_id`]).
    pub fn knows_shares_by_id(&self) -> bool {
        !self.shares_ids.is_empty()
    }

    /// The folders at the top left out as holding what other identities share
    /// ([`Filter::with_shared`]), as their names compare.
    pub fn shared(&self) -> &[String] {
        &self.shared
    }

    /// `names` as [`Filter::shared`] holds them: each as it compares, sorted, once.
    fn top_keys<S: AsRef<str>>(&self, names: impl IntoIterator<Item = S>) -> Vec<String> {
        let keys = names.into_iter();
        let mut keys: Vec<String> = keys
            .map(|name| top_key(name.as_ref(), self.case_insensitive).into_owned())
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// The lines of the rules that aren't valid rules, skipped.
    pub fn skipped(&self) -> &[SkippedRule] {
        &self.skipped
    }

    /// The number of user rules (negations included).
    pub fn rule_count(&self) -> usize {
        self.rules.num_ignores() as usize + self.rules.num_whitelists() as usize
    }

    /// Changes whenever the filter would decide differently: other rules, another junk list,
    /// another case sensitivity, other folders that hold what other identities share, other
    /// file ids on record for that folder (it is left out by them below the top: a walk of
    /// the whole folder takes out what the DB has of it there from before). Nothing is added
    /// where no id is on record: such a folder's fingerprint is as it was.
    pub fn fingerprint(&self) -> String {
        let ids = match self.shares_ids.is_empty() {
            true => String::new(),
            false => format!("; shares' ids {:?}", self.shares_ids),
        };
        format!(
            "junk {JUNK_VERSION}; case-insensitive {}; shared {:?}{ids}; rules:\n{}",
            self.case_insensitive, self.shared, self.text
        )
    }

    /// The verdict on the entry `name` in `folder` (its `/`-separated path, `""` for the root),
    /// assuming the folder itself is synced (a walk never enters a filtered folder).
    pub fn in_folder(&self, folder: &str, name: &str, is_dir: bool) -> Verdict {
        let at_root = folder.is_empty();
        if let Some(verdict) = self.by_name(name, at_root) {
            return verdict;
        }
        if self.rules.is_empty() {
            return Verdict::Sync; // no path to build
        }
        let path = if at_root {
            name.to_owned()
        } else {
            format!("{folder}/{name}")
        };
        self.by_rules(&path, is_dir)
    }

    /// The verdict on the item at `rel`: the first of its folders (outermost first) or itself
    /// that isn't synced decides.
    pub fn path(&self, rel: &RelPath, is_dir: bool) -> Verdict {
        self.names(rel.components(), is_dir)
    }

    /// [`Filter::path`] for a path given as its names, outermost first (a remote node's
    /// ancestry, say).
    pub fn names<'a>(&self, names: impl IntoIterator<Item = &'a str>, is_dir: bool) -> Verdict {
        let mut path = String::new();
        let mut names = names.into_iter().peekable();
        while let Some(name) = names.next() {
            let at_root = path.is_empty();
            if !at_root {
                path.push('/');
            }
            path.push_str(name);
            let last = names.peek().is_none();
            let verdict = self.check(&path, name, at_root, !last || is_dir);
            if !verdict.is_sync() {
                return verdict;
            }
        }
        Verdict::Sync
    }

    fn check(&self, path: &str, name: &str, at_root: bool, is_dir: bool) -> Verdict {
        self.by_name(name, at_root)
            .unwrap_or_else(|| self.by_rules(path, is_dir))
    }

    /// The verdict the name alone gives (a state folder, a folder that holds what other
    /// identities share, junk), if any. A state folder at any depth: below the top it is
    /// another sync root's (a folder of shares that lost its marker, a sync folder moved in),
    /// whose DB and half-done downloads make no sense on another device, and whose marker
    /// there would be taken for a set-up sync folder's. In any case or spelling a volume may
    /// take for it (one that ignores case is most): another writer's `.HBSYNC` at the top
    /// would be this client's own folder there, and what is in it would land in it (an
    /// `ignore` file setting this device's rules).
    fn by_name(&self, name: &str, at_root: bool) -> Option<Verdict> {
        if is_state_dir(name) || (at_root && self.is_shared(name)) {
            Some(Verdict::Excluded)
        } else if is_junk(name) {
            Some(Verdict::Junk)
        } else {
            None
        }
    }

    /// Whether `name`, at the top, is left out as the name of a folder that holds what other
    /// identities share ([`Filter::shared`]).
    pub fn is_shared(&self, name: &str) -> bool {
        if self.shared.is_empty() {
            return false;
        }
        // An ASCII name's key is the name, in lower case where the volume ignores case: asked
        // of every event's path, so without building one.
        if name.is_ascii() {
            let same = |s: &String| match self.case_insensitive {
                true => s.eq_ignore_ascii_case(name),
                false => s == name,
            };
            return self.shared.iter().any(same);
        }
        let key = top_key(name, self.case_insensitive);
        self.shared.iter().any(|s| *s == key)
    }

    fn by_rules(&self, path: &str, is_dir: bool) -> Verdict {
        if self.rules.matched(path, is_dir).is_ignore() {
            Verdict::Ignored
        } else {
            Verdict::Sync
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(s: &str) -> RelPath {
        RelPath::new(s).expect("valid path")
    }

    fn rules(text: &str) -> Filter {
        Filter::from_rules(text, false).expect("valid rules")
    }

    /// The state folder in any case or spelling (`.HBSYNC`, a long s) is excluded as the state
    /// folder: on a volume that ignores case, it is. At any depth, a file of the name too; not
    /// a name near it (an item parked for a move, `.hbsync-move-…`).
    #[test]
    fn the_state_folder_is_excluded_in_any_case() {
        let filter = Filter::default();
        for name in [".hbsync", ".HBSYNC", ".HbSync", ".hb\u{17f}ync"] {
            assert!(is_state_dir(name), "{name}");
            assert_eq!(filter.names([name], true), Verdict::Excluded, "{name}");
            assert_eq!(filter.names(["a", name], true), Verdict::Excluded, "{name}");
            assert_eq!(filter.in_folder("a/b", name, false), Verdict::Excluded);
        }
        for name in [
            ".hbsync2",
            ".hbsync-move-1",
            "hbsync",
            ".hb\u{161}ync",
            "\u{e9}",
        ] {
            assert!(!is_state_dir(name), "{name}");
            assert!(filter.names(["a", name], true).is_sync(), "{name}");
        }
    }

    /// [`is_state_dir`] builds no key for a name that doesn't start with a `.`: no other
    /// character's key starts with one.
    #[test]
    fn only_a_dot_has_a_key_that_starts_with_one() {
        let mut buf = [0; 4];
        for c in (0..=char::MAX as u32).filter_map(char::from_u32) {
            let key = name_key(c.encode_utf8(&mut buf));
            assert_eq!(key.starts_with('.'), c == '.', "{c:?}");
        }
    }

    #[test]
    fn junk_matches_in_any_case_and_nothing_near_it() {
        for name in [
            ".DS_Store",
            ".ds_store",
            "THUMBS.DB",
            "Desktop.ini",
            ".directory",
            "._report.pdf",
            "Icon\r",
            "~$budget.xlsx",
            "~WRL0001.tmp",
            ".~lock.notes.odt#",
            ".notes.txt.swp",
            "film.mkv.part",
            "setup.exe.crdownload",
            "report.pdf.partial",
            "photo.jpg.download",
            ".dropbox",
            ".Dropbox.Cache",
            ".849c9593-d756-4e56-8d6e-42412f2a707b",
            ".tmp.driveupload",
            ".hbsync-shared",
            ".HBSYNC-SHARED",
        ] {
            assert!(is_junk(name), "{name:?}");
        }
        for name in [
            "DS_Store",
            "Icon",
            "a.tmp",
            "notes.txt~",
            "notes.swp",
            "~",
            "Thumbs.db.bak",
            "part",
            "partial",
            "Downloads",
            "lock#",
            "desktop.ini.txt",
            "Dropbox",
            ".dropbox.txt",
            ".tmp",
            ".hbsync-shared-2",
        ] {
            assert!(!is_junk(name), "{name:?}");
        }
    }

    #[test]
    fn a_state_folder_is_excluded_at_any_depth_with_all_in_it() {
        let f = Filter::default();
        assert_eq!(f.in_folder("", ".hbsync", true), Verdict::Excluded);
        assert_eq!(f.in_folder("a", ".hbsync", true), Verdict::Excluded);
        assert_eq!(f.path(&rel(".hbsync/state.db"), false), Verdict::Excluded);
        let nested = rel("Shared/tom/.hbsync/state.db");
        assert_eq!(f.path(&nested, false), Verdict::Excluded);
        // Whatever the user's rules say: it is no rule of theirs.
        let kept = rules("!.hbsync\n!**/.hbsync/**\n");
        assert_eq!(kept.path(&nested, false), Verdict::Excluded);
    }

    #[test]
    fn rules_follow_gitignore() {
        let f = rules("# comment\n*.log\n!keep.log\nbuild/\n/top.txt\ndocs/*.pdf\n");
        assert_eq!(f.rule_count(), 5);
        let at = |p: &str, is_dir| f.path(&rel(p), is_dir);
        assert_eq!(at("a.log", false), Verdict::Ignored);
        assert_eq!(at("x/y/a.log", false), Verdict::Ignored);
        assert_eq!(at("x/keep.log", false), Verdict::Sync);
        // `build/` names folders only, and everything inside them.
        assert_eq!(at("build", false), Verdict::Sync);
        assert_eq!(at("src/build", true), Verdict::Ignored);
        assert_eq!(at("src/build/out/a.o", false), Verdict::Ignored);
        // Anchored rules.
        assert_eq!(at("top.txt", false), Verdict::Ignored);
        assert_eq!(at("x/top.txt", false), Verdict::Sync);
        assert_eq!(at("docs/a.pdf", false), Verdict::Ignored);
        assert_eq!(at("x/docs/a.pdf", false), Verdict::Sync);
        // Case follows the volume.
        assert_eq!(at("A.LOG", false), Verdict::Sync);
        let ci = Filter::from_rules("*.log\n", true).expect("valid");
        assert_eq!(ci.path(&rel("A.LOG"), false), Verdict::Ignored);
    }

    #[test]
    fn a_path_is_judged_by_its_folders_first() {
        let f = rules("node_modules/\n");
        assert_eq!(f.path(&rel("a/.DS_Store"), false), Verdict::Junk);
        assert_eq!(f.path(&rel("._x/b.txt"), false), Verdict::Junk);
        assert_eq!(
            f.path(&rel("app/node_modules/x/.DS_Store"), false),
            Verdict::Ignored
        );
        assert_eq!(
            f.names(["app", "node_modules", "x.js"], false),
            Verdict::Ignored
        );
        assert_eq!(f.names(["app", "src", "x.js"], false), Verdict::Sync);
        // The entry check trusts its folder.
        assert_eq!(
            f.in_folder("app/node_modules", "x.js", false),
            Verdict::Sync
        );
        assert_eq!(f.in_folder("app", "node_modules", true), Verdict::Ignored);
    }

    /// A line that isn't a valid rule is skipped and matches nothing, as in Git; the others
    /// apply. Each is listed by its line.
    #[test]
    fn a_bad_line_is_skipped_and_the_others_apply() {
        let f = rules("*.log\n[z-a\n\n# a comment\nbuild/\nsrc/[z-a]\n");
        let lines: Vec<usize> = f.skipped().iter().map(|s| s.line).collect();
        assert_eq!(lines, [2, 6]);
        assert!(f.skipped().iter().all(|s| !s.message.is_empty()));
        assert_eq!(f.rule_count(), 2);
        assert_eq!(f.names(["debug.log"], false), Verdict::Ignored);
        assert_eq!(f.names(["build"], true), Verdict::Ignored);
        assert_eq!(f.names(["z"], false), Verdict::Sync);
        assert_eq!(f.names(["[z-a"], false), Verdict::Sync);
        assert_eq!(f.names(["src", "x"], false), Verdict::Sync);
        assert!(rules("*.log\n").skipped().is_empty());
        // Fixing the line changes the fingerprint: the filter change runs again.
        assert_ne!(
            f.fingerprint(),
            rules("*.log\n[a-z]\n\n# a comment\nbuild/\n").fingerprint()
        );
    }

    /// A negation that isn't a valid rule isn't skipped: what it keeps synced would be ignored,
    /// and leave the drive (§10 rule 1). The rules are an error naming the line; one that only
    /// looks like a negation, escaped (`\!`), is skipped as any other bad line.
    #[test]
    fn a_bad_negation_is_an_error_naming_its_line() {
        let bad = Filter::from_rules("build/\n!build/[z-a]\n[z-a\r\n  !x/[z-a]\r\n", false);
        match bad {
            Err(FilterError::Negation { lines, message }) => {
                // Indented too, and in a CRLF file; the plain bad line isn't one.
                assert_eq!(lines, [2, 4]);
                assert!(!message.is_empty());
            }
            other => panic!("not a negation error: {other:?}"),
        }
        let text = Filter::from_rules("*.log\n![z-a\n", false)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            text.contains("line 2") && text.contains("fix it to resume"),
            "{text}"
        );
        let escaped = rules("build/\n\\![z-a\n");
        assert_eq!(
            escaped.skipped().iter().map(|s| s.line).collect::<Vec<_>>(),
            [2]
        );
        // A valid negation applies.
        let ok = rules("build/*\n!build/keep\n");
        assert!(ok.skipped().is_empty());
        assert_eq!(ok.names(["build", "keep"], false), Verdict::Sync);
    }

    /// The folder of shares is the client's own, whatever its name, with all in it; the marker
    /// itself is junk wherever it is. A marker elsewhere makes no folder one: not at the top
    /// (copied into a plain folder), deeper, or in the root.
    #[test]
    fn the_folder_of_shares_is_left_out_and_a_marker_elsewhere_does_nothing() -> io::Result<()> {
        use crate::layout::{SHARED_MARKER, shared_marker};
        use crate::shares::{Made, make};
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        for folder in ["plain", "deep/Shared", "odd"] {
            std::fs::create_dir_all(root.join(folder))?;
        }
        assert_eq!(make(root, "Shared")?, Made::Made);
        for folder in ["plain", "deep/Shared"] {
            std::fs::copy(
                shared_marker(&root.join("Shared")),
                shared_marker(&root.join(folder)),
            )?;
        }
        std::fs::create_dir(shared_marker(&root.join("odd")))?;
        std::fs::write(shared_marker(root), "")?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.join("Shared"), root.join("link"))?;

        let f = Filter::load(root, false).map_err(io::Error::other)?;
        assert_eq!(f.shared(), ["Shared"]);
        assert_eq!(f.in_folder("", "Shared", true), Verdict::Excluded);
        assert_eq!(f.path(&rel("Shared/tom/a.txt"), false), Verdict::Excluded);
        // The name is held there, whatever would take it.
        assert_eq!(f.in_folder("", "Shared", false), Verdict::Excluded);
        assert_eq!(f.in_folder("", "shared", true), Verdict::Sync);
        assert_eq!(f.in_folder("", "plain", true), Verdict::Sync);
        assert_eq!(f.in_folder("", "link", true), Verdict::Sync);
        assert_eq!(f.in_folder("", "deep", true), Verdict::Sync);
        assert_eq!(f.in_folder("deep", "Shared", true), Verdict::Sync);
        assert_eq!(f.in_folder("", "odd", true), Verdict::Sync);
        for folder in ["", "plain", "deep/Shared"] {
            assert_eq!(f.in_folder(folder, SHARED_MARKER, false), Verdict::Junk);
        }

        // Asked without the rules read again: the same folder or not.
        assert!(!f.shared_changed(root));
        assert!(Filter::default().shared_changed(root));
        std::fs::rename(root.join("Shared"), root.join("Renamed by hand"))?;
        assert!(f.shared_changed(root));
        let f = Filter::load(root, false).map_err(io::Error::other)?;
        assert_eq!(f.shared(), ["Renamed by hand"]);
        std::fs::remove_file(shared_marker(&root.join("Renamed by hand")))?;
        assert!(f.shared_changed(root));
        // The one on record is a plain folder now, and another there holds its marker: it
        // may be the folder of shares back as a copy, to be judged as the sync folder opens
        // again, once.
        assert!(Filter::default().shared_changed(root));
        let f = Filter::load(root, false).map_err(io::Error::other)?;
        if f.knows_shares_by_id() {
            assert_eq!(f.returned(), ["plain"]);
        }
        // The ids on record are part of what the filter decides by (below the top), and
        // nothing of them is in the fingerprint of a folder without.
        let ids = |f: &Filter| f.fingerprint().contains("shares' ids");
        assert_eq!(ids(&f), f.knows_shares_by_id());
        assert!(!ids(&Filter::default()));
        assert!(f.shared().is_empty() && !f.shared_changed(root));
        Ok(())
    }

    /// A folder that isn't there, or that can't be looked into, holds no marker; a marker that
    /// can't be looked for is an error, never "none": a folder of shares taken for a plain
    /// one would be sent.
    #[test]
    fn a_marker_that_cant_be_looked_for_isnt_none() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        std::fs::write(root.join("file"), "")?;
        assert!(!holds_marker(&root.join("missing"))?);
        assert!(!holds_marker(&root.join("file"))?);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert!(holds_marker(&root.join("x".repeat(300))).is_err());
            assert_eq!(
                crate::shares::make(root, "closed")?,
                crate::shares::Made::Made
            );
            let closed = root.join("closed");
            std::fs::set_permissions(&closed, std::fs::Permissions::from_mode(0o000))?;
            // Root looks into anything: nothing to see then.
            let seen = holds_marker(&closed)?;
            let found = crate::shares::folders(root)?;
            std::fs::set_permissions(&closed, std::fs::Permissions::from_mode(0o700))?;
            assert_eq!(found.is_empty(), !seen);
        }
        Ok(())
    }

    /// Where the volume ignores case, the folder's name in any case or spelling is the folder.
    #[test]
    fn a_folder_of_shares_holds_its_name_as_the_volume_compares_names() {
        let f = Filter::from_rules("", true).expect("valid").with_shared([
            "Shared",
            "SHARED",
            "Stra\u{df}e",
        ]);
        assert_eq!(f.shared().len(), 2);
        for name in ["Shared", "shared", "SHARED", "\u{17f}hared", "STRASSE"] {
            assert_eq!(f.in_folder("", name, true), Verdict::Excluded, "{name}");
        }
        assert!(f.in_folder("", "Shared-2", true).is_sync());
        assert!(f.in_folder("a", "Shared", true).is_sync());

        // Where it tells case apart, byte for byte; on macOS in any normalization.
        let exact = Filter::from_rules("", false).expect("valid");
        let exact = exact.with_shared(["Partage\u{301}", "Shared"]);
        assert_eq!(exact.in_folder("", "Shared", true), Verdict::Excluded);
        assert!(exact.in_folder("", "shared", true).is_sync());
        assert_eq!(
            exact.in_folder("", "Partage\u{301}", true),
            Verdict::Excluded
        );
        assert!(exact.in_folder("", "partage\u{301}", true).is_sync());
        assert_eq!(
            exact.in_folder("", "Partag\u{e9}", true).is_sync(),
            !cfg!(target_os = "macos")
        );
    }

    #[test]
    fn fingerprints() {
        let a = rules("*.log\n");
        assert_ne!(a.fingerprint(), rules("*.tmp\n").fingerprint());
        assert_ne!(
            a.fingerprint(),
            Filter::from_rules("*.log\n", true)
                .expect("valid")
                .fingerprint()
        );
        assert_eq!(a.fingerprint(), rules("*.log\n").fingerprint());
        // A folder of shares made, renamed or gone leaves other things out.
        let shared = |names: &[&str]| rules("*.log\n").with_shared(names).fingerprint();
        assert_ne!(a.fingerprint(), shared(&["Shared"]));
        assert_ne!(shared(&["Shared"]), shared(&["Shared-2"]));
        assert_eq!(shared(&["b", "a"]), shared(&["a", "b"]));
        assert_eq!(a.fingerprint(), shared(&[]));
    }

    #[test]
    fn load_reads_the_rules_file() -> Result<(), FilterError> {
        let dir = tempfile::tempdir().map_err(|source| FilterError::Read {
            path: PathBuf::new(),
            source,
        })?;
        assert_eq!(Filter::load(dir.path(), false)?.rule_count(), 0);
        let state = crate::layout::state_dir(dir.path());
        std::fs::create_dir_all(&state)
            .and_then(|()| std::fs::write(crate::layout::ignore_file(dir.path()), "*.log\n"))
            .map_err(|source| FilterError::Read {
                path: state.clone(),
                source,
            })?;
        let f = Filter::load(dir.path(), false)?;
        assert_eq!(f.path(&rel("a.log"), false), Verdict::Ignored);
        Ok(())
    }
}
