//! What is never synced: the client's own folder, built-in junk, and the user's ignore rules.
//!
//! Junk is disposable (OS metadata, lock and swap files, partial downloads): it goes to the
//! trash with its folder. Ignored items are the user's data kept on this device only: they
//! keep their folder. Both are invisible to sync in either direction (sync semantics §10).

use std::io;
use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use hbsync_platform::RelPath;
use hbsync_platform::name::name_key;

/// Bumped whenever the built-in junk list changes, so a changed list counts as a rule change.
const JUNK_VERSION: u32 = 3;

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
];

fn starts(name: &str, p: &str) -> bool {
    name.len() >= p.len() && name.as_bytes()[..p.len()].eq_ignore_ascii_case(p.as_bytes())
}

fn ends(name: &str, s: &str) -> bool {
    name.len() >= s.len()
        && name.as_bytes()[name.len() - s.len()..].eq_ignore_ascii_case(s.as_bytes())
}

/// The state folder's ([`crate::STATE_DIR`]) name key (`name_key`): its name in any case or
/// spelling.
pub fn state_dir_key() -> &'static str {
    use std::sync::LazyLock;
    static KEY: LazyLock<String> = LazyLock::new(|| name_key(crate::STATE_DIR));
    &KEY
}

/// Whether `name` is the state folder's ([`crate::STATE_DIR`]) in any case or spelling: caseless
/// and normalization-insensitive (`name_key`), as a volume that ignores case compares names.
fn is_state_dir(name: &str) -> bool {
    name.len() <= crate::STATE_DIR.len() * 4 && name_key(name) == state_dir_key()
}

/// Whether `name` is on the built-in junk list (at any depth, in any case).
pub(crate) fn is_junk(name: &str) -> bool {
    JUNK.iter().any(|pat| match *pat {
        Pat::Name(n) => name.eq_ignore_ascii_case(n),
        Pat::Prefix(p) => starts(name, p),
        Pat::Suffix(s) => ends(name, s),
        Pat::Both(p, s) => name.len() >= p.len() + s.len() && starts(name, p) && ends(name, s),
    })
}

/// What the filter says about an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Sync,
    /// The client's own folder at the top of the sync root.
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
    #[error(".hbsync/ignore: {message}")]
    Rules { message: String },
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
}

impl Default for Filter {
    fn default() -> Self {
        Filter {
            rules: Gitignore::empty(),
            text: String::new(),
            case_insensitive: false,
            skipped: Vec::new(),
        }
    }
}

impl Filter {
    /// The user's rules from `.hbsync/ignore` under `root`, if the file exists; built-ins only
    /// otherwise. A line that isn't a valid rule is skipped ([`Filter::skipped`]); a file that
    /// can't be read is an error: its rules are unknown, not mistyped.
    pub fn load(root: &Path, case_insensitive: bool) -> Result<Self, FilterError> {
        let path = crate::layout::ignore_file(root);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
            Err(source) => return Err(FilterError::Read { path, source }),
        };
        Self::from_rules(&text, case_insensitive)
    }

    /// The built-ins plus `text` (gitignore syntax, paths relative to the sync root). A line
    /// that doesn't parse matches nothing, as in Git, and is listed in [`Filter::skipped`].
    pub fn from_rules(text: &str, case_insensitive: bool) -> Result<Self, FilterError> {
        let failed = |e: ignore::Error| FilterError::Rules {
            message: e.to_string(),
        };
        let mut builder = GitignoreBuilder::new("");
        // Never fails (its `Result` is a leftover of the `ignore` crate's).
        let _ = builder.case_insensitive(case_insensitive);
        let mut skipped = Vec::new();
        for (i, line) in text.lines().enumerate() {
            if let Err(e) = builder.add_line(None, line) {
                skipped.push(SkippedRule {
                    line: i + 1,
                    message: e.to_string(),
                });
            }
        }
        let rules = builder.build().map_err(failed)?;
        Ok(Filter {
            rules,
            text: text.to_owned(),
            case_insensitive,
            skipped,
        })
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
    /// another case sensitivity.
    pub fn fingerprint(&self) -> String {
        format!(
            "junk {JUNK_VERSION}; case-insensitive {}; rules:\n{}",
            self.case_insensitive, self.text
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

    /// The verdict the name alone gives (the state folder, junk), if any. The state folder in
    /// any case or spelling a volume may take for it (one that ignores case is most): another
    /// writer's `.HBSYNC` there would be this client's own folder, and what is in it would land
    /// in it (an `ignore` file setting this device's rules).
    fn by_name(&self, name: &str, at_root: bool) -> Option<Verdict> {
        if at_root && is_state_dir(name) {
            Some(Verdict::Excluded)
        } else if is_junk(name) {
            Some(Verdict::Junk)
        } else {
            None
        }
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

    /// The state folder at the top in any case or spelling (`.HBSYNC`, a long s) is excluded
    /// as the state folder: on a volume that ignores case, it is. Not deeper, nor a name near it.
    #[test]
    fn the_state_folder_is_excluded_in_any_case() {
        let filter = Filter::default();
        for name in [".hbsync", ".HBSYNC", ".HbSync", ".hb\u{17f}ync"] {
            assert_eq!(filter.names([name], true), Verdict::Excluded, "{name}");
        }
        assert!(filter.names(["a", ".HBSYNC"], true).is_sync());
        assert!(filter.names([".hbsync2"], true).is_sync());
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
        ] {
            assert!(!is_junk(name), "{name:?}");
        }
    }

    #[test]
    fn the_state_folder_is_excluded_only_at_the_top() {
        let f = Filter::default();
        assert_eq!(f.in_folder("", ".hbsync", true), Verdict::Excluded);
        assert_eq!(f.in_folder("a", ".hbsync", true), Verdict::Sync);
        assert_eq!(f.path(&rel(".hbsync/state.db"), false), Verdict::Excluded);
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
