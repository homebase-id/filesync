//! What is never synced: the client's own folder, built-in junk, and the user's ignore rules.
//!
//! Junk is disposable (OS metadata, lock and swap files, partial downloads): it goes to the
//! trash with its folder. Ignored items are the user's data kept on this device only: they
//! keep their folder. Both are invisible to sync in either direction (sync semantics §10).

use std::io;
use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use hbsync_platform::RelPath;

/// The user's ignore rules (gitignore syntax), in the state folder: per device, never synced.
pub const IGNORE_FILE: &str = "ignore";

/// Bumped whenever the built-in junk list changes, so a changed list counts as a rule change.
const JUNK_VERSION: u32 = 1;

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
    // Downloads in progress (Chrome, Firefox): they get their real name when done.
    Pat::Suffix(".crdownload"),
    Pat::Suffix(".part"),
];

fn starts(name: &str, p: &str) -> bool {
    name.len() >= p.len() && name.as_bytes()[..p.len()].eq_ignore_ascii_case(p.as_bytes())
}

fn ends(name: &str, s: &str) -> bool {
    name.len() >= s.len()
        && name.as_bytes()[name.len() - s.len()..].eq_ignore_ascii_case(s.as_bytes())
}

/// Whether `name` is on the built-in junk list (at any depth, in any case).
pub fn is_junk(name: &str) -> bool {
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
    /// A rule in `.hbsync/ignore` that doesn't parse (line 1-based; 0 for the whole set).
    #[error(".hbsync/ignore, line {line}: {message}")]
    Rule { line: usize, message: String },
}

/// The filter of one sync root.
#[derive(Debug, Clone)]
pub struct Filter {
    rules: Gitignore,
    /// The rules as written, for the fingerprint.
    text: String,
    case_insensitive: bool,
}

impl Default for Filter {
    fn default() -> Self {
        Filter {
            rules: Gitignore::empty(),
            text: String::new(),
            case_insensitive: false,
        }
    }
}

impl Filter {
    /// The user's rules from `.hbsync/ignore` under `root`, if the file exists; built-ins only
    /// otherwise. A rule that doesn't parse is an error: skipping it would sync what the user
    /// meant to keep here.
    pub fn load(root: &Path, case_insensitive: bool) -> Result<Self, FilterError> {
        let path = root.join(crate::STATE_DIR).join(IGNORE_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => String::new(),
            Err(source) => return Err(FilterError::Read { path, source }),
        };
        Self::from_rules(&text, case_insensitive)
    }

    /// The built-ins plus `text` (gitignore syntax, paths relative to the sync root). An error
    /// names the first bad line.
    pub fn from_rules(text: &str, case_insensitive: bool) -> Result<Self, FilterError> {
        let bad = |line| {
            move |e: ignore::Error| FilterError::Rule {
                line,
                message: e.to_string(),
            }
        };
        let mut builder = GitignoreBuilder::new("");
        builder.case_insensitive(case_insensitive).map_err(bad(0))?;
        for (i, line) in text.lines().enumerate() {
            builder.add_line(None, line).map_err(bad(i + 1))?;
        }
        let rules = builder.build().map_err(bad(0))?;
        Ok(Filter {
            rules,
            text: text.to_owned(),
            case_insensitive,
        })
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

    /// The verdict on the entry `name` in `dir`, assuming `dir` itself is synced (a walk never
    /// enters a filtered folder).
    pub fn entry(&self, dir: &RelPath, name: &str, is_dir: bool) -> Verdict {
        let path = if dir.is_root() {
            name.to_owned()
        } else {
            format!("{}/{name}", dir.as_str())
        };
        self.check(&path, name, dir.is_root(), is_dir)
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
        if at_root && name == crate::STATE_DIR {
            Verdict::Excluded
        } else if is_junk(name) {
            Verdict::Junk
        } else if self.rules.matched(path, is_dir).is_ignore() {
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

    #[test]
    fn junk_matches_in_any_case_and_nothing_near_it() {
        for name in [
            ".DS_Store",
            ".ds_store",
            "THUMBS.DB",
            "Desktop.ini",
            "._report.pdf",
            "Icon\r",
            "~$budget.xlsx",
            "~WRL0001.tmp",
            ".~lock.notes.odt#",
            ".notes.txt.swp",
            "film.mkv.part",
            "setup.exe.crdownload",
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
            "lock#",
            "desktop.ini.txt",
        ] {
            assert!(!is_junk(name), "{name:?}");
        }
    }

    #[test]
    fn the_state_folder_is_excluded_only_at_the_top() {
        let f = Filter::default();
        assert_eq!(
            f.entry(&RelPath::root(), ".hbsync", true),
            Verdict::Excluded
        );
        assert_eq!(f.entry(&rel("a"), ".hbsync", true), Verdict::Sync);
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
            f.entry(&rel("app/node_modules"), "x.js", false),
            Verdict::Sync
        );
        assert_eq!(f.entry(&rel("app"), "node_modules", true), Verdict::Ignored);
    }

    #[test]
    fn bad_rules_and_fingerprints() {
        assert!(matches!(
            Filter::from_rules("ok\n[z-a\n", false),
            Err(FilterError::Rule { line: 2, .. })
        ));
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
        let state = dir.path().join(crate::STATE_DIR);
        std::fs::create_dir_all(&state)
            .and_then(|()| std::fs::write(state.join(IGNORE_FILE), "*.log\n"))
            .map_err(|source| FilterError::Read {
                path: state.clone(),
                source,
            })?;
        let f = Filter::load(dir.path(), false)?;
        assert_eq!(f.path(&rel("a.log"), false), Verdict::Ignored);
        Ok(())
    }
}
