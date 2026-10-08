//! Tier-B catch-up: replay the OS change journal instead of walking the whole tree.
//!
//! A journal only says *where* to look (which folders changed since a stored position). What
//! changed is always decided by rescanning those folders against the DB
//! ([`crate::scan::rescan_dirs`]), so a journal can make catch-up faster but never wrong.
//!
//! Whenever a journal can't vouch for completeness (wrapped, recreated, dropped events, a new
//! volume) catch-up falls back to the full walk (tier C) and stores a fresh position taken
//! *before* the walk, so changes made during the walk are replayed next time.

use std::path::Path;

use hbsync_platform::RelPath;
pub use hbsync_platform::journal::{
    ChangeJournal, JournalDelta, JournalError, Touched, open_journal,
};
use hbsync_state::StateDb;
use hbsync_types::NodeKind;

use crate::scan::{ScanError, ScanOptions, ScanReport, rescan_dirs, scan};

/// The state DB key of the stored journal position.
pub const CURSOR_KEY: &str = "local.journal_cursor";

/// How catch-up ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tier {
    /// Tier B: only the folders the journal named were rescanned.
    Journal { dirs: usize },
    /// Tier C: the whole tree was walked, for the given reason.
    FullWalk { reason: String },
}

#[derive(Debug)]
pub struct CatchUp {
    pub tier: Tier,
    pub report: ScanReport,
}

/// Brings `local_items` and the dirty set up to date with the disk, using the journal when it
/// can and the full walk otherwise.
pub fn catch_up<J: ChangeJournal + ?Sized>(
    db: &mut StateDb,
    root: &Path,
    journal: Option<&mut J>,
    opts: &ScanOptions,
) -> Result<CatchUp, ScanError> {
    let stored = db.read(|s| s.get_kv(CURSOR_KEY))?;
    let Some(journal) = journal else {
        let report = scan(db, root, opts)?;
        return Ok(CatchUp {
            tier: Tier::FullWalk {
                reason: "no journal".into(),
            },
            report,
        });
    };
    let reason = match stored {
        None => "no stored journal position".to_owned(),
        Some(cursor) => match journal.since(&cursor) {
            Ok(delta) if !delta.rescan_all => {
                let dirs = resolve_touched(db, root, &delta.touched)?;
                let report = rescan_dirs(db, root, &dirs, opts)?;
                db.write(|s| s.set_kv(CURSOR_KEY, &delta.cursor))?;
                return Ok(CatchUp {
                    tier: Tier::Journal { dirs: dirs.len() },
                    report,
                });
            }
            Ok(_) => "journal asked for a full rescan".to_owned(),
            Err(e) => e.to_string(),
        },
    };
    // Position first: anything that changes during the walk is replayed next time.
    let position = journal.now();
    let report = scan(db, root, opts)?;
    db.write(|s| match &position {
        Ok(cursor) => s.set_kv(CURSOR_KEY, cursor),
        Err(_) => s.delete_kv(CURSOR_KEY),
    })?;
    Ok(CatchUp {
        tier: Tier::FullWalk { reason },
        report,
    })
}

/// Turns journal locations into folders below the root. Locations outside the root, and ids of
/// folders the DB doesn't know, are dropped: a new folder's creation is recorded in its parent.
fn resolve_touched(
    db: &mut StateDb,
    root: &Path,
    touched: &[Touched],
) -> Result<Vec<RelPath>, ScanError> {
    let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let root_id = hbsync_platform::stat(root).ok().map(|m| m.file_id);
    let mut dirs = Vec::new();
    for t in touched {
        match t {
            Touched::Dir(abs) => {
                let rel = abs
                    .strip_prefix(&canonical_root)
                    .or_else(|_| abs.strip_prefix(root));
                if let Ok(rel) = rel
                    && let Some(path) = relpath_prefix(rel)
                {
                    dirs.push(path);
                }
            }
            Touched::DirId(id) if Some(*id) == root_id => dirs.push(RelPath::root()),
            Touched::DirId(id) => {
                let rows = db.read(|s| s.local_by_file_id(id))?;
                dirs.extend(
                    rows.into_iter()
                        .filter(|r| r.kind == NodeKind::Folder)
                        .map(|r| r.path),
                );
            }
        }
    }
    dirs.sort();
    dirs.dedup();
    Ok(dirs)
}

/// The longest valid [`RelPath`] prefix of a relative native path (a component that isn't
/// valid UTF-8 or a valid name ends it, so the containing folder is rescanned instead).
fn relpath_prefix(rel: &Path) -> Option<RelPath> {
    let mut path = RelPath::root();
    for component in rel.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        match name.to_str().map(|n| path.join(n)) {
            Some(Ok(next)) => path = next,
            _ => break,
        }
    }
    Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_fileio::HashAlgorithm;
    use hbsync_platform::FsFamily;
    use std::fs;

    /// A scripted journal for testing the catch-up logic on every OS.
    #[derive(Debug, Default)]
    struct FakeJournal {
        position: u64,
        answers: Vec<Result<JournalDelta, String>>,
    }

    impl ChangeJournal for FakeJournal {
        fn now(&mut self) -> Result<String, JournalError> {
            Ok(format!("fake:{}", self.position))
        }
        fn since(&mut self, _cursor: &str) -> Result<JournalDelta, JournalError> {
            match self.answers.pop() {
                Some(Ok(delta)) => Ok(delta),
                Some(Err(msg)) => Err(JournalError::Unusable(msg)),
                None => Ok(JournalDelta {
                    cursor: self.now()?,
                    ..Default::default()
                }),
            }
        }
    }

    fn opts(root: &Path) -> ScanOptions {
        ScanOptions {
            algorithm: HashAlgorithm::Blake3,
            fs_family: hbsync_platform::fskind::detect(root).unwrap_or(FsFamily::Other),
            exclude_at_root: vec![crate::STATE_DIR.into()],
        }
    }

    #[test]
    fn falls_back_then_uses_journal() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        fs::create_dir_all(root.join("a/b"))?;
        fs::write(root.join("a/b/f"), b"1")?;
        let mut db = StateDb::open_in_memory()?;
        let mut journal = FakeJournal {
            position: 7,
            ..Default::default()
        };

        // First run: no stored position → full walk, position stored.
        let first = catch_up(&mut db, root, Some(&mut journal), &opts(root))?;
        assert!(matches!(first.tier, Tier::FullWalk { .. }));
        assert_eq!(first.report.added, 3);
        assert_eq!(
            db.read(|s| s.get_kv(CURSOR_KEY))?.as_deref(),
            Some("fake:7")
        );

        // The journal names the folder; only it is rescanned.
        fs::write(root.join("a/b/g"), b"2")?;
        let canonical = root.canonicalize()?;
        journal.answers.push(Ok(JournalDelta {
            touched: vec![
                Touched::Dir(canonical.join("a/b")),
                Touched::Dir("/elsewhere".into()),
            ],
            rescan_all: false,
            cursor: "fake:8".into(),
        }));
        let second = catch_up(&mut db, root, Some(&mut journal), &opts(root))?;
        assert_eq!(second.tier, Tier::Journal { dirs: 1 });
        assert_eq!(second.report.added, 1);
        assert_eq!(
            db.read(|s| s.get_kv(CURSOR_KEY))?.as_deref(),
            Some("fake:8")
        );

        // A folder named by file id.
        fs::write(root.join("a/h"), b"3")?;
        let a_id = hbsync_platform::stat(&root.join("a"))?.file_id;
        journal.answers.push(Ok(JournalDelta {
            touched: vec![Touched::DirId(a_id)],
            rescan_all: false,
            cursor: "fake:9".into(),
        }));
        let third = catch_up(&mut db, root, Some(&mut journal), &opts(root))?;
        assert_eq!(
            (third.tier, third.report.added),
            (Tier::Journal { dirs: 1 }, 1)
        );

        // The journal wrapped → full walk with a fresh position.
        journal.position = 42;
        journal.answers.push(Err("wrapped".into()));
        let fourth = catch_up(&mut db, root, Some(&mut journal), &opts(root))?;
        assert!(matches!(fourth.tier, Tier::FullWalk { reason } if reason.contains("wrapped")));
        assert_eq!(
            db.read(|s| s.get_kv(CURSOR_KEY))?.as_deref(),
            Some("fake:42")
        );
        Ok(())
    }

    /// The OS journal end to end: tier B on Windows (USN) and macOS (FSEvents), tier C on Linux.
    #[test]
    fn real_journal_catch_up() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir_all(root.join("a/b"))?;
        fs::write(root.join("a/b/f"), b"1")?;
        let opts = opts(&root);
        let mut journal = open_journal(&root, opts.fs_family);
        if cfg!(any(windows, target_os = "macos")) {
            assert!(
                journal.is_some(),
                "{:?} should have a journal",
                opts.fs_family
            );
        }
        let mut db = StateDb::open_in_memory()?;
        let first = catch_up(&mut db, &root, journal.as_deref_mut(), &opts)?;
        assert!(matches!(first.tier, Tier::FullWalk { .. }));

        fs::write(root.join("a/b/g"), b"2")?;
        for _ in 0..20 {
            let next = catch_up(&mut db, &root, journal.as_deref_mut(), &opts)?;
            if journal.is_some() {
                assert!(matches!(next.tier, Tier::Journal { .. }), "{:?}", next.tier);
            }
            if next.report.added == 1 {
                return Ok(());
            }
            // FSEvents delivers with a small delay.
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        Err("the journal never reported the new file".into())
    }

    #[test]
    fn relpath_prefix_stops_at_bad_components() {
        assert_eq!(relpath_prefix(Path::new("a/b")), RelPath::new("a/b").ok());
        assert_eq!(relpath_prefix(Path::new("")), Some(RelPath::root()));
    }
}
