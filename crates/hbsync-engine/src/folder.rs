//! One sync folder as the engine works it: where it is, how its volume tells names apart, and
//! what part of the drive it syncs. The engine owns it; the executor, the view, adoption and
//! recovery read it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use hbsync_fileio::HashAlgorithm;
use hbsync_local::{Filter, ScanOptions};
use hbsync_platform::{FsFamily, Os};

use crate::plan::Masking;
use crate::scope::Scope;

#[derive(Debug, Clone)]
pub struct FolderCtx {
    pub root: PathBuf,
    pub os: Os,
    /// The hash this client writes and keeps in its `local` rows.
    pub algorithm: HashAlgorithm,
    /// Names that differ only in case (or Unicode normalization) are the same file here
    /// (Windows, default macOS). Probed at the root by [`FolderCtx::new`].
    pub case_insensitive: bool,
    /// What this device never syncs (sync semantics §10).
    pub filter: Arc<Filter>,
    /// The part of the drive this folder syncs (§12).
    pub scope: Arc<Scope>,
    /// Whether the root's volume keeps tags, learned at the first tag write on it: a
    /// download's record claims the tags it is to write only where it does.
    pub tags_kept: Arc<OnceLock<bool>>,
    /// Whether a file's id, size and mtime prove its bytes here (the volume keeps mtimes to the
    /// microsecond and stable ids), learned at the first delete that checks a file: elsewhere
    /// the check hashes it (sync semantics §7).
    pub mtime_proves_bytes: Arc<OnceLock<bool>>,
    /// The file system family of the root's volume, learned when first asked.
    pub family: Arc<OnceLock<FsFamily>>,
}

impl FolderCtx {
    /// The folder at `root`, on this OS, with the built-in filter only, syncing the main part of
    /// the drive; its volume probed for case.
    pub fn new(root: &Path) -> Self {
        FolderCtx {
            root: root.to_path_buf(),
            os: Os::current(),
            algorithm: HashAlgorithm::Blake3,
            case_insensitive: probe_case_insensitive(root),
            filter: Arc::default(),
            scope: Arc::new(Scope::main()),
            tags_kept: Arc::default(),
            mtime_proves_bytes: Arc::default(),
            family: Arc::default(),
        }
    }

    /// What the folder leaves out of the drive, for the reconciler to mask.
    pub fn masking(&self) -> Masking<'_> {
        Masking {
            filter: &self.filter,
            scope: &self.scope,
        }
    }

    /// How the scan reads the folder (with a look at its volume: [`ScanOptions::for_root`]).
    pub(crate) fn scan_options(&self) -> ScanOptions {
        ScanOptions {
            filter: Arc::clone(&self.filter),
            // Byte-exact names on case-sensitive Linux volumes (macOS folds normalization
            // whatever its case, and Windows can turn case sensitivity on per folder).
            exact_names: cfg!(target_os = "linux") && !self.case_insensitive,
            ..ScanOptions::for_root(&self.root, self.algorithm)
        }
    }
}

impl FolderCtx {
    /// Whether a file's id, size and mtime prove its bytes on the folder's volume
    /// ([`FolderCtx::mtime_proves_bytes`]): its family keeps fine mtimes and stable ids, and a
    /// probe in the client's state folder reads a sub-second mtime back. Asked once.
    pub(crate) fn mtime_proves_bytes(&self) -> bool {
        *self.mtime_proves_bytes.get_or_init(|| {
            let family = self.family();
            family.has_fine_mtime()
                && family.has_stable_file_ids()
                && hbsync_platform::fskind::probe_fine_mtime(&ensure_state_dir(&self.root))
                    .unwrap_or(false)
        })
    }

    /// Whether an item keeps its file id on this volume when renamed (not FAT, exFAT, a network
    /// or FUSE volume): only then does an id tell an item.
    pub(crate) fn ids_stable(&self) -> bool {
        self.family().has_stable_file_ids()
    }

    fn family(&self) -> FsFamily {
        *self
            .family
            .get_or_init(|| hbsync_platform::fskind::detect(&self.root).unwrap_or(FsFamily::Other))
    }
}

/// Creates the client's state folder at the top of the sync root if needed, hidden on Windows
/// (a leading dot hides it elsewhere). Done at every start, so folders from before are hidden
/// too.
fn ensure_state_dir(root: &Path) -> PathBuf {
    let dir = hbsync_local::layout::state_dir(root);
    let _ = std::fs::create_dir_all(&dir);
    // Only looks: a folder that can't be hidden still works.
    let _ = hbsync_platform::ops::set_hidden(&dir);
    dir
}

/// Probes the volume of the sync root, inside the client's own state folder so nothing shows
/// up in the user's tree. Falls back to the OS default.
fn probe_case_insensitive(root: &Path) -> bool {
    let dir = ensure_state_dir(root);
    match hbsync_platform::fskind::probe_case_sensitive(&dir) {
        Ok(sensitive) => !sensitive,
        Err(_) => cfg!(any(windows, target_os = "macos")),
    }
}
