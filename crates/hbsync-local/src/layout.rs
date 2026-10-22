//! What the client keeps in a synced folder's [`STATE_DIR`], and the names it gives what it moves
//! about in the folder, in one place.

use std::path::{Path, PathBuf};

use hbsync_types::{ContentHash, NodeId, Uuid};

use crate::STATE_DIR;

/// The folder's [`STATE_DIR`].
pub fn state_dir(root: &Path) -> PathBuf {
    root.join(STATE_DIR)
}

/// The folder's marker ([`crate::Marker`]).
pub(crate) fn marker_file(root: &Path) -> PathBuf {
    state_dir(root).join("folder")
}

/// The user's ignore rules for this device ([`crate::Filter::load`]).
pub fn ignore_file(root: &Path) -> PathBuf {
    state_dir(root).join(IGNORE_FILE)
}

/// Downloads in progress: on the folder's volume, so they land with an atomic rename.
pub fn tmp_dir(root: &Path) -> PathBuf {
    state_dir(root).join("tmp")
}

/// What syncing took from the folder (an entry each, [`trash_entry`]), until the purge.
pub fn trash_dir(root: &Path) -> PathBuf {
    state_dir(root).join("trash")
}

/// The name of `node`'s entry in the [`trash_dir`]: `<Unix s>-<node>`, by when it went there
/// (`at`, Unix s; a second or so later where the node's entry of that second was taken).
pub fn trash_entry(at: u64, node: NodeId) -> String {
    format!("{at}-{}", node.0.simple())
}

/// When a trash entry ([`trash_entry`]) went there, Unix s.
pub fn trashed_at(entry: &str) -> Option<u64> {
    entry.split_once('-')?.0.parse().ok()
}

/// When a trash entry ([`trash_entry`]) went there, Unix s, and whose it is.
pub fn parse_trash_entry(entry: &str) -> Option<(u64, NodeId)> {
    let node = Uuid::try_parse(entry.split_once('-')?.1).ok()?;
    Some((trashed_at(entry)?, NodeId(node)))
}

/// Where an item being deleted waits, for the moment between its rename out of the folder and
/// its unlink, once checked to be what was synced: on the folder's volume, so it leaves with one
/// rename. Nothing stays there once the delete is committed. Its names are fixed by node and
/// bytes: one taken (left by a crash) is a place the next one waits for, never one to rename
/// over.
pub fn aside_dir(root: &Path) -> PathBuf {
    state_dir(root).join("aside")
}

/// The entries of the [`aside_dir`] of the folder at `root`, each with what it holds (`None`:
/// a name no delete gives).
pub fn aside_entries(root: &Path) -> impl Iterator<Item = (PathBuf, Option<AsideEntry>)> {
    std::fs::read_dir(aside_dir(root))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| {
            let entry = e.file_name().to_str().and_then(AsideEntry::parse);
            (e.path(), entry)
        })
}

/// What an entry of the [`aside_dir`] holds, by its name ([`AsideEntry::name`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsideEntry {
    /// `node`'s folder, being deleted: `<node>`.
    Folder(NodeId),
    /// A file of `node` holding the bytes `hash` (as its row said when it went aside): one being
    /// deleted, or the old version a download replaces. `<node>-<algorithm>-<hex>`.
    File(NodeId, ContentHash),
}

impl AsideEntry {
    /// The entry's name in the [`aside_dir`].
    pub fn name(&self) -> String {
        match self {
            AsideEntry::Folder(node) => node.0.simple().to_string(),
            AsideEntry::File(node, hash) => {
                format!("{}-{}-{}", node.0.simple(), hash.algorithm, hash.to_hex())
            }
        }
    }

    /// The entry a name in the [`aside_dir`] stands for, spelled as [`AsideEntry::name`]
    /// spells it; `None` for anything else.
    pub fn parse(name: &str) -> Option<AsideEntry> {
        let entry = match name.split_once('-') {
            None => AsideEntry::Folder(NodeId(Uuid::try_parse(name).ok()?)),
            Some((node, hash)) => {
                let node = NodeId(Uuid::try_parse(node).ok()?);
                let (algorithm, hex) = hash.split_once('-')?;
                let hash = ContentHash::from_hex(algorithm.parse().ok()?, hex).ok()?;
                AsideEntry::File(node, hash)
            }
        };
        (entry.name() == name).then_some(entry)
    }

    pub fn node(&self) -> NodeId {
        match self {
            AsideEntry::Folder(node) | AsideEntry::File(node, _) => *node,
        }
    }
}

/// The temporary name a node is parked under, in its own folder, while its place is wanted by
/// another (the engine's executor).
pub fn parked_name(node: NodeId) -> String {
    format!(".hbsync-move-{}", node.0.simple())
}

/// The user's ignore rules' file name in [`STATE_DIR`] (gitignore syntax): per device, never
/// synced.
pub const IGNORE_FILE: &str = "ignore";

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_types::HashAlgorithm;

    #[test]
    fn aside_names_read_back() {
        let node = NodeId(Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0));
        let hash = ContentHash {
            algorithm: HashAlgorithm::Sha256,
            bytes: [7; 32],
        };
        for entry in [AsideEntry::Folder(node), AsideEntry::File(node, hash)] {
            assert_eq!(AsideEntry::parse(&entry.name()), Some(entry));
            assert_eq!(entry.node(), node);
        }
        // A file's name fits any volume's 255 bytes, without a character Windows refuses.
        let name = AsideEntry::File(node, hash).name();
        assert!(name.len() < 255 && !name.contains(':'));
        for other in [
            "",
            "x",
            "1700000000-12345678",
            &format!("{}-sha256", node.0.simple()),
        ] {
            assert_eq!(AsideEntry::parse(other), None, "{other}");
        }
    }
}
