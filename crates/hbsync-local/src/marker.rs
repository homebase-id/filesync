//! The marker in a synced folder's [`STATE_DIR`]: which account's state DB holds the folder,
//! and which of its folders it is. The DB lives with the account, outside the folder; a folder
//! without its marker is gone or not mounted, and one whose marker names another account isn't
//! this one's.

use std::io;
use std::path::Path;

use crate::STATE_DIR;

/// The marker's file name in [`STATE_DIR`].
pub const MARKER_FILE: &str = "folder";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    /// The account (identity) whose state DB holds the folder.
    pub identity: String,
    /// The folder's `marker` in that DB.
    pub id: String,
}

impl Marker {
    /// A new marker for a folder of `identity`.
    pub fn new(identity: &str) -> Marker {
        Marker {
            identity: identity.to_owned(),
            id: hbsync_types::Uuid::new_v4().to_string(),
        }
    }

    /// The marker of the folder at `root`; `None` if it has none.
    pub fn read(root: &Path) -> io::Result<Option<Marker>> {
        let text = match std::fs::read_to_string(root.join(STATE_DIR).join(MARKER_FILE)) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let field = |name: &str| {
            text.lines()
                .find_map(|l| l.strip_prefix(name)?.strip_prefix(' '))
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        match (field("identity"), field("marker")) {
            (Some(identity), Some(id)) => Ok(Some(Marker { identity, id })),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: not a Filesync folder marker", root.display()),
            )),
        }
    }

    /// Writes it into the folder at `root` (making its [`STATE_DIR`]).
    pub fn write(&self, root: &Path) -> io::Result<()> {
        let text = format!("identity {}\nmarker {}\n", self.identity, self.id);
        hbsync_fileio::write_atomic(&root.join(STATE_DIR).join(MARKER_FILE), text.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_marker_reads_back() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        assert_eq!(Marker::read(dir.path())?, None);
        let m = Marker::new("anna.example.com");
        m.write(dir.path())?;
        assert_eq!(Marker::read(dir.path())?, Some(m));
        std::fs::write(dir.path().join(STATE_DIR).join(MARKER_FILE), "junk")?;
        assert!(Marker::read(dir.path()).is_err());
        Ok(())
    }
}
