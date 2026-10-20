//! Small helpers shared across the client: files written in one step.

use std::io::{self, Write};
use std::path::Path;

/// Writes `bytes` to `path` in one step (its folder made if needed): a crash leaves the old
/// file or the new one. For this program's own files: it is readable by this user only (0600
/// on Unix), and replaces a symlink rather than writing through it.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// Writes `bytes` to `path` in place (its folder made if needed), unless it holds them
/// already; whether it wrote. For files other programs read (autostart entries, launchers): the
/// usual permissions, and through a symlink (dotfiles kept elsewhere).
pub fn write_if_changed(path: &Path, bytes: &[u8]) -> io::Result<bool> {
    if std::fs::read(path).is_ok_and(|old| old == bytes) {
        return Ok(false);
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, bytes)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_once_and_in_one_step() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("sub/file");
        assert!(write_if_changed(&path, b"one")?);
        assert!(!write_if_changed(&path, b"one")?);
        assert_eq!(std::fs::read(&path)?, b"one");
        write_atomic(&path, b"two")?;
        assert_eq!(std::fs::read(&path)?, b"two");
        assert_eq!(
            std::fs::read_dir(dir.path().join("sub"))?.count(),
            1,
            "no temp file left"
        );
        Ok(())
    }
}
