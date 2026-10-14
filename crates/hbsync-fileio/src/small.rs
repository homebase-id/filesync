//! Small helpers shared across the client: a hash that never changes between builds, and
//! files written in one step.

use std::io::{self, Write};
use std::path::Path;

/// The FNV-1a 64-bit offset basis: the state [`fnv1a64`] starts from.
pub const FNV1A64_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a, 64 bits. Unlike `std`'s hasher it's the same in every build, for names and ids
/// that are stored or shared and must not change with the compiler.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_from(FNV1A64_OFFSET, bytes)
}

/// [`fnv1a64`] continued from `state` (a hash so far, or another seed).
pub fn fnv1a64_from(state: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(state, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

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

    /// The published FNV-1a 64 test vectors: stored names and ids depend on these.
    #[test]
    fn fnv1a64_golden_values() {
        let h = |s: &str| fnv1a64(s.as_bytes());
        assert_eq!(h(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(h("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(h("foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(fnv1a64_from(h("foo"), b"bar"), h("foobar"), "it continues");
    }

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
