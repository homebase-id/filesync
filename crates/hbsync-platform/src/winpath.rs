//! Windows extended-length paths.
//!
//! Win32 limits ordinary paths to `MAX_PATH` (260) characters unless the process opts into long
//! paths. The `\\?\` prefix lifts the limit for every API we use, but it also disables `/`
//! translation and `.`/`..` processing, so it is only added to absolute, normalized paths.
//!
//! The conversion works on strings so it is tested on every OS; [`extended`] applies it to a
//! [`Path`](std::path::Path) on Windows.

/// Adds the `\\?\` prefix to an absolute Windows path:
/// `C:\a` → `\\?\C:\a`, `\\server\share\a` → `\\?\UNC\server\share\a`.
///
/// Paths that already carry a `\\?\` or `\\.\` prefix and relative paths are returned unchanged.
/// Forward slashes are converted to backslashes, since the kernel won't do it for prefixed paths.
pub fn to_extended(path: &str) -> String {
    if path.starts_with(r"\\?\") || path.starts_with(r"\\.\") {
        return path.to_owned();
    }
    let path = path.replace('/', r"\");
    if let Some(unc) = path.strip_prefix(r"\\") {
        return format!(r"\\?\UNC\{unc}");
    }
    let bytes = path.as_bytes();
    let is_drive_absolute =
        bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && bytes[2] == b'\\';
    if is_drive_absolute {
        format!(r"\\?\{path}")
    } else {
        path
    }
}

/// Returns `path` with the extended-length prefix on Windows; unchanged elsewhere.
#[cfg(windows)]
pub(crate) fn extended(path: &std::path::Path) -> std::path::PathBuf {
    match path.to_str() {
        Some(s) => std::path::PathBuf::from(to_extended(s)),
        // Not valid Unicode: can't come from our own path model, leave it alone.
        None => path.to_path_buf(),
    }
}

/// Returns `path` with the extended-length prefix on Windows; unchanged elsewhere.
#[cfg(not(windows))]
pub(crate) fn extended(path: &std::path::Path) -> std::path::PathBuf {
    path.to_path_buf()
}

/// Takes the `\\?\` prefix off again, for the shell and WinRT, which don't take it (what
/// `canonicalize` returns on Windows carries it): `\\?\C:\a` → `C:\a`,
/// `\\?\UNC\server\share\a` → `\\server\share\a`. Other paths are returned unchanged.
pub fn to_plain(path: &str) -> String {
    if let Some(unc) = path.strip_prefix(r"\\?\UNC\") {
        return format!(r"\\{unc}");
    }
    match path.strip_prefix(r"\\?\") {
        Some(rest) if rest.as_bytes().get(1) == Some(&b':') => rest.to_owned(),
        _ => path.to_owned(),
    }
}

/// Returns `path` without the extended-length prefix ([`to_plain`]).
pub fn plain(path: &std::path::Path) -> std::path::PathBuf {
    match path.to_str() {
        Some(s) => std::path::PathBuf::from(to_plain(s)),
        None => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drive_paths() {
        assert_eq!(to_extended(r"C:\Users\a"), r"\\?\C:\Users\a");
        assert_eq!(to_extended("d:/x/y"), r"\\?\d:\x\y");
    }

    #[test]
    fn unc_paths() {
        assert_eq!(to_extended(r"\\srv\share\dir"), r"\\?\UNC\srv\share\dir");
    }

    #[test]
    fn already_prefixed_or_relative() {
        assert_eq!(to_extended(r"\\?\C:\a"), r"\\?\C:\a");
        assert_eq!(to_extended(r"\\.\pipe\x"), r"\\.\pipe\x");
        assert_eq!(to_extended(r"rel\path"), r"rel\path");
        assert_eq!(to_extended("C:"), "C:");
    }

    #[test]
    fn plain_paths() {
        assert_eq!(to_plain(r"\\?\C:\Users\a"), r"C:\Users\a");
        assert_eq!(to_plain(r"\\?\UNC\srv\share\dir"), r"\\srv\share\dir");
        assert_eq!(to_plain(r"C:\Users\a"), r"C:\Users\a");
        assert_eq!(to_plain(r"\\?\Volume{x}\a"), r"\\?\Volume{x}\a");
        for p in [r"C:\Users\a", r"\\srv\share\dir"] {
            assert_eq!(to_plain(&to_extended(p)), p);
        }
    }
}
