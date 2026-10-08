//! Names: normalization, the comparison key, and per-OS legality rules.
//!
//! Remote names are Unicode NFC (node schema v1). Uniqueness inside a folder is checked with
//! [`name_key`], which is stricter than any of the three OSes, so a tree that passes the check
//! can be materialized on all of them.

use std::fmt;

use unicode_normalization::UnicodeNormalization;

/// Maximum length of a name in UTF-8 bytes (node schema v1).
pub const MAX_NAME_BYTES: usize = 255;

/// Maximum length of a name on NTFS, in UTF-16 code units.
pub const MAX_WINDOWS_NAME_UNITS: usize = 255;

/// The operating systems a sync root can live on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Os {
    Windows,
    MacOs,
    Linux,
}

impl Os {
    /// The OS this binary was built for. Other Unix targets use the Linux rules.
    pub const fn current() -> Self {
        if cfg!(windows) {
            Os::Windows
        } else if cfg!(target_os = "macos") {
            Os::MacOs
        } else {
            Os::Linux
        }
    }
}

/// Returns `name` in Unicode NFC.
pub fn nfc(name: &str) -> String {
    name.nfc().collect()
}

/// Returns `true` if `name` is already in NFC.
pub fn is_nfc(name: &str) -> bool {
    unicode_normalization::is_nfc(name)
}

/// The key two names are compared by: `NFC(casefold(NFD(name)))`.
///
/// This is Unicode canonical caseless matching (D145) with full case folding, so `"ß"` and
/// `"SS"` collide. That is stricter than NTFS, APFS and ext4, which is the point: names that
/// differ under this key differ on every OS.
pub fn name_key(name: &str) -> String {
    let decomposed: String = name.nfd().collect();
    caseless::default_case_fold_str(&decomposed).nfc().collect()
}

/// Why a name can't be used as-is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameIssue {
    Empty,
    DotOrDotDot,
    /// Contains `/` or NUL, which no OS allows in a component.
    Separator,
    TooLong,
    /// A character that is illegal on this OS (Windows: `<>:"\|?*` and control characters).
    IllegalChar(char),
    /// A Windows device name such as `CON` or `com1.txt`.
    ReservedName,
    /// Windows strips a trailing dot or space.
    TrailingDotOrSpace,
}

impl fmt::Display for NameIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameIssue::Empty => f.write_str("name is empty"),
            NameIssue::DotOrDotDot => f.write_str("name is `.` or `..`"),
            NameIssue::Separator => f.write_str("name contains `/` or NUL"),
            NameIssue::TooLong => f.write_str("name is too long"),
            NameIssue::IllegalChar(c) => write!(f, "name contains illegal character {c:?}"),
            NameIssue::ReservedName => f.write_str("name is a reserved device name"),
            NameIssue::TrailingDotOrSpace => f.write_str("name ends with a dot or a space"),
        }
    }
}

impl std::error::Error for NameIssue {}

/// Checks a remote (portable) name: what node schema v1 allows, independent of any OS.
pub fn check_remote_name(name: &str) -> Result<(), NameIssue> {
    check_component(name)?;
    if name.len() > MAX_NAME_BYTES {
        return Err(NameIssue::TooLong);
    }
    Ok(())
}

/// What every OS and the remote require of one path component.
fn check_component(name: &str) -> Result<(), NameIssue> {
    if name.is_empty() {
        return Err(NameIssue::Empty);
    }
    if name == "." || name == ".." {
        return Err(NameIssue::DotOrDotDot);
    }
    if name.contains(['/', '\0']) {
        return Err(NameIssue::Separator);
    }
    Ok(())
}

/// Checks whether `name` can be used verbatim as a file name on `os`.
///
/// The first problem found is returned. A name that fails here is escaped locally by the name
/// map and keeps its remote spelling.
pub fn check_local_name(name: &str, os: Os) -> Result<(), NameIssue> {
    check_component(name)?;
    if os != Os::Windows && name.len() > MAX_NAME_BYTES {
        return Err(NameIssue::TooLong);
    }
    if os == Os::Windows {
        if let Some(c) = name.chars().find(|&c| is_windows_illegal(c)) {
            return Err(NameIssue::IllegalChar(c));
        }
        // NTFS counts UTF-16 units: an escaped name may be longer in UTF-8 than its remote
        // spelling while still fitting.
        if name.encode_utf16().count() > MAX_WINDOWS_NAME_UNITS {
            return Err(NameIssue::TooLong);
        }
        if name.ends_with(['.', ' ']) {
            return Err(NameIssue::TrailingDotOrSpace);
        }
        if is_windows_reserved(name) {
            return Err(NameIssue::ReservedName);
        }
    }
    Ok(())
}

/// Characters Win32 refuses in a file name component.
pub fn is_windows_illegal(c: char) -> bool {
    matches!(c, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*') || (c as u32) < 0x20
}

/// Win32 device names: reserved with or without an extension, in any case, and also with
/// trailing spaces before the extension (`"CON .txt"`). `COM¹`–`COM³` count too.
pub fn is_windows_reserved(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).trim_end_matches(' ');
    let upper = stem.to_uppercase();
    match upper.as_str() {
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" => true,
        _ => {
            let mut chars = upper.chars();
            let prefix: String = chars.by_ref().take(3).collect();
            let rest: String = chars.collect();
            (prefix == "COM" || prefix == "LPT")
                && matches!(
                    rest.as_str(),
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
        }
    }
}

/// Splits a name into stem and extension the way conflict names need it:
/// `"a.tar.gz"` → (`"a.tar"`, `".gz"`), `".bashrc"` → (`".bashrc"`, `""`).
pub fn split_extension(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(0) | None => (name, ""),
        Some(i) => name.split_at(i),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn key_folds_case_and_normalization() {
        assert_eq!(name_key("Résumé.TXT"), name_key("re\u{301}sume\u{301}.txt"));
        assert_eq!(name_key("STRASSE"), name_key("straße"));
        assert_ne!(name_key("a.txt"), name_key("b.txt"));
        // Greek final sigma folds to sigma.
        assert_eq!(name_key("ΟΔΟΣ"), name_key("οδος"));
    }

    #[test]
    fn nfc_round_trip() {
        let decomposed = "e\u{301}";
        assert!(!is_nfc(decomposed));
        assert_eq!(nfc(decomposed), "é");
        assert!(is_nfc(&nfc(decomposed)));
    }

    #[test]
    fn remote_name_rules() {
        assert_eq!(check_remote_name(""), Err(NameIssue::Empty));
        assert_eq!(check_remote_name(".."), Err(NameIssue::DotOrDotDot));
        assert_eq!(check_remote_name("a/b"), Err(NameIssue::Separator));
        assert_eq!(check_remote_name("a\0b"), Err(NameIssue::Separator));
        assert_eq!(check_remote_name(&"x".repeat(256)), Err(NameIssue::TooLong));
        assert_eq!(check_remote_name(&"x".repeat(255)), Ok(()));
        assert_eq!(check_remote_name("a:b?.txt "), Ok(()));
    }

    #[test]
    fn windows_rules() {
        let w = Os::Windows;
        assert_eq!(check_local_name("a:b", w), Err(NameIssue::IllegalChar(':')));
        assert_eq!(
            check_local_name("tab\there", w),
            Err(NameIssue::IllegalChar('\t'))
        );
        assert_eq!(
            check_local_name("x.", w),
            Err(NameIssue::TrailingDotOrSpace)
        );
        assert_eq!(
            check_local_name("x ", w),
            Err(NameIssue::TrailingDotOrSpace)
        );
        for reserved in [
            "CON",
            "con.txt",
            "Aux.tar.gz",
            "COM1",
            "lpt9.log",
            "COM¹",
            "NUL .txt",
        ] {
            assert_eq!(
                check_local_name(reserved, w),
                Err(NameIssue::ReservedName),
                "{reserved}"
            );
        }
        for fine in ["CONSOLE", "COM10", "COM0", "lpt", "xcon", "a.con"] {
            assert_eq!(check_local_name(fine, w), Ok(()), "{fine}");
        }
        // "€" is 3 UTF-8 bytes and 1 UTF-16 unit: 85 of them = 255 bytes, 85 units.
        assert_eq!(check_local_name(&"€".repeat(85), w), Ok(()));
        // 64 emoji = 256 UTF-8 bytes but 128 units: fine for NTFS (not for the remote).
        assert_eq!(check_local_name(&"😀".repeat(64), w), Ok(()));
        assert_eq!(
            check_local_name(&"😀".repeat(128), w),
            Err(NameIssue::TooLong)
        );
        assert_eq!(
            check_local_name(&"😀".repeat(64), Os::Linux),
            Err(NameIssue::TooLong)
        );
    }

    #[test]
    fn unix_rules_are_permissive() {
        for os in [Os::Linux, Os::MacOs] {
            assert_eq!(check_local_name("a:b?*<>|\\", os), Ok(()));
            assert_eq!(check_local_name("CON", os), Ok(()));
            assert_eq!(check_local_name("trailing. ", os), Ok(()));
        }
    }

    #[test]
    fn extension_split() {
        assert_eq!(split_extension("a.tar.gz"), ("a.tar", ".gz"));
        assert_eq!(split_extension(".bashrc"), (".bashrc", ""));
        assert_eq!(split_extension("README"), ("README", ""));
        assert_eq!(split_extension("x."), ("x", "."));
    }

    proptest! {
        #[test]
        fn key_is_idempotent(s in "\\PC{0,24}") {
            let k = name_key(&s);
            prop_assert_eq!(name_key(&k), k.clone());
        }

        #[test]
        fn key_ignores_normalization_form(s in "\\PC{0,24}") {
            let composed: String = s.nfc().collect();
            let decomposed: String = s.nfd().collect();
            prop_assert_eq!(name_key(&composed), name_key(&decomposed));
        }

        #[test]
        fn key_ignores_ascii_case(s in "[a-zA-Z0-9 ._-]{0,24}") {
            prop_assert_eq!(name_key(&s.to_ascii_uppercase()), name_key(&s.to_ascii_lowercase()));
        }

        #[test]
        fn local_ok_implies_remote_ok(s in "\\PC{0,40}", os in prop_oneof![Just(Os::Windows), Just(Os::MacOs), Just(Os::Linux)]) {
            if check_local_name(&s, os).is_ok() {
                prop_assert!(check_remote_name(&s).is_ok());
            }
        }
    }
}
