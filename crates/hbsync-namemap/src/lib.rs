//! Local name mapping: how a remote (portable) name is spelled on this OS, and back.
//!
//! The remote name never changes because of one OS (foundations §2). When a name is illegal
//! locally it is escaped:
//!
//! - **Windows** uses the SFM mapping that the macOS SMB client and Samba's `catia` module also
//!   use: control characters `0x01..=0x1F` → `U+F001..=U+F01F`, `" * : < > ? \ |` →
//!   `U+F020..=U+F027`, a trailing space → `U+F028`, a trailing dot → `U+F029`. Device names
//!   (`CON`, `com1.txt`) get the marker `U+F02A` at the end of the stem.
//! - **macOS / Linux** accept every remote name as-is.
//!
//! Local → remote reverses the escapes (Windows only) and normalizes to NFC.
//!
//! The lexical mapping round-trips for every remote name except those that already contain the
//! escape code points. For those, [`LocalName::recorded`] is set and the engine stores the
//! local spelling in the state DB instead of relying on [`to_remote`].

use hbsync_platform::name::{
    NameIssue, check_local_name, check_remote_name, is_windows_reserved, name_key,
};
use hbsync_platform::{Os, nfc};

/// Marker inserted after the stem of a Windows device name.
const RESERVED_MARKER: char = '\u{F02A}';
const TRAILING_SPACE: char = '\u{F028}';
const TRAILING_DOT: char = '\u{F029}';
const SFM_PUNCT: [(char, char); 8] = [
    ('"', '\u{F020}'),
    ('*', '\u{F021}'),
    (':', '\u{F022}'),
    ('<', '\u{F023}'),
    ('>', '\u{F024}'),
    ('?', '\u{F025}'),
    ('\\', '\u{F026}'),
    ('|', '\u{F027}'),
];

/// A remote name as spelled on the local file system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalName {
    pub name: String,
    /// `true` if [`to_remote`] would not give the remote name back, so the engine must record
    /// the mapping.
    pub recorded: bool,
}

/// Spells a valid remote name for the local file system of `os`.
///
/// Fails for the few names that can't be spelled at all: on Windows, a device name whose
/// marker would take it past 255 UTF-16 units (`"CON."` followed by 251 more characters).
pub fn to_local(remote: &str, os: Os) -> Result<LocalName, NameIssue> {
    check_remote_name(remote)?;
    let name = match os {
        Os::Windows => escape_windows(remote),
        Os::MacOs | Os::Linux => remote.to_owned(),
    };
    check_local_name(&name, os)?;
    let recorded = to_remote(&name, os).as_deref() != Ok(remote);
    Ok(LocalName { name, recorded })
}

/// Derives the remote name from a local one, when no mapping is recorded.
pub fn to_remote(local: &str, os: Os) -> Result<String, NameIssue> {
    let unescaped = match os {
        Os::Windows => unescape_windows(local),
        Os::MacOs | Os::Linux => local.to_owned(),
    };
    let remote = nfc(&unescaped);
    check_remote_name(&remote)?;
    Ok(remote)
}

/// A small hash that names which share a place in a folder have in common, on any OS: the
/// same name under [`name_key`], and a local spelling and the remote name it stands for
/// (whichever OS spelled it). The state DB stores it with each row of every side, so a folder's
/// rows that may clash with a name are found without listing the folder. Different names can
/// share one (a lookup by it finds a few more rows, never fewer).
///
/// Stored: never change what it gives for a name. It rests on [`name_key`]'s Unicode tables
/// (case folding, normalization): an update of them that changes a key needs a migration that
/// computes the stored hashes again (`sibling_hashes_are_pinned` fails then).
pub fn sibling_hash(name: &str) -> i32 {
    // The key with the Windows escapes taken back; its low 32 bits fit an SQLite row in 4 bytes.
    let key = name_key(&unescape_windows(name));
    hbsync_platform::fnv::fnv1a64(key.as_bytes()) as i32
}

fn escape_windows(remote: &str) -> String {
    let mut out: String = remote
        .chars()
        .map(|c| match c {
            '\u{1}'..='\u{1F}' => sfm_control(c),
            _ => SFM_PUNCT
                .iter()
                .find(|(from, _)| *from == c)
                .map_or(c, |(_, to)| *to),
        })
        .collect();
    match out.pop() {
        Some(' ') => out.push(TRAILING_SPACE),
        Some('.') => out.push(TRAILING_DOT),
        Some(c) => out.push(c),
        None => {}
    }
    if is_windows_reserved(&out) {
        let stem_end = out.find('.').unwrap_or(out.len());
        // Trailing spaces in the stem are part of what Windows ignores; mark after them.
        out.insert(stem_end, RESERVED_MARKER);
    }
    out
}

fn sfm_control(c: char) -> char {
    char::from_u32(0xF000 + c as u32).unwrap_or(c)
}

fn unescape_windows(local: &str) -> String {
    local
        .chars()
        .filter(|&c| c != RESERVED_MARKER)
        .map(|c| match c {
            '\u{F001}'..='\u{F01F}' => char::from_u32(c as u32 - 0xF000).unwrap_or(c),
            TRAILING_SPACE => ' ',
            TRAILING_DOT => '.',
            _ => SFM_PUNCT
                .iter()
                .find(|(_, to)| *to == c)
                .map_or(c, |(from, _)| *from),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn local(remote: &str, os: Os) -> LocalName {
        to_local(remote, os).expect("valid remote name")
    }

    #[test]
    fn plain_names_are_unchanged_everywhere() {
        for os in [Os::Windows, Os::MacOs, Os::Linux] {
            let l = local("Report Q3.pdf", os);
            assert_eq!(
                l,
                LocalName {
                    name: "Report Q3.pdf".into(),
                    recorded: false
                }
            );
        }
    }

    #[test]
    fn windows_escapes() {
        assert_eq!(
            local("a:b?.txt", Os::Windows).name,
            "a\u{F022}b\u{F025}.txt"
        );
        assert_eq!(local("trailing.", Os::Windows).name, "trailing\u{F029}");
        assert_eq!(local("space ", Os::Windows).name, "space\u{F028}");
        assert_eq!(local("tab\there", Os::Windows).name, "tab\u{F009}here");
        assert_eq!(local("CON", Os::Windows).name, "CON\u{F02A}");
        assert_eq!(
            local("com1.tar.gz", Os::Windows).name,
            "com1\u{F02A}.tar.gz"
        );
        // Escaping the trailing dot already makes the stem non-reserved.
        assert_eq!(local("NUL.", Os::Windows).name, "NUL\u{F029}");
        for name in ["a:b?.txt", "trailing.", "CON", "com1.tar.gz", "x|y*z\"<>\\"] {
            assert!(!local(name, Os::Windows).recorded, "{name}");
            assert_eq!(
                to_remote(&local(name, Os::Windows).name, Os::Windows).as_deref(),
                Ok(name)
            );
        }
    }

    #[test]
    fn long_names_on_windows() {
        // Every character escaped: three times the UTF-8 bytes, the same UTF-16 units.
        let all_escaped = "?".repeat(255);
        let l = local(&all_escaped, Os::Windows);
        assert_eq!(l.name.encode_utf16().count(), 255);
        assert_eq!(
            to_remote(&l.name, Os::Windows).as_deref(),
            Ok(all_escaped.as_str())
        );
        // The device-name marker is one unit more than the name can take.
        let device = format!("CON.{}", "x".repeat(251));
        assert_eq!(to_local(&device, Os::Windows), Err(NameIssue::TooLong));
        assert!(to_local(&device, Os::Linux).is_ok());
    }

    #[test]
    fn unix_keeps_windows_illegal_names() {
        assert_eq!(local("a:b?", Os::Linux).name, "a:b?");
        assert_eq!(local("CON", Os::MacOs).name, "CON");
    }

    #[test]
    fn names_with_escape_code_points_are_recorded() {
        let l = local("pua\u{F022}", Os::Windows);
        assert!(l.recorded);
        // On Unix the code point is just a character.
        assert!(!local("pua\u{F022}", Os::Linux).recorded);
    }

    #[test]
    fn local_to_remote_normalizes() {
        assert_eq!(
            to_remote("e\u{301}t\u{e9}", Os::MacOs).as_deref(),
            Ok("\u{e9}t\u{e9}")
        );
        assert_eq!(to_remote("..", Os::Linux), Err(NameIssue::DotOrDotDot));
    }

    /// [`sibling_hash`] is stored: pinned.
    #[test]
    fn sibling_hashes_are_pinned() {
        assert_eq!(sibling_hash("Report.pdf"), sibling_hash("report.PDF"));
        assert_eq!(sibling_hash("a:b"), sibling_hash("A\u{F022}B"));
        assert_eq!(sibling_hash("CON\u{F02A}.txt"), sibling_hash("con.txt"));
        assert_eq!(sibling_hash("Report.pdf"), 757_669_927);
        assert_eq!(sibling_hash(""), -2_078_137_563);
        // Case pairs and compositions across scripts, the newer ones included (Georgian
        // Mtavruli, Unicode 11; Cherokee small letters, 8): an update of the tables that
        // changes any of these keys changes this hash.
        let scripts = "ÀÉÎÕÜßẞ ΑΒΓΣςσ АБВЁЙ ᲐᲑᲒ ᏣᏤᏥ ꭰꭱ ǅǈǋ ﬁﬀ Å Ω K e\u{301} ᄀ\u{1161}";
        assert_eq!(
            name_key(scripts),
            "àéîõüssss αβγσσσ абвёй აბგ ᏣᏤᏥ ᎠᎡ ǆǉǌ fiff å ω k é 가"
        );
        assert_eq!(sibling_hash(scripts), 738_829_446);
    }

    fn any_os() -> impl Strategy<Value = Os> {
        prop_oneof![Just(Os::Windows), Just(Os::MacOs), Just(Os::Linux)]
    }

    proptest! {
        #[test]
        fn escaped_names_are_legal_and_round_trip(
            raw in "[a-zA-Z0-9 .:*?\"<>|\\\\\\t\\u{1}-\\u{1f}éß😀\\u{F020}-\\u{F02A}-]{1,300}",
            os in any_os(),
        ) {
            let remote = nfc(&raw);
            prop_assume!(check_remote_name(&remote).is_ok());
            let l = match to_local(&remote, os) {
                Ok(l) => l,
                Err(e) => {
                    // Only a device name at the length limit can't be spelled.
                    prop_assert_eq!(os, Os::Windows);
                    prop_assert!(is_windows_reserved(&remote) && remote.len() >= 255, "{:?}", e);
                    return Ok(());
                }
            };
            prop_assert!(check_local_name(&l.name, os).is_ok(), "{:?} → {:?}", remote, l.name);
            if !l.recorded {
                prop_assert_eq!(to_remote(&l.name, os)?, remote);
            }
        }

        #[test]
        fn only_escape_code_points_force_recording(
            raw in "[a-zA-Z0-9 .:*?\"<>|\\\\\\t]{1,30}",
            os in any_os(),
        ) {
            prop_assume!(check_remote_name(&raw).is_ok());
            prop_assert!(!to_local(&raw, os)?.recorded);
        }

        /// A name, its local spelling on any OS, the remote name a local one stands for, and
        /// every name with the same key share a sibling hash.
        #[test]
        fn spellings_share_a_sibling_hash(
            raw in "[a-zA-Z0-9 .:*?\"<>|\\\\\\t\\u{1}-\\u{1f}éÉßẞ\u{301}\u{338}😀\\u{F020}-\\u{F02A}-]{1,40}",
            os in any_os(),
        ) {
            let hash = sibling_hash(&raw);
            prop_assert_eq!(sibling_hash(&name_key(&raw)), hash);
            prop_assert_eq!(sibling_hash(&raw.to_uppercase()), sibling_hash(&raw.to_lowercase()));
            if let Ok(local) = to_local(&raw, os) {
                prop_assert_eq!(sibling_hash(&local.name), hash);
            }
            if let Ok(remote) = to_remote(&raw, os) {
                prop_assert_eq!(sibling_hash(&remote), hash);
            }
        }
    }
}
