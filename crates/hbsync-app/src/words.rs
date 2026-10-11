//! How the client words things for people, shared by the CLI and the tray.

use std::time::{SystemTime, UNIX_EPOCH};

use hbsync_types::activity::{Synced, SyncedKind};

use crate::runner::Progress;

/// Now, in Unix seconds.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// `at` (Unix seconds) relative to `now`: "12 s ago", "in 3 min".
pub fn when(at: u64, now: u64) -> String {
    let (secs, ago) = if at <= now {
        (now - at, true)
    } else {
        (at - now, false)
    };
    let span = match secs {
        0..60 => format!("{secs} s"),
        60..3600 => format!("{} min", secs / 60),
        3600..86_400 => format!("{} h", secs / 3600),
        _ => format!("{} days", secs / 86_400),
    };
    if ago {
        format!("{span} ago")
    } else {
        format!("in {span}")
    }
}

/// A size: "512 bytes", "40 kB", "2.1 MB", "1.2 GB" (decimal units, as file managers show).
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["kB", "MB", "GB", "TB"];
    if n < 1000 {
        return format!("{n} bytes");
    }
    let mut value = n as f64 / 1000.0;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if value < 10.0 {
        format!("{value:.1} {}", UNITS[unit])
    } else {
        format!("{value:.0} {}", UNITS[unit])
    }
}

/// What happened, as a verb phrase: "uploaded", "moved to the trash (deleted elsewhere)".
pub fn verb(kind: SyncedKind) -> &'static str {
    match kind {
        SyncedKind::Uploaded => "uploaded",
        SyncedKind::Downloaded => "downloaded",
        SyncedKind::MovedRemotely => "moved",
        SyncedKind::MovedLocally => "moved on another device",
        SyncedKind::DeletedRemotely => "deleted",
        SyncedKind::TrashedLocally => "deleted on another device (in the trash here)",
    }
}

/// One entry of the recently synced list: "Docs/report.txt: uploaded, 2.1 MB", or for
/// several taken together "Photos/2024: 312 files downloaded, 1.2 GB".
pub fn synced(s: &Synced) -> String {
    let place = if s.path.is_empty() {
        "the top folder"
    } else {
        s.path.as_str()
    };
    let size = if s.bytes > 0 {
        format!(", {}", bytes(s.bytes))
    } else {
        String::new()
    };
    if s.is_group() {
        format!("{place}: {} files {}{size}", s.files, verb(s.kind))
    } else {
        format!("{place}: {}{size}", verb(s.kind))
    }
}

/// A sync in progress: "12 of 40 files, 3.1 MB of 40 MB".
pub fn progress(p: &Progress) -> String {
    let files = format!(
        "{} of {} files",
        p.files_done.min(p.files_total),
        p.files_total
    );
    if p.bytes_total == 0 {
        return files;
    }
    format!(
        "{files}, {} of {}",
        bytes(p.bytes_done.min(p.bytes_total)),
        bytes(p.bytes_total)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(bytes(512), "512 bytes");
        assert_eq!(bytes(40_000), "40 kB");
        assert_eq!(bytes(2_100_000), "2.1 MB");
        assert_eq!(bytes(1_234_000_000), "1.2 GB");
    }

    #[test]
    fn times() {
        assert_eq!(when(100, 112), "12 s ago");
        assert_eq!(when(400, 220), "in 3 min");
        assert_eq!(when(0, 7200), "2 h ago");
    }

    #[test]
    fn entries_and_progress() {
        let one = Synced {
            at: 0,
            kind: SyncedKind::Uploaded,
            path: "Docs/report.txt".into(),
            files: 1,
            bytes: 2_100_000,
        };
        assert_eq!(synced(&one), "Docs/report.txt: uploaded, 2.1 MB");
        let group = Synced {
            kind: SyncedKind::Downloaded,
            path: "Photos/2024".into(),
            files: 312,
            bytes: 1_200_000_000,
            ..one
        };
        assert_eq!(synced(&group), "Photos/2024: 312 files downloaded, 1.2 GB");
        let p = Progress {
            files_done: 12,
            files_total: 40,
            bytes_done: 3_100_000,
            bytes_total: 40_000_000,
        };
        assert_eq!(progress(&p), "12 of 40 files, 3.1 MB of 40 MB");
    }
}
