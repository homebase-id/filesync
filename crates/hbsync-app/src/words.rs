//! How the client words things for people, shared by the CLI and the tray.

use std::path::Path;

use hbsync_types::NodeKind;
use hbsync_types::activity::{Deleted, Progress, Synced, SyncedKind};

use crate::protocol::{DeletedItem, FolderStatus, HeldAnswer, HeldDeletes, State};

/// Now, in Unix seconds.
pub use hbsync_types::unix_now as now;

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

/// [`when`], but "just now" for the last minute: text that changes once a minute at most.
pub fn when_roughly(at: u64, now: u64) -> String {
    if at <= now && now - at < 60 {
        "just now".into()
    } else {
        when(at, now)
    }
}

/// A size: "512 bytes", "40 kB", "2.1 MB", "1.2 GB" (decimal units, as file managers show).
/// A count with its thousands grouped: `127,000`.
pub fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, d) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(d);
    }
    out
}

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

/// What happened, as a verb phrase: "uploaded", "deleted on another device".
pub(crate) fn verb(kind: SyncedKind) -> &'static str {
    match kind {
        SyncedKind::Uploaded => "uploaded",
        SyncedKind::Downloaded => "downloaded",
        SyncedKind::MovedRemotely => "moved",
        SyncedKind::MovedLocally => "moved on another device",
        SyncedKind::DeletedRemotely => "deleted",
        SyncedKind::TrashedLocally => "deleted on another device",
    }
}

/// What happened to an entry: "uploaded", or for several taken together "312 files
/// downloaded".
pub fn what(s: &Synced) -> String {
    if s.is_group() {
        format!("{} files {}", s.files, verb(s.kind))
    } else {
        verb(s.kind).to_owned()
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
    format!("{place}: {}{size}", what(s))
}

/// An item of Recently deleted, for a list: "photos/a.jpg (2.1 MB), deleted 3 h ago".
pub fn deleted(item: &DeletedItem, now: u64) -> String {
    let d = &item.deleted;
    let size = d
        .size
        .map(|n| format!(" ({})", bytes(n)))
        .unwrap_or_default();
    let folder = if d.kind == NodeKind::Folder { "/" } else { "" };
    let place = item.place();
    let when = match d.removed_at_ms {
        Some(ms) => when(u64::try_from(ms / 1000).unwrap_or(0), now),
        None => "just now".to_owned(),
    };
    format!("{place}{folder}{size}, deleted {when}")
}

/// Why an item of Recently deleted stays when emptied.
pub const STAYS: &str = "stays when emptied: it holds items that aren't deleted";

/// What emptying leaves of Recently deleted, `n` items deleted on their own that hold a live
/// node: a sentence, empty for none.
pub fn staying(n: usize) -> String {
    match n {
        0 => String::new(),
        1 => "One folder stays: it holds items that aren't deleted (restoring it shows what it \
              holds)."
            .to_owned(),
        n => format!(
            "{n} folders stay: they hold items that aren't deleted (restoring one shows what it \
             holds)."
        ),
    }
}

/// Before a deleted item's path in a part of the drive no folder here syncs.
const ON_THE_DRIVE: &str = "(on the drive) ";

/// Where a deleted item was, as people name it (and `hbsync restore` takes it): its path in
/// the main folder (`main`), the folder's path before it in another folder of this computer,
/// `(on the drive) ` before it in a part of the drive no folder here syncs.
pub(crate) fn deleted_place(d: &Deleted, main: Option<&Path>) -> String {
    match d.folder.as_deref() {
        Some(f) if f.is_empty() || main.is_some_and(|m| m == Path::new(f)) => d.path.clone(),
        Some(f) => format!("{}/{}", f.trim_end_matches(['/', '\\']), d.path),
        None => format!("{ON_THE_DRIVE}{}", d.path),
    }
}

/// The name of the item at `place` ([`deleted_place`], a `/` after it or not): the last name
/// of its path.
pub(crate) fn deleted_place_name(place: &str) -> &str {
    let place = place.trim_end_matches('/');
    last_name(place.strip_prefix(ON_THE_DRIVE).unwrap_or(place))
}

/// The last name of a drive path (`/` between names).
pub fn last_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// What the held deletes `held` are and what each answer does, a paragraph for each way:
/// `delete` and `put_back` name the answers as the user gives them (the tray's buttons, the
/// CLI's commands).
pub fn held_deletes(held: &HeldDeletes, delete: &str, put_back: &str) -> Vec<String> {
    let mut out = Vec::new();
    if held.here > 0 {
        let were = if held.here == 1 { "was" } else { "were" };
        out.push(format!(
            "{} in Homebase {were} deleted on this computer (or newly ignored). {delete} \
             removes them from Homebase and your other devices too: Recently deleted keeps \
             them. {put_back} keeps them on Homebase and brings them back here. Until you \
             answer, a new item with one of their names syncs under a numbered name.",
            items(held.here)
        ));
    }
    if held.remotely > 0 {
        out.push(format!(
            "Everything here was removed on another device: {}. {delete} removes them \
             here too. {put_back} keeps them here and restores them on Homebase, for every \
             device.",
            items(held.remotely)
        ));
    }
    out
}

/// "1 item", "1,200 items".
fn items(n: u64) -> String {
    match n {
        1 => "1 item".into(),
        n => format!("{} items", count(n)),
    }
}

/// The reply to an answer to held deletes the runner took: it goes with the next sync, or,
/// `paused`, once syncing resumes.
pub fn held_answered(answer: HeldAnswer, paused: bool) -> &'static str {
    match (answer, paused) {
        (HeldAnswer::Delete, false) => "Confirmed. They are deleted with the next sync.",
        (HeldAnswer::Delete, true) => "Confirmed. They are deleted once you resume syncing.",
        (HeldAnswer::PutBack, false) => "They are put back with the next sync.",
        (HeldAnswer::PutBack, true) => "They are put back once you resume syncing.",
    }
}

/// A sync in progress: "12 of 40 files, 3.1 MB of 40 MB".
pub(crate) fn progress(p: &Progress) -> String {
    let files = format!(
        "{} of {} files",
        count(p.files_done.min(p.files_total)),
        count(p.files_total)
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

/// What a folder's sync is doing, in one line: the tray's status line and the head of `hbsync status`.
pub fn status_line(s: &FolderStatus, now: u64) -> String {
    match s.state {
        State::Starting if s.progress.is_none() => "Catching up…".into(),
        // Start-up's first sync shows its progress like any other: on a new install it can take
        // hours.
        State::Starting | State::Syncing => match &s.progress {
            Some(p) => format!("Syncing {}", progress(p)),
            None => "Syncing…".into(),
        },
        State::Idle if s.pending == 1 => "1 item waiting".into(),
        State::Idle if s.pending > 0 => format!("{} items waiting", count(s.pending)),
        State::Idle => match &s.last_sync {
            Some(last) => format!("Up to date · synced {}", when_roughly(last.at, now)),
            None => "Up to date".into(),
        },
        State::Paused => match s.paused_until {
            Some(until) => format!("Paused · syncing again {}", when(until, now)),
            None => "Paused".into(),
        },
        State::Offline => match s.retry_at {
            Some(at) => format!("Offline · trying again {}", when(at, now)),
            None => "Offline".into(),
        },
        State::Failing => format!(
            "Problem: {}",
            s.error.as_deref().unwrap_or("syncing failed")
        ),
        State::Denied => "Homebase no longer accepts this computer".into(),
        State::Stopped => "Stopped".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deleted item is named by its place: in the main folder by its path, in another
    /// folder of this computer under that folder, elsewhere as on the drive only.
    #[test]
    fn deleted_places() {
        let item = |folder: Option<&str>| Deleted {
            node: hbsync_types::NodeId::new(),
            parent: hbsync_types::NodeId::ROOT,
            path: "Work/cv.txt".into(),
            kind: NodeKind::File,
            size: None,
            removed_at_ms: Some(0),
            root: hbsync_types::NodeId::ROOT,
            folder: folder.map(Into::into),
            stays: false,
        };
        let main = Some(Path::new("/home/a/Homebase"));
        assert_eq!(
            deleted_place(&item(Some("/home/a/Homebase")), main),
            "Work/cv.txt"
        );
        assert_eq!(
            deleted_place(&item(Some("/home/a/Documents")), main),
            "/home/a/Documents/Work/cv.txt"
        );
        assert_eq!(
            deleted_place(&item(None), main),
            "(on the drive) Work/cv.txt"
        );
    }

    /// Held deletes in the words of each client's answers, one paragraph a way, no question
    /// in them (each client asks its own); the reply says when they go, and paused, that they
    /// wait for the resume.
    #[test]
    fn held_deletes_and_answers() {
        let held = HeldDeletes {
            here: 812,
            remotely: 0,
            examples: Vec::new(),
            token: "t".into(),
        };
        let cli = held_deletes(&held, "`hbsync confirm-deletes`", "`hbsync put-back`");
        assert_eq!(cli.len(), 1);
        assert!(cli[0].starts_with("812 items in Homebase were deleted on this computer"));
        assert!(cli[0].contains(" `hbsync confirm-deletes` removes them from Homebase"));
        assert!(cli[0].contains(" `hbsync put-back` keeps them on Homebase"));
        let elsewhere = HeldDeletes {
            here: 0,
            remotely: 1200,
            ..held
        };
        let tray = held_deletes(&elsewhere, "Delete", "Put them back");
        assert_eq!(
            tray,
            [
                "Everything here was removed on another device: 1,200 items. Delete removes them \
              here too. Put them back keeps them here and restores them on Homebase, for every \
              device."
            ]
        );
        assert!(tray.iter().chain(&cli).all(|p| !p.contains('?')));

        assert_eq!(
            held_answered(HeldAnswer::Delete, false),
            "Confirmed. They are deleted with the next sync."
        );
        assert_eq!(
            held_answered(HeldAnswer::PutBack, true),
            "They are put back once you resume syncing."
        );
    }

    /// The CLI and the tray say the same: waiting items, a refusal, a failure.
    #[test]
    fn status_lines() {
        let mut s = FolderStatus::new(
            &"anna.example.com".into(),
            std::path::Path::new("/home/anna/Homebase"),
        );
        s.state = State::Idle;
        s.pending = 1234;
        assert_eq!(status_line(&s, 0), "1,234 items waiting");
        s.state = State::Denied;
        assert_eq!(
            status_line(&s, 0),
            "Homebase no longer accepts this computer"
        );
        s.state = State::Failing;
        assert_eq!(status_line(&s, 0), "Problem: syncing failed");
        s.error = Some("disk full".into());
        assert_eq!(status_line(&s, 0), "Problem: disk full");
    }

    #[test]
    fn counts_are_grouped() {
        let cases = [
            (0, "0"),
            (999, "999"),
            (1_000, "1,000"),
            (127_000, "127,000"),
        ];
        for (n, said) in cases {
            assert_eq!(count(n), said);
        }
        assert_eq!(count(1_234_567), "1,234,567");
    }

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
