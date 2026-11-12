//! The people whose shares this identity's computers sync: the markers on its own Filesync
//! drive (`docs/spec/node-schema-v1.md`, "Sync markers"), written by chat-kmp's "Sync to my
//! computers" (#2307, **[verified]** `FilesyncSharing.kt`).

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use hbsync_remote::Result;
use hbsync_types::schema::FILE_TYPE_MARKER;

use crate::HomebaseError;
use crate::crypto::Key;
use crate::drive::wire::{self, FileHeader, Keys, Order, Shape};

/// Markers read per page: one per person who ever shared with this identity.
const PAGE: usize = 100;

/// A person whose share this identity's computers sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sharer {
    /// Their identity, lower case.
    pub odin_id: String,
    /// The drive their files are on, as the marker names it.
    pub drive_alias: Uuid,
}

/// What the owner's markers say.
#[derive(Debug, PartialEq, Eq)]
pub struct Markers {
    /// The owner, as markers name it ([`owner_of`]).
    owner: String,
    /// The people to sync.
    pub synced: Vec<Sharer>,
    /// The markers of the owner's that don't read, as the [`marker_id`]s they may be (their own
    /// `uniqueId`, and the one of the sharer their content names).
    unreadable: Vec<Uuid>,
    /// One of them can't be tied to any sharer.
    unknown: bool,
}

/// The `uniqueId` of `owner`'s marker for `sharer`, as the node schema's "Sync markers" says:
/// the same on every device.
pub fn marker_id(sharer: &str, owner: &str) -> Uuid {
    let text = format!(
        "filesync-marker\n{}\n{}",
        sharer.to_lowercase(),
        owner.to_lowercase()
    );
    let hash = Sha256::digest(text.as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hash[..16]);
    Uuid::from_bytes(id)
}

/// The identity markers name as their owner: `identity` without a port, lower case (a sender
/// and a marker's id name the domain only).
fn owner_of(identity: &str) -> String {
    identity
        .split(':')
        .next()
        .unwrap_or(identity)
        .to_lowercase()
}

/// The `query-batch` body for a page of the live markers: in the order they were made, which
/// a write doesn't change and the server never holds back (only a change order is held back,
/// homebase-api-notes).
pub(crate) fn markers_query(cursor: Option<&str>) -> Value {
    let params = json!({ "fileType": [FILE_TYPE_MARKER], "fileState": ["active"] });
    wire::query(params, cursor, PAGE, Shape::Full, Order::Created)
}

/// The marker's content; keys this client doesn't know are skipped.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Content {
    odin_id: String,
    /// Needed only to sync: read leniently, so an off marker reads whatever it holds.
    #[serde(default, deserialize_with = "wire::lenient")]
    drive_alias: Option<Uuid>,
    /// Missing or null is off.
    #[serde(default, deserialize_with = "wire::lenient")]
    sync: Option<bool>,
}

/// Who wrote a row, read on its own and strictly: a value that isn't a string must not pass
/// for the owner's.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Sender {
    file_metadata: SenderMeta,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SenderMeta {
    #[serde(default)]
    sender_odin_id: Option<String>,
}

impl Markers {
    /// None read yet, of `identity`'s drive.
    pub(crate) fn of(identity: &str) -> Markers {
        Markers {
            owner: owner_of(identity),
            synced: Vec::new(),
            unreadable: Vec::new(),
            unknown: false,
        }
    }

    /// Whether it is unknown if `sharer` syncs: a marker of the owner's that may be theirs
    /// didn't read. A caller keeps them as they are, never reads them as off.
    pub fn unsure(&self, sharer: &str) -> bool {
        self.unknown || self.unreadable.contains(&marker_id(sharer, &self.owner))
    }

    /// Adds what the live marker `row` says. Only the owner's own marker counts: one written by
    /// anyone else (an editor with Write on the drive can add one) never turns sync on,
    /// whoever it names, and isn't opened. `drive_key` gives the drive's key when the row is
    /// locked with it (true); its error ends the reading (the drive refuses this computer).
    pub(crate) fn read(
        &mut self,
        row: &str,
        shared: &Key,
        drive_key: impl FnOnce(bool) -> Result<Option<Key>>,
    ) -> Result<()> {
        // Read once, then each view of it.
        let row: Value = match serde_json::from_str(row) {
            Ok(row) => row,
            Err(e) => {
                self.doesnt_read(None, None, &e);
                return Ok(());
            }
        };
        // What the query asked for, row by row, as far as the row reads: another kind of
        // row is no marker, readable or not.
        let ids = wire::row_ids(&row);
        let state = ids.file_state.as_deref().unwrap_or("active");
        if state != "active" || ids.file_type.is_some_and(|t| t != FILE_TYPE_MARKER) {
            return Ok(());
        }
        // An empty sender is none, as odin-core reads it (**[verified]** `FileRef`,
        // `IsOriginalSender`: null or empty).
        let sender = Sender::deserialize(&row)
            .map(|s| s.file_metadata.sender_odin_id.filter(|s| !s.is_empty()));
        if let Ok(Some(sender)) = &sender
            && !sender.eq_ignore_ascii_case(&self.owner)
        {
            return Ok(());
        }
        let header = match sender.and_then(|_| FileHeader::deserialize(&row)) {
            Ok(header) => header,
            Err(e) => {
                self.doesnt_read(ids.unique_id, None, &e);
                return Ok(());
            }
        };
        let drive = drive_key(header.locked_with_drive_key())?;
        let keys = Keys {
            shared,
            drive: drive.as_ref(),
        };
        let unique_id = header.file_metadata.app_data.unique_id;
        match sharer_of(&header, keys, &self.owner) {
            Ok(Some(sharer)) => self.synced.push(sharer),
            Ok(None) => {}
            Err((e, named)) => self.doesnt_read(unique_id, named, &e),
        }
        Ok(())
    }

    /// A marker of the owner's that doesn't read, with the ids it may be.
    fn doesnt_read(
        &mut self,
        unique_id: Option<Uuid>,
        named: Option<Uuid>,
        why: &dyn std::fmt::Display,
    ) {
        // Once per read of every marker: the caller says when it changes.
        tracing::debug!("a sync marker doesn't read: {why}");
        let ids = [unique_id, named];
        self.unreadable.extend(ids.into_iter().flatten());
        self.unknown |= ids.iter().all(Option::is_none);
    }
}

/// The sharer the owner's marker `header` says to sync, if it says so; `Err` says why it
/// doesn't read, with the [`marker_id`] of the sharer its content names, if it names one.
fn sharer_of(
    header: &FileHeader,
    keys: Keys<'_>,
    owner: &str,
) -> std::result::Result<Option<Sharer>, (HomebaseError, Option<Uuid>)> {
    let app = &header.file_metadata.app_data;
    let content: Content = header
        .key(keys)
        .and_then(|key| key.open_text(app.content.as_deref().unwrap_or_default()))
        .and_then(|text| {
            serde_json::from_str(&text)
                .map_err(|e| HomebaseError::Protocol(format!("its content: {e}")))
        })
        .map_err(|e| (e, None))?;
    let odin_id = content.odin_id.to_lowercase();
    if !is_domain(&odin_id) {
        let why = format!("it names {odin_id:?}, not an identity");
        return Err((HomebaseError::Protocol(why), None));
    }
    // One marker per sharer: its id is theirs.
    let id = marker_id(&odin_id, owner);
    if app.unique_id != Some(id) {
        let why = format!("its id isn't {odin_id}'s marker's");
        return Err((HomebaseError::Protocol(why), Some(id)));
    }
    if content.sync != Some(true) || odin_id == owner {
        return Ok(None);
    }
    let Some(drive_alias) = content.drive_alias else {
        let why = format!("it syncs {odin_id} without naming their drive");
        return Err((HomebaseError::Protocol(why), Some(id)));
    };
    Ok(Some(Sharer {
        odin_id,
        drive_alias,
    }))
}

/// Whether `name` (lower case) is a domain name: labels of letters, digits and inner hyphens,
/// joined by dots, at least two, the last not all digits (an IP address). It becomes a host, so
/// nothing else passes (no port, no path, no `..`); as a folder's name it is spelled as any
/// name is on its OS (Windows reserves `nul.example`).
fn is_domain(name: &str) -> bool {
    let label = |l: &str| {
        (1..=63).contains(&l.len())
            && l.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !l.starts_with('-')
            && !l.ends_with('-')
    };
    let top = name.rsplit('.').next().unwrap_or_default();
    name.len() <= 253
        && name.split('.').count() >= 2
        && name.split('.').all(label)
        && !top.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drive::sealed::KeyHeader;
    use hbsync_remote::{Refusal, RemoteError};

    const SS: Key = [3; 16];
    const DRIVE: Key = [5; 16];
    const OWNER: &str = "frodo.test";
    const ALIAS: &str = "cfceb937-b868-496b-91cb-3402f18320f8";
    const ON: &str = r#","sync":true"#;

    /// A marker as chat-kmp writes it with `unique_id`, `content` and `sender`, its key under
    /// the shared secret, or the drive's key when `private`.
    fn row(unique_id: Option<Uuid>, content: &str, sender: Value, private: bool) -> String {
        let key = KeyHeader::random().expect("random");
        let sealed = match private {
            true => key.lock(&DRIVE),
            false => key.seal(&SS, &[7; 16]),
        };
        json!({
            "fileId": Uuid::from_u128(1),
            "fileState": "active",
            "sharedSecretEncryptedKeyHeader": sealed,
            "fileMetadata": {
                "isEncrypted": true,
                "versionTag": Uuid::from_u128(2),
                "senderOdinId": sender,
                "appData": {
                    "uniqueId": unique_id,
                    "fileType": FILE_TYPE_MARKER,
                    "dataType": 0,
                    "content": key.seal_text(content),
                },
            },
        })
        .to_string()
    }

    fn content(who: &str, sync: &str) -> String {
        format!(r#"{{"odinId":"{who}","driveAlias":"{ALIAS}"{sync},"folder":"Sam"}}"#)
    }

    /// `who`'s marker saying `sync`, sent by `sender`.
    fn marker(who: &str, sync: &str, sender: Value) -> String {
        let id = marker_id(who, OWNER);
        row(Some(id), &content(&who.to_uppercase(), sync), sender, false)
    }

    fn markers(rows: &[String]) -> Markers {
        let mut markers = Markers::of("Frodo.Test:8443");
        for row in rows {
            markers.read(row, &SS, |_| Ok(Some(DRIVE))).expect("read");
        }
        markers
    }

    fn synced(rows: &[String]) -> Vec<String> {
        let markers = markers(rows);
        assert!(
            markers.unreadable.is_empty() && !markers.unknown,
            "{markers:?}"
        );
        markers.synced.into_iter().map(|s| s.odin_id).collect()
    }

    #[test]
    fn the_marker_id_is_chat_kmps() {
        // sha256("filesync-marker\nsam.test\nfrodo.test")[..16], as chat-kmp's markerId.
        assert_eq!(
            marker_id("Sam.Test", "frodo.test").to_string(),
            "c37e6f34-b2e6-a518-0290-5f3c0d71f2ee"
        );
        assert_ne!(marker_id("sam.test", OWNER), marker_id(OWNER, "sam.test"));
        assert_eq!(owner_of("Frodo.Test:8443"), OWNER);
    }

    #[test]
    fn only_our_own_marker_saying_so_syncs() {
        assert_eq!(
            markers(&[marker("sam.test", ON, Value::Null)]).synced,
            [Sharer {
                odin_id: "sam.test".into(),
                drive_alias: ALIAS.parse().expect("uuid"),
            }]
        );
        // odin-core names the owner as the sender of its own apps' files.
        assert_eq!(
            synced(&[marker("sam.test", ON, json!("Frodo.Test"))]),
            ["sam.test"]
        );
        assert!(synced(&[marker("sam.test", r#","sync":false"#, Value::Null)]).is_empty());
        // An older marker, without the key: off.
        assert!(synced(&[marker("sam.test", "", Value::Null)]).is_empty());
        assert_eq!(synced(&[marker("sam.test", ON, json!(""))]), ["sam.test"]);
        // Only empty is none: a blank sender is someone else's, as odin-core reads it.
        assert!(synced(&[marker("sam.test", ON, json!(" "))]).is_empty());
        // Ours, on a private drive.
        let private = row(
            Some(marker_id("sam.test", OWNER)),
            &content("sam.test", ON),
            Value::Null,
            true,
        );
        assert_eq!(synced(&[private]), ["sam.test"]);
    }

    #[test]
    fn a_marker_someone_else_left_never_syncs_nor_is_opened() {
        for sender in ["sam.test", "merry.test"] {
            let mut markers = Markers::of(OWNER);
            let refuse = |_| Err(RemoteError::Denied("no key".into(), Refusal::NoKey));
            // Theirs, and one whose header doesn't read either.
            for row in [
                marker("sam.test", ON, json!(sender)),
                json!({"fileMetadata": {"senderOdinId": sender}}).to_string(),
            ] {
                markers.read(&row, &SS, refuse).expect("not opened");
            }
            assert_eq!(markers, Markers::of(OWNER));
        }
    }

    #[test]
    fn a_row_that_is_no_live_marker_is_left_out() {
        let mut gone: Value =
            serde_json::from_str(&marker("sam.test", ON, Value::Null)).expect("json");
        gone["fileState"] = json!("deleted");
        let mut node = gone.clone();
        node["fileState"] = json!("active");
        node["fileMetadata"]["appData"]["fileType"] = json!(7102);
        // Nor one that doesn't read.
        let mut odd = node.clone();
        odd["fileMetadata"]["senderOdinId"] = json!(7);
        let mut tombstone = gone.clone();
        tombstone["fileMetadata"] = json!("junk");
        assert!(synced(&[gone, node, odd, tombstone].map(|r| r.to_string())).is_empty());
    }

    #[test]
    fn only_a_marker_that_syncs_needs_the_drive() {
        let sam = Some(marker_id("sam.test", OWNER));
        let off = row(
            sam,
            r#"{"odinId":"sam.test","sync":false}"#,
            Value::Null,
            false,
        );
        assert!(synced(&[off]).is_empty());
        let on = row(
            sam,
            r#"{"odinId":"sam.test","sync":true}"#,
            Value::Null,
            false,
        );
        assert!(markers(&[on]).unsure("sam.test"));
    }

    #[test]
    fn a_marker_naming_the_owner_does_not_sync() {
        assert!(synced(&[marker(OWNER, ON, Value::Null)]).is_empty());
    }

    #[test]
    fn a_marker_that_does_not_read_is_unreadable_not_off() {
        let bad = [
            "sam",
            "../sam.test",
            "sam.test:443",
            "sam test.x",
            "-sam.test",
            "10.0.0.1",
        ];
        for who in bad {
            let markers = markers(&[marker(who, ON, Value::Null)]);
            assert_eq!(markers.unreadable, [marker_id(who, OWNER)], "{who}");
        }
        // Another sharer's id: both are unknown.
        let (sam, merry) = (marker_id("sam.test", OWNER), marker_id("merry.test", OWNER));
        let theirs = row(Some(merry), &content("sam.test", ON), Value::Null, false);
        assert_eq!(markers(&[theirs]).unreadable, [merry, sam]);
        // No content to read.
        let empty = row(Some(sam), r#"{"sync":true}"#, Value::Null, false);
        assert_eq!(markers(std::slice::from_ref(&empty)).unreadable, [sam]);
        // A sender that isn't a string never passes for the owner.
        assert_eq!(
            markers(&[marker("sam.test", ON, json!(7))]).unreadable,
            [sam]
        );
        // Nothing ties it to a sharer.
        let unknown = markers(&[row(None, "junk", Value::Null, false)]);
        assert!(unknown.unknown && unknown.unreadable.is_empty());
        assert!(unknown.unsure("anyone.test"));
        assert!(markers(&["{}".into()]).unknown);
        // A caller asks by sharer, the owner's port left out of the ids.
        let one = markers(&[empty]);
        assert!(one.unsure("Sam.Test") && !one.unsure("merry.test"));
        // The others still read.
        assert_eq!(synced(&[marker("sam.test", ON, Value::Null)]), ["sam.test"]);
    }

    #[test]
    fn a_drive_that_refuses_this_computer_ends_the_reading() {
        let row = marker("sam.test", ON, Value::Null);
        let refuse = |_| Err(RemoteError::Denied("no key".into(), Refusal::NoKey));
        let got = Markers::of(OWNER).read(&row, &SS, refuse);
        assert!(matches!(got, Err(RemoteError::Denied(..))), "{got:?}");
    }

    #[test]
    fn the_query_lists_live_markers_in_the_order_made() {
        let query = markers_query(Some("c"));
        assert_eq!(query["queryParams"]["fileType"], json!([FILE_TYPE_MARKER]));
        assert_eq!(query["queryParams"]["fileState"], json!(["active"]));
        assert_eq!(query["resultOptionsRequest"]["sorting"], "createdDate");
        assert_eq!(query["resultOptionsRequest"]["cursorState"], "c");
    }
}
