//! Node schema v1: the `appData.content` JSON and the constants of a sync drive.
//! Spec: `docs/spec/node-schema-v1.md`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::NodeId;

/// `appData.fileType` of a folder node (provisional, see the spec).
pub const FILE_TYPE_FOLDER: i32 = 7101;
/// `appData.fileType` of a file node (provisional, see the spec).
pub const FILE_TYPE_FILE: i32 = 7102;
/// The one payload key of a file node.
pub const PAYLOAD_KEY: &str = "sync_data";
/// The current schema version.
pub const VERSION: u32 = 1;
/// The content key of the creation time ([`NodeContent::created_ms`]).
const CREATED: &str = "ct";
/// The content key of a removal ([`NodeContent::removal`]).
pub const REMOVAL_KEY: &str = "rm";
/// `appData.archivalStatus` of a node in Recently deleted (0 is a live one).
pub const ARCHIVAL_REMOVED: i32 = 2;

/// When a node went to Recently deleted, and the topmost node deleted with it (the node
/// itself, or the folder whose delete took it along): restoring that one restores this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Removal {
    /// Unix ms.
    pub at_ms: i64,
    pub root: NodeId,
}

/// The `appData.content` JSON of a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeContent {
    pub v: u32,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub x: bool,
    /// Keys from newer writers, kept so a rewrite doesn't drop them.
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

impl NodeContent {
    pub fn folder(name: impl Into<String>) -> Self {
        NodeContent {
            v: VERSION,
            name: name.into(),
            mtime: None,
            size: None,
            x: false,
            unknown: Map::new(),
        }
    }

    pub fn file(name: impl Into<String>, mtime_ms: i64, size: u64, executable: bool) -> Self {
        NodeContent {
            v: VERSION,
            name: name.into(),
            mtime: Some(mtime_ms),
            size: Some(size),
            x: executable,
            unknown: Map::new(),
        }
    }

    pub fn to_json(&self) -> String {
        // A struct of strings, numbers and a JSON map can't fail to serialize.
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// The creation time `ct`, Unix ms: the item's own when the node was created, then never
    /// changed (an editor that saves by replacing the file would otherwise reset it each time).
    /// It lives with the unknown keys, which every rewrite keeps; a value that isn't a number
    /// (from some other writer) reads as none and is kept as it is.
    pub fn created_ms(&self) -> Option<i64> {
        Self::created_ms_in(&self.unknown)
    }

    /// [`NodeContent::created_ms`] of the unknown keys alone (as a client stores them).
    pub fn created_ms_in(unknown: &Map<String, Value>) -> Option<i64> {
        let ct = unknown.get(CREATED)?;
        // An integer, or a JavaScript-style fractional ms count.
        ct.as_i64().or_else(|| {
            ct.as_f64()
                .filter(|f| f.is_finite())
                .map(|f| f.round() as i64)
        })
    }

    pub fn set_created_ms(&mut self, created_ms: Option<i64>) {
        match created_ms {
            Some(ms) => self.unknown.insert(CREATED.into(), ms.into()),
            None => self.unknown.remove(CREATED),
        };
    }

    /// The removal `rm` of a node in Recently deleted (`archivalStatus` 2), kept with the
    /// unknown keys like `ct`; one that doesn't read (another writer's) is none.
    pub fn removal(&self) -> Option<Removal> {
        let rm = self.unknown.get(REMOVAL_KEY)?;
        let at = &rm["at"];
        Some(Removal {
            at_ms: at.as_i64().or_else(|| {
                at.as_f64()
                    .filter(|f| f.is_finite())
                    .map(|f| f.round() as i64)
            })?,
            root: NodeId(rm["root"].as_str()?.parse().ok()?),
        })
    }

    pub fn set_removal(&mut self, removal: Option<Removal>) {
        match removal {
            Some(r) => self.unknown.insert(
                REMOVAL_KEY.into(),
                json!({"at": r.at_ms, "root": r.root.0.to_string()}),
            ),
            None => self.unknown.remove(REMOVAL_KEY),
        };
    }

    /// A reader that finds a newer version treats the node as read-only.
    pub fn is_writable(&self) -> bool {
        self.v <= VERSION
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_content_round_trip() -> Result<(), serde_json::Error> {
        let c = NodeContent::file("Résumé.txt", 1_759_920_000_123, 1025, false);
        let json = c.to_json();
        assert_eq!(
            json,
            r#"{"v":1,"name":"Résumé.txt","mtime":1759920000123,"size":1025}"#
        );
        assert_eq!(NodeContent::from_json(&json)?, c);
        Ok(())
    }

    #[test]
    fn folder_and_executable() {
        assert_eq!(
            NodeContent::folder("Projects").to_json(),
            r#"{"v":1,"name":"Projects"}"#
        );
        assert!(
            NodeContent::file("run", 0, 0, true)
                .to_json()
                .contains(r#""x":true"#)
        );
    }

    #[test]
    fn creation_time_reads_leniently_and_rides_with_the_unknown_keys()
    -> Result<(), serde_json::Error> {
        let mut c = NodeContent::file("a", 5, 1, false);
        c.set_created_ms(Some(1_451_606_400_123));
        assert_eq!(
            c.to_json(),
            r#"{"v":1,"name":"a","mtime":5,"size":1,"ct":1451606400123}"#
        );
        // Another writer's fractional ms, and one that isn't a number: the node still reads.
        let float = NodeContent::from_json(r#"{"v":1,"name":"a","ct":1451606400123.6}"#)?;
        assert_eq!(float.created_ms(), Some(1_451_606_400_124));
        let odd = NodeContent::from_json(r#"{"v":1,"name":"a","ct":"yesterday"}"#)?;
        assert_eq!(odd.created_ms(), None);
        assert!(odd.to_json().contains(r#""ct":"yesterday""#));
        Ok(())
    }

    #[test]
    fn a_removal_rides_with_the_unknown_keys() -> Result<(), serde_json::Error> {
        let mut c = NodeContent::file("a", 5, 1, false);
        let root = NodeId::ROOT;
        c.set_removal(Some(Removal { at_ms: 7, root }));
        let json = c.to_json();
        assert_eq!(
            json,
            r#"{"v":1,"name":"a","mtime":5,"size":1,"rm":{"at":7,"root":"2a1c0d55-8ad3-417e-a5d7-42c3d0baba0e"}}"#
        );
        assert_eq!(
            NodeContent::from_json(&json)?.removal(),
            Some(Removal { at_ms: 7, root })
        );
        c.set_removal(None);
        assert_eq!(c.removal(), None);
        // Another writer's odd one: the node still reads, with no removal.
        let odd = NodeContent::from_json(r#"{"v":1,"name":"a","rm":{"at":"x"}}"#)?;
        assert_eq!(odd.removal(), None);
        Ok(())
    }

    #[test]
    fn unknown_keys_survive() -> Result<(), serde_json::Error> {
        let json = r#"{"v":2,"name":"a","future":{"k":1}}"#;
        let c = NodeContent::from_json(json)?;
        assert!(!c.is_writable());
        assert!(c.to_json().contains(r#""future":{"k":1}"#));
        Ok(())
    }

    #[test]
    fn fixtures_parse() -> Result<(), Box<dyn std::error::Error>> {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/spec/fixtures");
        for (file, name) in [
            ("folder-node.json", "Projects"),
            ("file-node.json", "Résumé.txt"),
        ] {
            let header: Value =
                serde_json::from_str(&std::fs::read_to_string(format!("{dir}/{file}"))?)?;
            let content = header["fileMetadata"]["appData"]["content"]
                .as_str()
                .ok_or("content")?;
            assert_eq!(NodeContent::from_json(content)?.name, name);
        }
        Ok(())
    }
}
