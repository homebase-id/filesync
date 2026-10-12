//! Node schema v1: the `appData.content` JSON and the constants of a sync drive.
//! Spec: `docs/spec/node-schema-v1.md`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// `appData.fileType` of a folder node (provisional, see the spec).
pub const FILE_TYPE_FOLDER: i32 = 7101;
/// `appData.fileType` of a file node (provisional, see the spec).
pub const FILE_TYPE_FILE: i32 = 7102;
/// The one payload key of a file node.
pub const PAYLOAD_KEY: &str = "sync_data";
/// The current schema version.
pub const VERSION: u32 = 1;

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
    /// Creation time, Unix ms: the item's own when it was first uploaded, then never changed
    /// (an editor that saves by replacing the file would otherwise reset it every time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ct: Option<i64>,
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
            ct: None,
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
            ct: None,
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

    /// The keys sync carries along without acting on them: `ct` and the unknown ones. Every
    /// rewrite of the node puts them back ([`NodeContent::carry`]).
    pub fn carried(&self) -> Map<String, Value> {
        let mut keys = self.unknown.clone();
        if let Some(ct) = self.ct {
            keys.insert("ct".into(), ct.into());
        }
        keys
    }

    /// Puts back what [`NodeContent::carried`] gave.
    pub fn carry(&mut self, mut keys: Map<String, Value>) {
        self.ct = keys.remove("ct").and_then(|ct| ct.as_i64());
        self.unknown = keys;
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
    fn creation_time_is_carried_with_the_unknown_keys() -> Result<(), serde_json::Error> {
        let json = r#"{"v":1,"name":"a","mtime":5,"size":1,"ct":1451606400123,"future":7}"#;
        let read = NodeContent::from_json(json)?;
        assert_eq!(read.ct, Some(1_451_606_400_123));
        // A rewrite (new bytes, a rename) starts from scratch and gets them back.
        let mut rewritten = NodeContent::file("b", 6, 2, false);
        rewritten.carry(read.carried());
        assert_eq!(
            rewritten.to_json(),
            r#"{"v":1,"name":"b","mtime":6,"size":2,"ct":1451606400123,"future":7}"#
        );
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
