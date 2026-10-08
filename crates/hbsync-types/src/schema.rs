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
