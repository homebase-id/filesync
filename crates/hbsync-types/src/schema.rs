//! Node schema v1: the `appData.content` JSON and the constants of a sync drive.
//! Spec: `docs/spec/node-schema-v1.md`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{NodeKind, Tag, Tags};

/// `appData.fileType` of a folder node (provisional, see the spec).
pub const FILE_TYPE_FOLDER: i32 = 7101;
/// `appData.fileType` of a file node (provisional, see the spec).
pub const FILE_TYPE_FILE: i32 = 7102;
/// `appData.fileType` of the drive's marker, not a node (the spec, "The drive's marker").
pub const FILE_TYPE_MARKER: i32 = 7103;
/// The `uniqueId` of the drive's marker: the drive holds one at most.
pub const MARKER_ID: uuid::Uuid = uuid::uuid!("34f57652-5082-4f24-9246-e3ca8b991373");
/// The one payload key of a file node.
pub const PAYLOAD_KEY: &str = "sync_data";
/// The current schema version.
pub(crate) const VERSION: u32 = 1;
/// The content key of the creation time ([`NodeContent::created_ms`]).
const CREATED: &str = "ct";
/// The content key of the Finder tags ([`NodeContent::tags`]).
const TAGS_KEY: &str = "tags";
/// The most bytes of another writer's `tags` kept as written: about what this client syncs of
/// tags per item (`hbsync_platform::tags::Tags::fits`).
const RAW_TAGS_MAX: usize = 2000;
/// The content key of an older writer's removal marker: read by no one, dropped when the
/// content is read (node schema, Recently deleted).
const REMOVAL_KEY: &str = "rm";
/// `appData.archivalStatus` of a node in Recently deleted (0 is a live one).
pub const ARCHIVAL_REMOVED: i32 = 2;

/// The `appData.content` JSON of a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeContent {
    pub v: u32,
    pub name: String,
    /// Read leniently: a JavaScript or Kotlin writer's fractional ms count is rounded.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "lenient_ms"
    )]
    pub mtime: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Read leniently: `1` and `0` are `true` and `false`.
    #[serde(
        default,
        skip_serializing_if = "std::ops::Not::not",
        deserialize_with = "lenient_bool"
    )]
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

    /// An older writer's removal marker `rm` is dropped: the header says what is removed and
    /// when, and the next write goes without it.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let mut content: Self = serde_json::from_str(json)?;
        content.unknown.remove(REMOVAL_KEY);
        Ok(content)
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
        ms_of(unknown.get(CREATED)?)
    }

    pub fn set_created_ms(&mut self, created_ms: Option<i64>) {
        match created_ms {
            Some(ms) => self.unknown.insert(CREATED.into(), ms.into()),
            None => self.unknown.remove(CREATED),
        };
    }

    /// The Finder tags `tags`, kept with the unknown keys like `ct`. A malformed entry (from
    /// some other writer) is left out; none means no tags.
    pub fn tags(&self) -> Tags {
        self.unknown
            .get(TAGS_KEY)
            .map(tags_from_value)
            .unwrap_or_default()
    }

    /// Sets the tags; none removes the key.
    pub fn set_tags(&mut self, tags: &Tags) {
        if tags.is_empty() {
            self.unknown.remove(TAGS_KEY);
        } else {
            self.unknown.insert(TAGS_KEY.into(), tags_value(tags));
        }
    }

    /// [`NodeContent::set_tags`] on content whose tags read as `had`: the same set leaves
    /// `tags` as written (another writer's form, its entries' other keys, an entry this client
    /// skips), unless that is past what tags may take ([`RAW_TAGS_MAX`]). It changes only with
    /// the set (node schema, `tags`).
    pub fn set_tags_over(&mut self, tags: &Tags, had: &Tags) {
        let raw_fits = self
            .unknown
            .get(TAGS_KEY)
            .is_none_or(|raw| raw.to_string().len() <= RAW_TAGS_MAX);
        if tags != had || !raw_fits {
            self.set_tags(tags);
        }
    }

    /// The keys sync carries without acting on them, as a JSON object (read back by
    /// [`keys_from_str`]): the unknown ones, the creation time and the tags; and a folder's
    /// `mtime`, `size` and `x`, which only files use (another writer's: written back as read).
    pub fn carried_json(&self, kind: NodeKind) -> Option<String> {
        // `x` as it serializes: only when true. A map of JSON values can't fail to serialize.
        let file_keys = [
            ("mtime", self.mtime.map(Value::from)),
            ("size", self.size.map(Value::from)),
            ("x", self.x.then_some(Value::Bool(true))),
        ];
        if kind == NodeKind::File || file_keys.iter().all(|(_, v)| v.is_none()) {
            return (!self.unknown.is_empty())
                .then(|| serde_json::to_string(&self.unknown).unwrap_or_default());
        }
        let mut keys = self.unknown.clone();
        keys.extend(
            file_keys
                .into_iter()
                .filter_map(|(k, v)| Some((k.to_owned(), v?))),
        );
        Some(Value::Object(keys).to_string())
    }

    /// A reader that finds a newer version treats the node as read-only.
    pub fn is_writable(&self) -> bool {
        self.v <= VERSION
    }

    /// The version `json` says it is, where that is newer than this one: for content that
    /// doesn't read as this version (a newer one may change a field's type).
    pub fn newer_version(json: &str) -> Option<u64> {
        let v = serde_json::from_str::<Value>(json)
            .ok()?
            .get("v")?
            .as_u64()?;
        (v > u64::from(VERSION)).then_some(v)
    }
}

/// A Unix ms count: an integer, or a JavaScript-style fractional one rounded.
fn ms_of(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| {
        v.as_f64()
            .filter(|f| f.is_finite())
            .map(|f| f.round() as i64)
    })
}

/// [`ms_of`] for a known key: `null` is none, anything but a number isn't one.
fn lenient_ms<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    Option::<Value>::deserialize(d)?
        .map(|v| ms_of(&v).ok_or_else(|| serde::de::Error::custom(format!("not a time: {v}"))))
        .transpose()
}

/// A flag: `true` or `false`, or `1` or `0` (a writer without booleans).
fn lenient_bool<'de, D: serde::Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    match Value::deserialize(d)? {
        Value::Bool(b) => Ok(b),
        v => match v.as_u64() {
            Some(1) => Ok(true),
            Some(0) => Ok(false),
            _ => Err(serde::de::Error::custom(format!("not a flag: {v}"))),
        },
    }
}

/// Tags as JSON: `[{"n": "Work", "c": 4}, …]`, `c` left out when 0.
fn tags_value(tags: &Tags) -> Value {
    tags.iter()
        .map(|t| match t.colour {
            0 => json!({"n": t.name}),
            c => json!({"n": t.name, "c": c}),
        })
        .collect()
}

/// Reads [`tags_value`] leniently: entries without a string name are skipped (and [`Tags::new`]
/// reads a colour that isn't one of Finder's as none).
fn tags_from_value(value: &Value) -> Tags {
    let Some(list) = value.as_array() else {
        return Tags::default();
    };
    Tags::new(list.iter().filter_map(|t| {
        let colour = t["c"].as_u64().and_then(|c| u8::try_from(c).ok());
        Some(Tag::new(t["n"].as_str()?, colour.unwrap_or(0)))
    }))
}

/// Content keys as JSON: [`NodeContent::unknown`] and what's kept of it.
pub type Keys = Map<String, Value>;

/// Reads [`NodeContent::carried_json`]; anything else reads as none.
pub fn keys_from_str(keys: &str) -> Keys {
    serde_json::from_str(keys).unwrap_or_default()
}

/// The tags among content keys kept as a JSON object (a client's unknown keys).
pub fn tags_in_keys(keys: &str) -> Tags {
    if !keys.contains(&format!("\"{TAGS_KEY}\"")) {
        return Tags::default();
    }
    tags_from_json(keys, |keys| &keys[TAGS_KEY])
}

/// Tags as a JSON string (how the state DB keeps them).
pub fn tags_to_string(tags: &Tags) -> String {
    tags_value(tags).to_string()
}

/// Reads [`tags_to_string`]; anything else reads as no tags.
pub fn tags_from_str(s: &str) -> Tags {
    if s == "[]" {
        return Tags::default(); // most rows
    }
    tags_from_json(s, |v| v)
}

fn tags_from_json(json: &str, at: impl Fn(&Value) -> &Value) -> Tags {
    serde_json::from_str::<Value>(json)
        .map(|v| tags_from_value(at(&v)))
        .unwrap_or_default()
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
    fn a_js_or_kotlin_writers_mtime_and_flag_read() -> Result<(), serde_json::Error> {
        let c =
            NodeContent::from_json(r#"{"v":1,"name":"a","mtime":1759920000123.6,"size":3,"x":1}"#)?;
        assert_eq!(
            (c.mtime, c.size, c.x),
            (Some(1_759_920_000_124), Some(3), true)
        );
        let c = NodeContent::from_json(r#"{"v":1,"name":"a","mtime":null,"x":0}"#)?;
        assert_eq!((c.mtime, c.x), (None, false));
        // Written back as this client writes them.
        assert_eq!(c.to_json(), r#"{"v":1,"name":"a"}"#);
        // A size or version that isn't a whole number, a time or flag that isn't one: unreadable.
        for bad in [
            r#"{"v":1,"name":"a","size":3.5}"#,
            r#"{"v":1.5,"name":"a"}"#,
            r#"{"v":1,"name":"a","mtime":"yesterday"}"#,
            r#"{"v":1,"name":"a","x":2}"#,
            r#"{"v":1,"name":"a","x":"yes"}"#,
        ] {
            assert!(NodeContent::from_json(bad).is_err(), "{bad}");
        }
        Ok(())
    }

    #[test]
    fn tags_ride_with_the_unknown_keys() -> Result<(), serde_json::Error> {
        let mut c = NodeContent::folder("a");
        let tags = Tags::new([Tag::new("Work", 0), Tag::new("Red", 6)]);
        c.set_tags(&tags);
        let json = c.to_json();
        assert_eq!(
            json,
            r#"{"v":1,"name":"a","tags":[{"c":6,"n":"Red"},{"n":"Work"}]}"#
        );
        assert_eq!(NodeContent::from_json(&json)?.tags(), tags);
        c.set_tags(&Tags::default());
        assert_eq!(c.to_json(), r#"{"v":1,"name":"a"}"#);
        // Another writer's odd entries: skipped, and an unknown colour reads as none.
        let odd = NodeContent::from_json(
            r#"{"v":1,"name":"a","tags":[{"n":"x","c":42},{"c":1},"y",{"n":"z","c":2}]}"#,
        )?;
        assert_eq!(odd.tags(), Tags::new([Tag::new("x", 0), Tag::new("z", 2)]));
        // Written back as read while the set is the same; as this client writes them once it
        // changes.
        let mut kept = odd.clone();
        kept.set_tags_over(&odd.tags(), &odd.tags());
        assert_eq!(kept.to_json(), odd.to_json());
        kept.set_tags_over(&Tags::new([Tag::new("z", 2)]), &odd.tags());
        assert_eq!(
            kept.to_json(),
            r#"{"v":1,"name":"a","tags":[{"c":2,"n":"z"}]}"#
        );
        assert_eq!(tags_from_str(&tags_to_string(&tags)), tags);
        Ok(())
    }

    #[test]
    fn an_older_writers_removal_marker_is_dropped() -> Result<(), serde_json::Error> {
        let c = NodeContent::from_json(
            r#"{"v":1,"name":"a","rm":{"at":7,"root":"2a1c0d55-8ad3-417e-a5d7-42c3d0baba0e"},"ct":3}"#,
        )?;
        assert_eq!(c.to_json(), r#"{"v":1,"name":"a","ct":3}"#);
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
