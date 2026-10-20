//! An identity (a Homebase address) as the client keeps it: normalised once, where it comes
//! in (typed, read from the config, decoded from the control socket), so identities compare
//! as plain strings everywhere after.

use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// An identity's domain, normalised: `Anna.Example.com/` is `anna.example.com`. A string on
/// the wire and in the config.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Identity(String);

impl Identity {
    /// The identity as typed (`Anna.Example.com/`), as the client keeps it
    /// (`anna.example.com`).
    pub fn new(typed: &str) -> Identity {
        Identity(typed.trim().trim_end_matches('/').to_lowercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Its name in a file or folder name (its state DB's folder, its credentials file): a
    /// port's `:` as `_`, which no file system refuses.
    pub fn file_key(&self) -> String {
        self.0.replace(':', "_")
    }
}

impl From<&str> for Identity {
    fn from(typed: &str) -> Identity {
        Identity::new(typed)
    }
}

impl Deref for Identity {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// As the string it is: `"anna.example.com"`.
impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl Serialize for Identity {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

/// Normalised as it is read: a config edited by hand, or a client of another build.
impl<'de> Deserialize<'de> for Identity {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Identity, D::Error> {
        Ok(Identity::new(&String::deserialize(d)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_identity_is_normalised_once() {
        let typed = Identity::new(" Anna.Example.com/ ");
        assert_eq!(typed.as_str(), "anna.example.com");
        assert_eq!(format!("{typed:?}"), r#""anna.example.com""#);
        assert_eq!(Identity::new("anna.example.com"), typed);
        let read: Identity = serde_json::from_str(r#""Anna.Example.COM""#).expect("a string");
        assert_eq!(read, typed, "normalised as it is read");
        assert_eq!(
            serde_json::to_string(&typed).expect("a string"),
            r#""anna.example.com""#
        );
    }

    /// Its file names hold no `:` (refused on Windows); without a port it is the identity.
    #[test]
    fn its_file_key_has_no_colon() {
        assert_eq!(
            Identity::new("Frodo.dotyou.cloud:8443").file_key(),
            "frodo.dotyou.cloud_8443"
        );
        assert_eq!(Identity::new("a.example").file_key(), "a.example");
    }
}
