//! The special folders a device can sync where they are (node schema v1, "Role folders"): each
//! is a folder at the drive's top whose node id is fixed, the same on every drive, so devices
//! adding the same one join it.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

use crate::NodeId;

/// A special folder, found on each device by its role, never by its name.
/// Written and read as [`Role::key`] names it (config files, the daemon's status).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(into = "&'static str", try_from = "String")]
pub enum Role {
    Documents,
    Pictures,
    Music,
    Videos,
    Downloads,
}

impl Role {
    pub const ALL: [Role; 5] = [
        Role::Documents,
        Role::Pictures,
        Role::Music,
        Role::Videos,
        Role::Downloads,
    ];

    /// Its folder's node id on every drive.
    pub const fn node(self) -> NodeId {
        NodeId(Uuid::from_u128(match self {
            Role::Documents => 0x0274575b_c2cf_4ac7_b39b_d56b2c121b75,
            Role::Pictures => 0xc2c9af9b_4873_48d8_a8ff_4779802f038f,
            Role::Music => 0x0f4cd8f9_a914_476d_86d0_b1d1a5215a85,
            Role::Videos => 0xb133291b_9309_46c5_9bfd_ac6c81669049,
            Role::Downloads => 0x5290e709_bc6d_4e12_827e_96d6dd006403,
        }))
    }

    /// The role whose folder `node` is.
    pub fn of(node: NodeId) -> Option<Role> {
        Role::ALL.into_iter().find(|r| r.node() == node)
    }

    /// Its folder's name on the drive when a device creates it.
    pub const fn name(self) -> &'static str {
        match self {
            Role::Documents => "Documents",
            Role::Pictures => "Pictures",
            Role::Music => "Music",
            Role::Videos => "Videos",
            Role::Downloads => "Downloads",
        }
    }

    /// How config files and the command line name it.
    pub const fn key(self) -> &'static str {
        match self {
            Role::Documents => "documents",
            Role::Pictures => "pictures",
            Role::Music => "music",
            Role::Videos => "videos",
            Role::Downloads => "downloads",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.key())
    }
}

impl From<Role> for &'static str {
    fn from(role: Role) -> Self {
        role.key()
    }
}

impl TryFrom<String> for Role {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl FromStr for Role {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Role::ALL
            .into_iter()
            .find(|r| r.key().eq_ignore_ascii_case(s))
            .ok_or_else(|| {
                let keys: Vec<_> = Role::ALL.iter().map(|r| r.key()).collect();
                format!("{s:?} is none of {}", keys.join(", "))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_have_their_own_ids_and_read_back() {
        let ids: std::collections::HashSet<_> = Role::ALL.iter().map(|r| r.node()).collect();
        assert_eq!(ids.len(), Role::ALL.len());
        assert!(!ids.contains(&NodeId::ROOT));
        for role in Role::ALL {
            assert_eq!(Role::of(role.node()), Some(role));
            assert_eq!(role.key().parse::<Role>(), Ok(role));
            assert_eq!(role.name().parse::<Role>(), Ok(role));
        }
        assert_eq!(Role::of(NodeId::ROOT), None);
        assert!("desktop".parse::<Role>().is_err());
    }
}
