//! A folder's coordinates. Each folder's own rows (base, local) are in its coordinates: its
//! top is the root. The drive's rows (the mirror, Recently deleted) are the account's, in the
//! drive's: [`Coords`] translates them for a folder holding part of the drive.

use hbsync_types::NodeId;

/// How a folder whose top is the drive's node `top` sees the drive. The main folder's top is
/// the drive's root: nothing changes. Any other folder sees `top` as its root, and the rest of
/// the drive as one folder, [`NodeId::OUTSIDE`], at its top.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coords {
    pub top: NodeId,
}

impl Coords {
    pub const MAIN: Coords = Coords { top: NodeId::ROOT };
    /// The name of the one folder the rest of the drive is to a folder holding part of it.
    pub const OUTSIDE_NAME: &str = "Outside";

    fn is_main(self) -> bool {
        self.top.is_root()
    }

    /// A node of the drive as the folder sees it.
    pub fn from_drive(self, node: NodeId) -> NodeId {
        match node {
            _ if self.is_main() => node,
            n if n == self.top => NodeId::ROOT,
            n if n.is_root() => NodeId::OUTSIDE,
            n => n,
        }
    }

    /// A node the folder sees, on the drive.
    pub fn to_drive(self, node: NodeId) -> NodeId {
        match node {
            _ if self.is_main() => node,
            n if n.is_root() => self.top,
            n if n == NodeId::OUTSIDE => NodeId::ROOT,
            n => n,
        }
    }

    /// Whether the folder doesn't see `node` (a drive node) as a node: its top, which is its
    /// root.
    pub fn hides(self, node: NodeId) -> bool {
        !self.is_main() && node == self.top
    }

    /// Whether `node` (one the folder sees) is the synthetic folder of the rest of the drive.
    pub fn is_outside(self, node: NodeId) -> bool {
        !self.is_main() && node == NodeId::OUTSIDE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_folder_sees_its_top_as_the_root_and_the_rest_outside() {
        let (top, other) = (NodeId::new(), NodeId::new());
        let docs = Coords { top };
        assert_eq!(docs.from_drive(top), NodeId::ROOT);
        assert_eq!(docs.from_drive(NodeId::ROOT), NodeId::OUTSIDE);
        assert_eq!(docs.from_drive(other), other);
        for n in [NodeId::ROOT, NodeId::OUTSIDE, other] {
            assert_eq!(docs.from_drive(docs.to_drive(n)), n);
        }
        assert!(docs.hides(top) && !docs.hides(other));
        assert!(docs.is_outside(NodeId::OUTSIDE));
        // The main folder sees the drive as it is.
        for n in [NodeId::ROOT, top, other] {
            assert_eq!(Coords::MAIN.from_drive(n), n);
            assert_eq!(Coords::MAIN.to_drive(n), n);
        }
        assert!(!Coords::MAIN.hides(NodeId::ROOT) && !Coords::MAIN.is_outside(NodeId::OUTSIDE));
    }
}
