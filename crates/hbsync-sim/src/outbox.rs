//! A writing member's writes on their way to the sharer's drive: the member's own server
//! holds them ([`crate::Sim::hold_member_writes`]) until the sharer's server takes them
//! ([`crate::Event::Deliver`]), as `hbsync_remote`'s fake does (`Faults::on_their_way`,
//! `FakeRemote::deliver`).

use std::collections::BTreeMap;

use hbsync_types::{NodeId, NodeState, RemoteRef};

/// A write of the member's as it is sent: by this a write is the same one asked again (the
/// fake's `Ask`, with its bytes: the state it writes holds them).
#[derive(Debug, Clone)]
pub(crate) enum Write {
    /// A new file or folder, as it is to be on the drive (in the drive's coordinates).
    Create { state: NodeState },
    /// A change of the node at `at`: as it is to be.
    Update { at: RemoteRef, state: NodeState },
    /// A move of the node at `at` to Recently deleted; `rightly`: as a user deleted it, at
    /// that version (judged when sent: the node is at that version when it lands, or the
    /// write isn't taken).
    Remove { at: RemoteRef, rightly: bool },
}

/// The same write: what it asks of the drive. (Not how a removal is judged, which the
/// oracle works out anew at each ask.)
impl PartialEq for Write {
    fn eq(&self, other: &Write) -> bool {
        match (self, other) {
            (Write::Create { state: a }, Write::Create { state: b }) => a == b,
            (Write::Update { at: a, state: s }, Write::Update { at: b, state: t }) => {
                a == b && s == t
            }
            (Write::Remove { at: a, .. }, Write::Remove { at: b, .. }) => a == b,
            _ => false,
        }
    }
}

impl Write {
    /// The version of their file it was sent over, where it changes one.
    pub(crate) fn over(&self) -> Option<RemoteRef> {
        match self {
            Write::Create { .. } => None,
            Write::Update { at, .. } | Write::Remove { at, .. } => Some(*at),
        }
    }
}

/// A write held on the member's server: which send it is, whose, and the tags it sends up.
#[derive(Debug, Clone)]
pub(crate) struct Held {
    pub(crate) send: u64,
    pub(crate) client: usize,
    pub(crate) node: NodeId,
    pub(crate) write: Write,
    pub(crate) tags: Vec<String>,
}

/// The member's server's side of its writes on their way, for all its clients (the fake's
/// `Outbox`).
#[derive(Debug, Default)]
pub(crate) struct Outbox {
    /// The writes held, in the order they were sent.
    pub(crate) held: Vec<Held>,
    /// The last send of a node the sharer's server took, and the version it wrote: the word
    /// a member hears of a send, until the client that sent it asks again.
    pub(crate) landed: BTreeMap<NodeId, (u64, RemoteRef)>,
    /// How many writes were held so far: the next send's number.
    pub(crate) sends: u64,
}

impl Outbox {
    /// Whether a write of `node` is held on its way.
    pub(crate) fn holds(&self, node: NodeId) -> bool {
        self.held.iter().any(|h| h.node == node)
    }
}
