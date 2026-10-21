//! What happens in a simulation: the users' actions and the events of a run, with the
//! pools the generated values pick from.

/// A user action on one client's local tree. Indices pick among existing items modulo their
/// count, so every generated value is meaningful (and shrinks well).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    CreateFile {
        parent: usize,
        name: usize,
    },
    CreateFolder {
        parent: usize,
        name: usize,
    },
    Edit {
        file: usize,
    },
    /// The user edits a file and is still writing it at the next plan: not hashed yet there
    /// (its merge deferred), hashed at the one after.
    EditWriting {
        file: usize,
    },
    /// Another app holds a file locked: no plan finds it hashed (its merge deferred, and what
    /// waits for it waits) till the clients settle, when it lets go.
    Lock {
        file: usize,
    },
    Move {
        node: usize,
        parent: usize,
        name: usize,
    },
    Delete {
        node: usize,
    },
    /// A file on the built-in junk list (an OS thumbnail cache).
    CreateJunk {
        parent: usize,
    },
    /// The client starts or stops ignoring `d` (a rule change, picked up at the next plan).
    ToggleIgnore,
    /// The user restores one item of Recently deleted, on the server at once, with what was
    /// deleted with it (node schema: in it, removed no earlier than it), and the folders above it
    /// in Recently deleted first: back in its folder under a free name, or as it was if that is
    /// gone for good (an orphan, for Recovered files).
    Restore {
        node: usize,
    },
    /// The daily purge ([`Server::purge`]): the server's clock moves on by [`PURGE_EVERY`];
    /// what the deletes of the items of Recently deleted removed [`TRACE`] before took along
    /// and left live is removed (`along::taken_levels`), and those items go for good, as the
    /// engine selects them (`hbsync_engine::deleted::select`): never a live node, so a folder
    /// holding one stays.
    ///
    /// [`Server::purge`]: crate::model::server::Server::purge
    /// [`PURGE_EVERY`]: crate::PURGE_EVERY
    /// [`TRACE`]: crate::TRACE
    Purge,
    /// Another device (the phone, or a computer never seen again) deletes a folder of the
    /// drive and stops half way: the folder, then the first `done` of what is in it, top-down
    /// (node schema, Recently deleted); the rest stays live, taken along. Any folder, the role
    /// folder and what is in it too ([`Sim::stopped_folders`]); nothing when the drive holds
    /// none.
    ///
    /// [`Sim::stopped_folders`]: crate::Sim::stopped_folders
    DeleteStopped {
        pick: usize,
        done: usize,
    },
    /// Another app deletes a live folder of the drive for good, outright (no trace in the feed:
    /// a hard delete): what was in it stays live with no folder, an orphan, which the main
    /// folders put into Recovered files. Any folder, as [`Action::DeleteStopped`] picks it;
    /// nothing when the drive holds none.
    DeleteOutright {
        pick: usize,
    },
    /// Another app moves a live node of the drive into a live file (its `groupId` a file's node
    /// id): the main folders move it into Recovered files, as an orphan. Any node but the fixed
    /// folders; nothing when the drive holds no file, or only that node.
    IntoFile {
        pick: usize,
        file: usize,
    },
    /// A newer client makes a folder at the drive's top, with a file in it: the folder doesn't
    /// read here (a newer schema, say), the file does. What is in it waits for it on every
    /// computer: it is never moved into Recovered files.
    NewerFolder {
        name: usize,
    },
    /// A user lists Recently deleted ([`hbsync_engine::deleted::deletes`]): what it holds now,
    /// each node at its version, is what emptying it as listed may take.
    ListDeleted,
    /// The user empties Recently deleted as last listed (nothing without a listing): what the
    /// engine selects of the rows still as listed (`hbsync_engine::deleted::select`), in its
    /// order.
    EmptyListed,
    /// The user empties one item of Recently deleted as last listed (a pick of its rows; nothing
    /// without a listing): it and all of Recently deleted in it ([`deleted::inside`]: what was
    /// deleted from it before too), at their listed versions, as the engine selects them
    /// ([`deleted::AccountOps::empty`] with the listing: nothing recorded since). An item holding
    /// nothing live and nothing written since the listing goes.
    ///
    /// [`deleted::inside`]: hbsync_engine::deleted::inside
    /// [`deleted::AccountOps::empty`]: hbsync_engine::deleted::AccountOps::empty
    EmptyItem {
        pick: usize,
    },
    /// Another device (the phone, which opened it before) saves an edit of a file a folder's
    /// delete took along, its own removal still on its way: on the server at once, the folder
    /// still deleted. Nothing when no file is taken along.
    EditTaken {
        pick: usize,
    },
    /// The user adds a tag of [`TAGS`] to an item, or removes it, as far as the client's OS
    /// lets them.
    Tag {
        node: usize,
        tag: usize,
    },
    /// The user gives a tag of an item another colour (macOS).
    Colour {
        node: usize,
        tag: usize,
    },
    /// Another device (the phone) changes the role folder ([`Sim::with_role_folder`]), on the
    /// server at once; nothing without one.
    ///
    /// [`Sim::with_role_folder`]: crate::Sim::with_role_folder
    Phone {
        op: usize,
        pick: usize,
    },
    /// The phone deletes the role folder itself, with what is in it (to Recently deleted); a
    /// device syncing it brings it back (as `Drive::ensure_role`).
    DeleteRoleFolder,
    /// The user moves a file from one folder of the client's computer to the other, at its
    /// top ([`Sim::with_role_device`]): the scan of the other folder finds a new item. Nothing
    /// on a client with one folder.
    ///
    /// [`Sim::with_role_device`]: crate::Sim::with_role_device
    MoveAcross {
        file: usize,
    },
    /// Another device changes the node of this client's next remote write once it landed,
    /// before its answer is recorded, and the account pulls that change meanwhile (another
    /// folder's engine, or an announcement): it removes the node the write left live (or, `op`
    /// odd, edits it where it is a file), or restores one the write removed. The answer is then
    /// recorded as the engine's store records it (`AccountTx::answered`), which must keep the
    /// newer state, and the node settles against the answer, as if the pull came after it.
    Meanwhile {
        op: usize,
    },
}

/// Tags the users pick from: one no Linux client can hold, one with a newline no macOS client
/// can hold.
pub(crate) const TAGS: &[(&str, u8)] = &[("Red", 6), ("Work", 0), ("a,b", 4), ("x\ny", 2)];

/// The junk name [`Action::CreateJunk`] uses.
pub(crate) const JUNK: &str = "Thumbs.db";

/// One step of a simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Act {
        client: usize,
        action: Action,
    },
    /// Snapshot the server and plan (replacing any unfinished plan).
    Plan {
        client: usize,
    },
    /// Execute the next planned operation.
    Step {
        client: usize,
    },
    /// The user confirms the deletes the mass-delete guard held in the client's last plan
    /// (with [`Sim::guard`] set).
    ///
    /// [`Sim::guard`]: crate::Sim::guard
    Confirm {
        client: usize,
    },
    /// The client's process dies during its next step: a download's commit is lost after its
    /// file is in place ([`Sim::with_split_downloads`]), and the rest of its plan with it. Its
    /// next plan recovers first, as the engine's start of a sync does.
    ///
    /// [`Sim::with_split_downloads`]: crate::Sim::with_split_downloads
    Crash {
        client: usize,
    },
}

/// Names drawn from a small pool so clashes (including case-only ones) are common. The last
/// two are as long as a name may be, so conflict names have to shorten them.
pub(crate) const NAMES: &[&str] = &[
    "a",
    "b",
    "A",
    "c.txt",
    "C.TXT",
    "d",
    "LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL.txt",
    "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll.TXT",
];

/// For unit tests: client `client` does `action`.
#[cfg(test)]
pub(crate) fn act(client: usize, action: Action) -> Event {
    Event::Act { client, action }
}
