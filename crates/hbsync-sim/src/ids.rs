//! The node ids a simulation hands out: counted, so a saved seed makes the same ids on any day
//! and replays the same run.

use hbsync_types::{NodeId, Uuid};

/// The time new items' ids carry (2026-10-08, Unix ms): a fixed day, as a client's ids made then.
const MADE_AT_MS: u128 = 1_791_417_600_000;

/// The simulation's one source of node ids, mapped from a count the same way on every run
/// (pinned by a test: changing it changes what every saved seed replays). Each is the id a device
/// makes for a new node (UUIDv7, `NodeId::new`): for a new item, a copy detached from its node,
/// and an item the scanner finds again after it stopped tracking it. At a fixed time, counting
/// up: they sort in the order made, and below nearly every hashed id (a conflict copy's, a role
/// folder's), as the ids made on a real day do.
#[derive(Debug, Default)]
pub(crate) struct Ids {
    made: u64,
}

impl Ids {
    /// A new id.
    pub(crate) fn fresh(&mut self) -> NodeId {
        self.made += 1;
        // UUIDv7: 48 bits of time, the version, 12 bits (zero), the variant, a 62-bit count.
        let count = u128::from(self.made) & ((1 << 62) - 1);
        let id = MADE_AT_MS << 80 | 0x7 << 76 | 0b10 << 62 | count;
        NodeId(Uuid::from_u128(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ids a run hands out stay what they are: the saved seeds
    /// (`tests/props.proptest-regressions`) replay the runs they were saved for only while
    /// they do.
    #[test]
    fn the_ids_stay_the_same() {
        let mut ids = Ids::default();
        let ids: Vec<String> = (0..2).map(|_| ids.fresh().0.to_string()).collect();
        assert_eq!(
            ids,
            [
                "01a118cf-4800-7000-8000-000000000001",
                "01a118cf-4800-7000-8000-000000000002"
            ]
        );
    }
}
