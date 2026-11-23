//! What this identity's server tells of what it sent on to another identity's (a write to
//! something they share with this one): the `peerSendResult` frames of the notification socket
//! (`docs/spec/homebase-api-notes.md`, "Writing to what others share"). A write waits here for
//! its own.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::Deserialize;
use uuid::Uuid;

/// One send's result, as the socket tells it (`data` of a `peerSendResult` frame; odin-core
/// `PeerSendResultNotification`, **[verified]** live at 3ace44b69).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SendResult {
    /// The file's global transit id: the same on both servers.
    pub global_transit_id: Uuid,
    /// The version the send's own answer gave (`newVersionTag`): which send this is of.
    pub sent_version_tag: Uuid,
    /// odin-core's `LatestTransferStatus`, by name.
    pub status: String,
    /// The server tries again by itself: not the last word.
    #[serde(default)]
    pub will_retry: bool,
    /// The version this write gave their file, once delivered (odin-core's
    /// `PeerTransferResponse.VersionTag`): a new file's is told here alone. Told again of a
    /// send that had landed before, it is the version a move to or from their Recently
    /// deleted gave the file since.
    #[serde(default)]
    pub version_tag: Option<Uuid>,
}

impl SendResult {
    /// Their server took the write.
    pub(crate) fn delivered(&self) -> bool {
        self.is("delivered")
    }

    /// `status` is this one of odin-core's (camelCase, as the socket sends an enum).
    pub(crate) fn is(&self, status: &str) -> bool {
        self.status.eq_ignore_ascii_case(status)
    }
}

/// The last word on this many sends is kept for whoever asks after it came.
const KEPT: usize = 256;

/// The results told, shared by a login's clients: the socket tells them
/// ([`Sends::told`]), a write waits for its own ([`Sends::wait`]).
#[derive(Debug, Default)]
pub(crate) struct Sends {
    /// The last word on each send, oldest first: one may come before its write waits for it.
    told: Mutex<VecDeque<SendResult>>,
    changed: Condvar,
}

impl Sends {
    fn lock(&self) -> MutexGuard<'_, VecDeque<SendResult>> {
        self.told.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A result the socket told. One the server will try again is not kept: its last word
    /// follows.
    pub(crate) fn told(&self, result: SendResult) {
        if result.will_retry {
            return;
        }
        let mut told = self.lock();
        if told.len() == KEPT {
            told.pop_front();
        }
        told.push_back(result);
        drop(told);
        self.changed.notify_all();
    }

    /// The last word on the send of `global_transit_id` that answered `sent`, waited for up to
    /// `within`: none came (no socket connected, or the frame lost: the caller looks itself).
    pub(crate) fn wait(
        &self,
        global_transit_id: Uuid,
        sent: Uuid,
        within: Duration,
    ) -> Option<SendResult> {
        let until = Instant::now() + within;
        let mut told = self.lock();
        loop {
            let found = told.iter().position(|r| {
                r.global_transit_id == global_transit_id && r.sent_version_tag == sent
            });
            if let Some(at) = found {
                return told.remove(at);
            }
            let left = until.checked_duration_since(Instant::now())?;
            told = self
                .changed
                .wait_timeout(told, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn result(gtid: u128, sent: u128, status: &str, will_retry: bool) -> SendResult {
        SendResult {
            global_transit_id: Uuid::from_u128(gtid),
            sent_version_tag: Uuid::from_u128(sent),
            status: status.into(),
            will_retry,
            version_tag: None,
        }
    }

    /// A frame's data as the dev server sent it (2026-11-23).
    #[test]
    fn a_result_reads_as_the_server_sends_it() -> Result<(), serde_json::Error> {
        let delivered: SendResult = serde_json::from_str(
            r#"{"recipient":"pippin.dotyou.cloud","driveId":"cfceb937-b868-496b-91cb-3402f18320f8","globalTransitId":"9a9210b8-c247-4547-a8e8-3116710ffdba","sentVersionTag":"1a81201a-4009-b000-3305-da0e149b18f0","isUpdate":true,"isDelete":false,"status":"delivered","willRetry":false,"versionTag":"1a81201a-4009-b000-3305-da0e149b18f0"}"#,
        )?;
        assert!(delivered.delivered() && !delivered.will_retry);
        assert_eq!(delivered.version_tag, Some(delivered.sent_version_tag));
        let refused: SendResult = serde_json::from_str(
            r#"{"recipient":"pippin.dotyou.cloud","driveId":"cfceb937-b868-496b-91cb-3402f18320f8","globalTransitId":"9a9210b8-c247-4547-a8e8-3116710ffdba","sentVersionTag":"1d81201a-b005-c700-ffb9-77a57915b545","isUpdate":true,"isDelete":false,"status":"recipientIdentityReturnedVersionTagMismatch","willRetry":false,"versionTag":null}"#,
        )?;
        assert!(!refused.delivered());
        assert!(refused.is("RecipientIdentityReturnedVersionTagMismatch"));
        assert_eq!(refused.version_tag, None);
        Ok(())
    }

    /// A write gets the last word on its own send: one told before it waits, one told while it
    /// waits, never another send's, nor one the server will try again; and nothing in time
    /// is none.
    #[test]
    fn a_write_waits_for_its_own_result() {
        let sends = Arc::new(Sends::default());
        let (gtid, sent) = (Uuid::from_u128(1), Uuid::from_u128(10));
        let soon = Duration::from_millis(20);

        sends.told(result(1, 11, "delivered", false));
        sends.told(result(2, 10, "delivered", false));
        sends.told(result(1, 10, "recipientServerNotResponding", true));
        assert_eq!(sends.wait(gtid, sent, soon), None);

        sends.told(result(1, 10, "delivered", false));
        let got = sends.wait(gtid, sent, soon).expect("told before");
        assert!(got.delivered());
        // Taken: not there twice.
        assert_eq!(sends.wait(gtid, sent, soon), None);

        let telling = Arc::clone(&sends);
        let teller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            telling.told(result(
                1,
                10,
                "recipientIdentityReturnedAccessDenied",
                false,
            ));
        });
        let got = sends
            .wait(gtid, sent, Duration::from_secs(10))
            .expect("told meanwhile");
        assert!(got.is("recipientIdentityReturnedAccessDenied"));
        teller.join().expect("the teller");
    }

    /// Only the last [`KEPT`] results are kept: the oldest go.
    #[test]
    fn old_results_are_dropped() {
        let sends = Sends::default();
        for n in 0..=KEPT as u128 {
            sends.told(result(n, n, "delivered", false));
        }
        let none = Duration::ZERO;
        assert_eq!(
            sends.wait(Uuid::from_u128(0), Uuid::from_u128(0), none),
            None
        );
        assert!(
            sends
                .wait(Uuid::from_u128(1), Uuid::from_u128(1), none)
                .is_some()
        );
    }
}
