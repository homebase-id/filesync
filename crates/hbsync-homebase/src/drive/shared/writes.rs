//! Writing to what another identity shares with this one for editing
//! (`docs/spec/homebase-api-notes.md`, "Writing to what others share"): a change to a file of
//! theirs, a new file or folder in a folder of theirs, and a move to their Recently deleted or
//! back. Nothing is deleted there from here.
//!
//! Every write goes through this identity's own server, which stages it and sends it on
//! (**[verified]**: their server takes no write from a member directly). The answer says only
//! that it is on its way; what became of it this identity's server tells on the notification
//! socket ([`crate::sends`]). A write waits for that word, and looks at their file itself
//! where none comes.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use hbsync_remote::{Answers, NodeMeta, RemoteBody, RemoteError, Result, Upload};
use hbsync_types::{ContentHash, NodeId, RemoteRef, Target, VersionTag, Written};
use serde::Deserialize;
use uuid::Uuid;

use super::super::{
    NOT_CONNECTED_WAIT, Sending, check_size, codeless_forbidden, files_route, finish,
    not_connected, parts, remote_error, wire,
};
use super::PeerRemote;
use crate::app::{APP_SLUG, SYNC_DRIVE};
use crate::client::UploadMethod;
use crate::crypto::Point;
use crate::drive::sealed::{self, KeyHeader};
use crate::multipart::Multipart;
use crate::sends::SendResult;
use crate::{Client, HomebaseError};

/// Why nothing is written to a share: the app may not send to other identities.
const MAY_NOT_SEND: &str = "your Homebase cloud hasn't given Filesync the permission to change \
                            what others share with you yet";

/// How long a write waits for the socket's word between its own looks at their file: 15
/// seconds in all, then it says it is waiting ([`RemoteError::Waiting`]). The word comes in
/// well under the first wait where both servers are up (20 to 120 ms measured for the whole
/// write): no look is made then. Without a socket (a sync run once, by hand) a write takes
/// the first wait and one look.
const WAITS: [Duration; 5] = [
    Duration::from_millis(300),
    Duration::from_millis(700),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];

/// When a write still on its way is asked about again.
const ASK_AGAIN: Duration = Duration::from_secs(60);

/// What a share's writes keep between them.
#[derive(Debug, Default)]
pub(super) struct Writes {
    /// Their drive's write-only public key, asked for once: a new file's key is sealed to it.
    point: Mutex<Option<Point>>,
    /// The files (by global transit id) whose key their server holds sealed to their drive
    /// still: ones added from here, and ones it said so of. A change to one is sent sealed.
    sealed: Mutex<HashSet<Uuid>>,
    /// The writes sent whose end isn't known yet, by node: asked again, each is looked for
    /// before it is sent a second time.
    on_their_way: Mutex<HashMap<NodeId, OnItsWay>>,
}

/// A write sent, its end not known.
#[derive(Debug, Clone)]
struct OnItsWay {
    global_transit_id: Uuid,
    /// The version the send's answer gave.
    sent: Uuid,
    /// What was written ([`what`]): another write of the node is another send.
    what: String,
    /// The hash of the bytes sent, if any.
    content: Option<ContentHash>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a write says, to tell it from another write of the same node.
fn what(meta: &NodeMeta, bytes: bool) -> String {
    format!(
        "{}|{:?}|{}|{bytes}",
        meta.content.to_json(),
        meta.parent,
        meta.removed
    )
}

/// This identity's server's answer to a write for another's drive: queued.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Queued {
    new_version_tag: Uuid,
}

/// `GET .../send-status`: whether the send still waits in this identity's outbox. Not pending
/// says nothing of how it ended (**[verified]** live: the same after a delivery and a refusal).
#[derive(Deserialize)]
struct SendStatus {
    #[serde(default)]
    pending: bool,
}

/// `GET .../public-key`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublicKey {
    public_key_jwk: String,
}

/// What became of a write sent.
enum Became {
    /// Their file is at this version now: the write's.
    Landed(VersionTag),
    /// Their server refused its key header: the file's key is sealed to their drive still
    /// (or no longer), so it goes the other way.
    OtherKeyHeader,
    /// Still in this identity's outbox.
    OnItsWay,
    /// Gone from the outbox, and their file isn't what was sent: refused there unheard (no
    /// socket told), or lost.
    Unheard,
}

impl PeerRemote {
    /// Refuses a write where the app may not send to other identities: before anything is
    /// sent, as a send refused for it leaves its staged copy behind on this identity's server
    /// (**[verified]** live, odin-core 3ace44b69).
    fn may_send(&self) -> Result<()> {
        match self.own.may_send()? {
            true => Ok(()),
            false => Err(RemoteError::Fatal(MAY_NOT_SEND.into())),
        }
    }

    fn owner(&self) -> &str {
        &self.sharer.odin_id
    }

    /// This identity's own server, as this login: what sends a write on, and tells what
    /// became of it.
    fn sender(&self) -> &Client {
        self.own.client()
    }

    /// A change to their file `target`: its header, and with `payload` its bytes
    /// ([`hbsync_remote::Remote::update`]).
    pub(super) fn change_theirs(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Written> {
        if let Some(p) = &payload {
            check_size(p.size)?;
        }
        self.may_send()?;
        let header = self.with_token(|client| self.raw_header(client, target.at.file_id))?;
        self.change(&header, target.at.version, meta, payload)
    }

    /// Their files `files` moved to their Recently deleted, or back
    /// ([`hbsync_remote::Remote::set_removed`]): each a change of its header alone, one after
    /// another (a header read, a send and its word for each: there is no batch for a member).
    pub(super) fn set_theirs_removed(
        &self,
        files: &[Target],
        removed: bool,
    ) -> Result<Answers<Written>> {
        self.may_send()?;
        let one = |target: &Target| -> Result<Written> {
            let header = self.with_token(|client| self.raw_header(client, target.at.file_id))?;
            let read = self
                .to_remote(&self.client(), header.clone())?
                .map_err(RemoteError::Fatal)?;
            let RemoteBody::Live { node, content, .. } = read.body else {
                return Err(RemoteError::Deleted);
            };
            if read.removed == removed {
                return Ok(Written {
                    target: Target {
                        at: RemoteRef {
                            file_id: read.file_id,
                            version: read.version,
                        },
                        token: None,
                    },
                    changed_at: read.changed_at,
                    content: None,
                });
            }
            let meta = NodeMeta {
                node,
                parent: read.parent,
                kind: read.kind,
                content,
                removed,
            };
            self.change(&header, target.at.version, &meta, None)
        };
        Ok(Answers::all(files.iter().map(one).collect()))
    }

    /// Sends `meta` (and `payload`) as their file `header`'s next version, if it is still at
    /// `version`, and waits for what became of it.
    fn change(
        &self,
        header: &wire::FileHeader,
        version: VersionTag,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Written> {
        if header.file_state == "deleted" {
            return Err(RemoteError::Deleted);
        }
        let stored = &header.file_metadata;
        let file_id = hbsync_types::RemoteFileId(header.file_id);
        let global_transit_id = stored.global_transit_id.ok_or_else(|| {
            RemoteError::Fatal(format!("file {}: no global transit id", header.file_id))
        })?;
        let written = |version: VersionTag, content: Option<ContentHash>| Written {
            target: Target {
                at: RemoteRef { file_id, version },
                token: None,
            },
            changed_at: None,
            content,
        };
        let what = what(meta, payload.is_some());
        // This very write, sent before: landed since, still on its way, or gone.
        if let Some(earlier) = self.writes.earlier(meta.node, &what) {
            if stored.version_tag == earlier.sent {
                self.writes.ended(meta.node);
                return Ok(written(VersionTag(earlier.sent), earlier.content));
            }
            if stored.version_tag == version.0 && self.pending(&earlier)? {
                return Err(self.on_its_way());
            }
            self.writes.ended(meta.node);
        }
        if stored.version_tag != version.0 {
            return Err(RemoteError::VersionMismatch);
        }
        if !header.shared_for_writing() {
            return Err(self.read_only());
        }
        let circle = self.circle_for(header)?;
        let key = header
            .key(Self::keys(&self.client(), circle.as_deref()))
            .and_then(|key| key.with_new_iv())
            .map_err(remote_error)?;
        let own = self.sender();
        let path = format!(
            "/api/v2/peer/{}/drives/{}/files/by-gtid/{global_transit_id}",
            self.owner(),
            self.sharer.drive_alias
        );
        let mut sending = Sending::of(payload, &key)?;
        // A header alone may go twice: with the key kept, and where their server holds it
        // sealed to their drive still, sealed (or the other way round).
        for last in [false, true] {
            let sealed = self.writes.is_sealed(global_transit_id);
            let key_header = match sealed {
                true => key.sealed_to(&self.their_point()?).map_err(remote_error)?,
                false => key.kept(),
            };
            let transfer_iv = sealed::strong().map_err(remote_error)?;
            let instructions = wire::member_update_instructions(
                &transfer_iv,
                sending.as_ref().map(|s| &s.descriptor),
                self.owner(),
            );
            let metadata =
                wire::member_update(meta, version, own.key(), &transfer_iv, &key, key_header);
            let form = Multipart::new(
                Uuid::new_v4().simple().to_string(),
                parts(&instructions, metadata, sending.as_mut(), &key),
            );
            let queued: Queued = own
                .upload(UploadMethod::Patch, &path, form)
                .map_err(|e| self.send_error(e))?;
            let content = sending.as_ref().map(|s| finish(&s.plain).0);
            let sent = OnItsWay {
                global_transit_id,
                sent: queued.new_version_tag,
                what: what.clone(),
                content,
            };
            let theirs = || {
                let now = self.with_token(|client| self.raw_header(client, file_id))?;
                Ok(Some(now.file_metadata.version_tag).filter(|v| *v == sent.sent))
            };
            match self.became(&sent, theirs)? {
                Became::Landed(version) => return Ok(written(version, content)),
                Became::OnItsWay => {
                    self.writes.sent(meta.node, sent);
                    return Err(self.on_its_way());
                }
                Became::OtherKeyHeader | Became::Unheard => {
                    // Unheard, it may have been that too: the next try goes the other way.
                    self.other_key_header(global_transit_id, sealed);
                    if last || sending.is_some() {
                        return Err(RemoteError::Transient(format!(
                            "{}'s server didn't take a change to file {file_id:?}: sent again",
                            self.owner()
                        )));
                    }
                }
            }
        }
        unreachable!("the second try returns")
    }

    /// A new file or folder in a folder of theirs ([`hbsync_remote::Remote::create`]): shared
    /// as that folder is with this identity, its key sealed to their drive.
    pub(super) fn add_to_theirs(
        &self,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Written> {
        if let Some(p) = &payload {
            check_size(p.size)?;
        }
        self.may_send()?;
        let written = |header: &wire::FileHeader, content: Option<ContentHash>| Written {
            target: Target {
                at: RemoteRef {
                    file_id: hbsync_types::RemoteFileId(header.file_id),
                    version: VersionTag(header.file_metadata.version_tag),
                },
                token: None,
            },
            changed_at: None,
            content,
        };
        let what = what(meta, payload.is_some());
        if let Some(earlier) = self.writes.earlier(meta.node, &what) {
            if let Some(theirs) = self.their_node(meta.node)? {
                self.writes.ended(meta.node);
                return Ok(written(&theirs, earlier.content));
            }
            if self.pending(&earlier)? {
                return Err(self.on_its_way());
            }
            self.writes.ended(meta.node);
        }
        let not_here = || {
            RemoteError::Fatal(format!(
                "nothing can be added there: {} doesn't share that folder with you for adding",
                self.owner()
            ))
        };
        if meta.parent.is_root() {
            return Err(not_here());
        }
        let folder = self.their_node(meta.parent)?.ok_or_else(not_here)?;
        if !folder.shared_for_writing() {
            return Err(self.read_only());
        }
        let key = KeyHeader::random().map_err(remote_error)?;
        let global_transit_id = Uuid::new_v4();
        // Shared as the folder is with this identity. The circles it isn't in get theirs from
        // a device of the owner's.
        let mut shares = Vec::new();
        for row in folder.shares.iter().flatten() {
            let Some(circle) = row.circle_id.filter(|_| row.permissions != 0) else {
                continue;
            };
            let locked = self
                .circle_key(circle)?
                .lock_file_key(&key.key, global_transit_id)
                .map_err(remote_error)?;
            shares.push(wire::share_of_new(row, circle, &locked));
        }
        let key_header = key.sealed_to(&self.their_point()?).map_err(remote_error)?;
        let own = self.sender();
        let mut sending = Sending::of(payload, &key)?;
        let transfer_iv = sealed::strong().map_err(remote_error)?;
        let instructions = wire::deposit_instructions(
            &transfer_iv,
            sending.as_ref().map(|s| &s.descriptor),
            self.owner(),
            &super::TargetDrive {
                alias: self.sharer.drive_alias,
                drive_type: SYNC_DRIVE.drive_type,
            },
        );
        let metadata = wire::deposit(
            meta,
            own.key(),
            &transfer_iv,
            &key,
            key_header,
            global_transit_id,
            shares,
        );
        let form = Multipart::new(
            Uuid::new_v4().simple().to_string(),
            parts(&instructions, metadata, sending.as_mut(), &key),
        );
        let queued: Queued = own
            .upload(
                UploadMethod::Post,
                &files_route(wire::TRANSIENT_DRIVE),
                form,
            )
            .map_err(|e| self.send_error(e))?;
        let content = sending.as_ref().map(|s| finish(&s.plain).0);
        let sent = OnItsWay {
            global_transit_id,
            sent: queued.new_version_tag,
            what,
            content,
        };
        // Sealed to their drive until a device of theirs locks its key with the drive's.
        self.writes.set_sealed(global_transit_id, true);
        let theirs = || {
            let found = self.their_node(meta.node)?;
            Ok(found.map(|header| header.file_metadata.version_tag))
        };
        match self.became(&sent, theirs)? {
            Became::Landed(_) => match self.their_node(meta.node)? {
                Some(header) => Ok(written(&header, content)),
                None => Err(RemoteError::Transient(format!(
                    "{}'s server took a new file that isn't there to read yet",
                    self.owner()
                ))),
            },
            Became::OnItsWay => {
                self.writes.sent(meta.node, sent);
                Err(self.on_its_way())
            }
            Became::OtherKeyHeader | Became::Unheard => {
                self.other_key_header(global_transit_id, true);
                Err(RemoteError::Transient(format!(
                    "{}'s server didn't take a new file: sent again",
                    self.owner()
                )))
            }
        }
    }

    /// What became of `sent`: the socket's word, waited for; where none comes, `theirs` (the
    /// version of their file now, if it is the write's) looked at between the waits, and at
    /// last this identity's outbox asked.
    fn became(&self, sent: &OnItsWay, theirs: impl Fn() -> Result<Option<Uuid>>) -> Result<Became> {
        let sends = self.sender().sends();
        for wait in WAITS {
            if let Some(told) = sends.wait(sent.global_transit_id, sent.sent, wait) {
                return self.told(&told, sent);
            }
            if let Some(version) = theirs()? {
                return Ok(Became::Landed(VersionTag(version)));
            }
        }
        Ok(match self.pending(sent)? {
            true => Became::OnItsWay,
            false => match theirs()? {
                Some(version) => Became::Landed(VersionTag(version)),
                None => Became::Unheard,
            },
        })
    }

    /// Their server refused a key header sent `sealed` or not: the next of the file goes the
    /// other way. One sealed may also have been sealed to a key their drive no longer has
    /// (the same refusal, **[verified]** odin-core `OutboxWorkerBase`): the key is asked for
    /// again.
    fn other_key_header(&self, global_transit_id: Uuid, sealed: bool) {
        self.writes.set_sealed(global_transit_id, !sealed);
        if sealed {
            *locked(&self.writes.point) = None;
        }
    }

    /// The socket's last word on a send, as what became of it; a refusal as the trait's error.
    fn told(&self, told: &SendResult, sent: &OnItsWay) -> Result<Became> {
        if !told.delivered() {
            tracing::debug!(
                "{}'s server didn't take a write of {}: {}",
                self.owner(),
                sent.global_transit_id,
                told.status
            );
        }
        if told.delivered() {
            return Ok(Became::Landed(VersionTag(
                told.version_tag.unwrap_or(sent.sent),
            )));
        }
        if told.is("recipientIdentityReturnedInvalidKeyHeader") {
            return Ok(Became::OtherKeyHeader);
        }
        Err(if told.is("recipientIdentityReturnedVersionTagMismatch") {
            RemoteError::VersionMismatch
        } else if told.is("recipientIdentityReturnedAccessDenied") {
            self.read_only()
        } else {
            RemoteError::Fatal(format!(
                "{}'s server didn't take the change ({})",
                self.owner(),
                told.status
            ))
        })
    }

    /// Whether `sent` still waits in this identity's outbox: one request.
    fn pending(&self, sent: &OnItsWay) -> Result<bool> {
        let path = format!(
            "/api/v2/peer/{}/drives/{}/files/by-gtid/{}/send-status",
            self.owner(),
            self.sharer.drive_alias,
            sent.global_transit_id
        );
        let tag = sent.sent.to_string();
        let status: SendStatus = self
            .sender()
            .get(&path, &[("sentVersionTag", tag.as_str())])
            .map_err(|e| self.send_error(e))?;
        Ok(status.pending)
    }

    fn on_its_way(&self) -> RemoteError {
        RemoteError::Waiting {
            why: format!(
                "waiting for {}'s server to take a change of yours",
                self.owner()
            ),
            retry_after: ASK_AGAIN,
        }
    }

    /// Their node `node`'s header as this identity may read it (by its unique id: one
    /// request to their server), none where it isn't there or isn't shared with it.
    fn their_node(&self, node: NodeId) -> Result<Option<wire::FileHeader>> {
        let path = format!(
            "{}/by-uid/{}/header",
            files_route(self.sharer.drive_alias),
            node.0
        );
        let asked = self.with_token(|client: &Client| {
            client
                .get::<wire::FileHeader>(&path, &[])
                .map_err(|e| self.error(e))
        });
        match asked {
            Ok(header) if header.file_state == "active" => Ok(Some(header)),
            Ok(_) | Err(RemoteError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Their drive's write-only public key, asked of their server through this identity's
    /// once (one request), and kept: a drive's key pair is made once.
    fn their_point(&self) -> Result<Point> {
        if let Some(point) = &*locked(&self.writes.point) {
            return Ok(point.clone());
        }
        let path = format!(
            "/api/v2/peer/{}/apps/{APP_SLUG}/drives/{}/public-key",
            self.owner(),
            SYNC_DRIVE.slug
        );
        let answer: PublicKey = self
            .sender()
            .get(&path, &[])
            .map_err(|e| self.send_error(e))?;
        let point = Point::from_jwk(&answer.public_key_jwk).map_err(remote_error)?;
        *locked(&self.writes.point) = Some(point.clone());
        Ok(point)
    }

    /// This identity's own server's refusal of a send, as the trait's error.
    fn send_error(&self, e: HomebaseError) -> RemoteError {
        match &e {
            // Its staged copy carries the node's id, one per drive: an earlier send of the
            // node is in the outbox still (**[verified]** live).
            HomebaseError::Api { error, .. } if error.is("existingFileWithUniqueId") => {
                self.on_its_way()
            }
            _ if not_connected(&e) => RemoteError::Waiting {
                why: format!(
                    "not connected to {}: what they share with you can't be changed until \
                     you are",
                    self.owner()
                ),
                retry_after: NOT_CONNECTED_WAIT,
            },
            _ if codeless_forbidden(&e) => RemoteError::Fatal(MAY_NOT_SEND.into()),
            _ => remote_error(e),
        }
    }
}

impl Writes {
    /// The send of `node` not known to have ended, if it wrote `what`.
    fn earlier(&self, node: NodeId, what: &str) -> Option<OnItsWay> {
        locked(&self.on_their_way)
            .get(&node)
            .filter(|sent| sent.what == what)
            .cloned()
    }

    fn sent(&self, node: NodeId, sent: OnItsWay) {
        locked(&self.on_their_way).insert(node, sent);
    }

    fn ended(&self, node: NodeId) {
        locked(&self.on_their_way).remove(&node);
    }

    fn is_sealed(&self, global_transit_id: Uuid) -> bool {
        locked(&self.sealed).contains(&global_transit_id)
    }

    fn set_sealed(&self, global_transit_id: Uuid, sealed: bool) {
        let mut files = locked(&self.sealed);
        match sealed {
            true => files.insert(global_transit_id),
            false => files.remove(&global_transit_id),
        };
    }
}

#[cfg(test)]
mod tests {
    use hbsync_remote::Remote;
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{NodeKind, RemoteFileId};

    use super::super::tests::sam;
    use super::*;

    fn result(status: &str, version: Option<Uuid>) -> SendResult {
        SendResult {
            global_transit_id: Uuid::from_u128(1),
            sent_version_tag: Uuid::from_u128(2),
            status: status.into(),
            will_retry: false,
            version_tag: version,
        }
    }

    fn sent() -> OnItsWay {
        OnItsWay {
            global_transit_id: Uuid::from_u128(1),
            sent: Uuid::from_u128(2),
            what: "what".into(),
            content: None,
        }
    }

    fn folder(name: &str, parent: NodeId) -> NodeMeta {
        NodeMeta {
            node: NodeId::new(),
            parent,
            kind: NodeKind::Folder,
            content: NodeContent::folder(name),
            removed: false,
        }
    }

    /// The socket's word on a send, as the write takes it: delivered is their file's version
    /// (the one told, a new file's; else the one sent), a refused key header sends it the
    /// other way, a file changed since is the trait's version mismatch, a share that no
    /// longer gives write is read-only, and anything else says what their server said.
    #[test]
    fn the_sockets_word_is_what_became_of_a_write() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
        let landed = |told: &SendResult| match sam.told(told, &sent()) {
            Ok(Became::Landed(version)) => version.0,
            _ => panic!("{told:?}"),
        };
        assert_eq!(
            landed(&result("delivered", Some(Uuid::from_u128(9)))),
            Uuid::from_u128(9)
        );
        assert_eq!(landed(&result("Delivered", None)), Uuid::from_u128(2));
        assert!(matches!(
            sam.told(
                &result("recipientIdentityReturnedInvalidKeyHeader", None),
                &sent()
            ),
            Ok(Became::OtherKeyHeader)
        ));
        assert!(matches!(
            sam.told(
                &result("recipientIdentityReturnedVersionTagMismatch", None),
                &sent()
            ),
            Err(RemoteError::VersionMismatch)
        ));
        match sam.told(
            &result("recipientIdentityReturnedAccessDenied", None),
            &sent(),
        ) {
            Err(RemoteError::Fatal(why)) => assert!(why.contains("read-only"), "{why}"),
            _ => panic!("taken"),
        }
        match sam.told(
            &result("recipientIdentityReturnedBadRequest", None),
            &sent(),
        ) {
            Err(RemoteError::Fatal(why)) => {
                assert!(
                    why.contains("sam.invalid") && why.contains("BadRequest"),
                    "{why}"
                )
            }
            _ => panic!("taken"),
        }
        Ok(())
    }

    /// A write sent whose end isn't known is found again by its node and what it wrote, not
    /// by another write of the node; a file's key header is sent sealed once their server
    /// refused it kept, and kept again once it refused it sealed, which also asks for their
    /// drive's key anew.
    #[test]
    fn what_its_writes_keep_between_them() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
        let node = NodeId::new();
        assert!(sam.writes.earlier(node, "what").is_none());
        sam.writes.sent(node, sent());
        assert_eq!(
            sam.writes.earlier(node, "what").map(|s| s.sent),
            Some(Uuid::from_u128(2))
        );
        assert!(sam.writes.earlier(node, "another").is_none());
        assert!(sam.writes.earlier(NodeId::new(), "what").is_none());
        sam.writes.ended(node);
        assert!(sam.writes.earlier(node, "what").is_none());

        let a = folder("A", NodeId::ROOT);
        let mut renamed = a.clone();
        renamed.content = NodeContent::folder("B");
        let (mut moved, mut removed) = (a.clone(), a.clone());
        moved.parent = NodeId::new();
        removed.removed = true;
        let whats = [
            what(&a, false),
            what(&renamed, false),
            what(&moved, false),
            what(&removed, false),
            what(&a, true),
        ];
        for (i, one) in whats.iter().enumerate() {
            assert!(whats.iter().skip(i + 1).all(|other| other != one), "{one}");
        }

        let file = Uuid::from_u128(5);
        let point = Point::from_jwk(crate::drive::share_keys::tests::WRITE_ONLY_JWK)?;
        *locked(&sam.writes.point) = Some(point);
        assert!(!sam.writes.is_sealed(file));
        sam.other_key_header(file, false);
        assert!(sam.writes.is_sealed(file) && locked(&sam.writes.point).is_some());
        sam.other_key_header(file, true);
        assert!(!sam.writes.is_sealed(file) && locked(&sam.writes.point).is_none());
        Ok(())
    }

    /// A share written to still deletes nothing there, and adds nothing at its top: both
    /// refuse before any request (their server and this identity's can't be reached here).
    #[test]
    fn a_share_written_to_deletes_nothing_and_adds_nothing_at_its_top()
    -> std::result::Result<(), HomebaseError> {
        let sam = sam()?.writing();
        sam.own.drive_key.lock().sends = true;
        for refused in [
            sam.delete(&[]).map(drop),
            sam.delete_for_good(&[]).map(drop),
        ] {
            match refused {
                Err(RemoteError::Fatal(why)) => assert!(why.contains("read-only"), "{why}"),
                other => panic!("{other:?}"),
            }
        }
        match sam.create(&folder("New", NodeId::ROOT), None) {
            Err(RemoteError::Fatal(why)) => assert!(why.contains("nothing can be added"), "{why}"),
            other => panic!("{other:?}"),
        }
        // A file too large costs nothing either.
        let target = Target {
            at: RemoteRef {
                file_id: RemoteFileId(Uuid::new_v4()),
                version: VersionTag(Uuid::new_v4()),
            },
            token: None,
        };
        let mut none = std::io::empty();
        let too_large = Upload {
            hash: hbsync_remote::Planned::OnSend {
                algorithm: hbsync_types::HashAlgorithm::Blake3,
                unchanged: &|| true,
            },
            size: u64::MAX,
            reader: &mut none,
            pace: None,
        };
        match sam.update(&target, &folder("New", NodeId::new()), Some(too_large)) {
            Err(RemoteError::Fatal(why)) => assert!(why.contains("too large"), "{why}"),
            other => panic!("{other:?}"),
        }
        Ok(())
    }
}
