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

use hbsync_remote::{
    Answers, ErrorClass, NodeMeta, RemoteBody, RemoteError, RemoteHeader, Result, Upload,
};
use hbsync_types::schema::ARCHIVAL_REMOVED;
use hbsync_types::{ContentHash, NodeId, RemoteFileId, RemoteRef, Target, VersionTag, Written};
use serde::Deserialize;
use uuid::Uuid;

use super::super::{
    Sending, check_size, files_route, finish, lacks_permission, not_connected, not_connected_to,
    parts, remote_error, wire,
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
/// seconds in all, then it says the write is on its way ([`RemoteError::OnItsWay`]). The word comes in
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

/// A send: the file's global transit id, the same on both servers, and the version its
/// answer gave.
#[derive(Debug, Clone, Copy)]
struct Sent {
    global_transit_id: Uuid,
    version: Uuid,
}

/// A write sent, its end not known.
#[derive(Debug, Clone)]
struct OnItsWay {
    sent: Sent,
    /// What was written, with bytes or not, over which version of their file (none: a new
    /// one): another write of the node is another send.
    meta: NodeMeta,
    bytes: bool,
    over: Option<VersionTag>,
    /// The hash of the bytes sent, if any.
    content: Option<ContentHash>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Their file as written from here: nothing of theirs has a token.
fn written(
    file_id: RemoteFileId,
    version: VersionTag,
    changed_at: Option<i64>,
    content: Option<ContentHash>,
) -> Written {
    Written {
        target: Target {
            at: RemoteRef { file_id, version },
            token: None,
        },
        changed_at,
        content,
    }
}

/// What moving their file to their Recently deleted, or back, takes.
#[derive(Debug)]
enum Removal {
    /// Nothing: it is so already, at the version asked.
    AsItIs(Written),
    /// Its header, sent with the removal changed.
    Sent(NodeMeta),
}

/// What it takes to have their file `read` (as it is now) `removed` or not, asked for its
/// `version`. One at another version is sent all the same, for [`PeerRemote::change`] to
/// say: a version mismatch, or the removal this run sent before, landed since.
fn removal(read: RemoteHeader, version: VersionTag, removed: bool) -> Result<Removal> {
    let RemoteBody::Live { node, content, .. } = read.body else {
        return Err(RemoteError::Deleted);
    };
    if read.removed == removed && read.version == version {
        let as_it_is = written(read.file_id, read.version, read.changed_at, None);
        return Ok(Removal::AsItIs(as_it_is));
    }
    Ok(Removal::Sent(NodeMeta {
        node,
        parent: read.parent,
        kind: read.kind,
        content,
        removed,
    }))
}

/// Whether `e`, the answer to one file of several, isn't that file's own: it is tried again
/// later, or lifted by someone, for all of them alike.
fn stops(e: &RemoteError) -> bool {
    matches!(
        e.class(),
        ErrorClass::Transient | ErrorClass::RateLimited | ErrorClass::Denied | ErrorClass::Full
    )
}

/// Their node `theirs`, found where a new one was `sent` (`meta`, with the bytes of
/// `content`): whose it is, and at which version it is this write's.
/// - Another send's (by its global transit id): there already, an earlier send of the node
///   that landed, whose bytes may be others than this one's
///   ([`RemoteError::AlreadyExists`]: the engine reads it).
/// - This send's, and the socket told the version it `landed` as: written, at that version,
///   whatever their file is at by now. (Unless that version is their file's now and the file
///   is in their Recently deleted where it wasn't sent so, or the other way round: told again
///   of a send that had landed before, their server names the version such a move gave the
///   file since, **[verified]** odin-core efc317ad4 `PeerFileWriter`, `CarrySendersWrite`.
///   That is no word of this write's version.)
/// - This send's, with no word of it: a new file's version is of their server's making and
///   in no answer of this identity's (**[verified]** odin-core efc317ad4, `PeerFileWriter`:
///   `CreateVersionTag`), so the one their file has now is this write's only while the file
///   is as it was sent (`read`: their file decoded, if it can be). One its owner changed
///   since is there already, for the engine to read: said written at that version, the
///   owner's change would count as synced, and never come down.
fn added(
    theirs: wire::FileHeader,
    sent: Sent,
    landed: Option<VersionTag>,
    read: impl FnOnce(wire::FileHeader) -> Result<Option<RemoteHeader>>,
    meta: &NodeMeta,
    content: Option<ContentHash>,
) -> Result<Written> {
    if theirs.file_metadata.global_transit_id != Some(sent.global_transit_id) {
        return Err(RemoteError::AlreadyExists);
    }
    let file_id = RemoteFileId(theirs.file_id);
    let stored = &theirs.file_metadata;
    let moved = (stored.app_data.archival_status == ARCHIVAL_REMOVED) != meta.removed;
    let landed = landed.filter(|told| !(moved && told.0 == stored.version_tag));
    let version = match landed {
        Some(version) => version,
        None => match read(theirs)? {
            Some(now) if as_sent(&now, meta, content) => now.version,
            _ => return Err(RemoteError::AlreadyExists),
        },
    };
    Ok(written(file_id, version, None, content))
}

/// Whether their file `now` is as it was sent: `meta`, with the bytes of `content`.
fn as_sent(now: &RemoteHeader, meta: &NodeMeta, content: Option<ContentHash>) -> bool {
    let RemoteBody::Live {
        node,
        content: theirs,
        payload,
    } = &now.body
    else {
        return false;
    };
    *node == meta.node
        && now.parent == meta.parent
        && now.kind == meta.kind
        && now.removed == meta.removed
        && *theirs == meta.content
        && payload.map(|payload| payload.hash) == content
}

/// The version the socket told a send landed as, if it told one.
fn landed(word: Option<&SendResult>) -> Option<VersionTag> {
    let version = word.filter(|told| told.delivered())?.version_tag?;
    Some(VersionTag(version))
}

/// This identity's server's answer to a write for another's drive.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Queued {
    new_version_tag: Uuid,
    /// What became of the send for each recipient: `enqueued`, or `enqueuedFailed` (odin-core
    /// `TransferStatus`), which the answer is a 200 for all the same.
    #[serde(default)]
    recipient_status: HashMap<String, String>,
}

impl Queued {
    /// The send to `owner` of the file with `global_transit_id`, where their server queued
    /// it. One it didn't queue has no word to wait for: asked again later, said so. (It
    /// leaves its staged copy behind all the same, odin-core #2158: the next send of the
    /// node then answers that one is on its way, until that is mended.)
    fn sent(self, owner: &str, global_transit_id: Uuid) -> Result<Sent> {
        let status = self
            .recipient_status
            .iter()
            .find(|(recipient, _)| recipient.eq_ignore_ascii_case(owner));
        match status {
            Some((_, status)) if !status.eq_ignore_ascii_case("enqueued") => {
                Err(RemoteError::Waiting {
                    why: format!(
                        "your Homebase cloud couldn't send a change of yours on to {owner} \
                         ({status})"
                    ),
                    retry_after: ASK_AGAIN,
                })
            }
            _ => Ok(Sent {
                global_transit_id,
                version: self.new_version_tag,
            }),
        }
    }
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
enum Became<T> {
    /// The socket told that their server took it, and with it (where the word has one) the
    /// version their file got by it. A new file's is in that word alone.
    Told(Option<VersionTag>),
    /// Their file, looked at, is what was sent.
    Seen(T),
    /// Still in this identity's outbox.
    OnItsWay,
    /// Their server didn't take it: it refused its key header (the file's key is sealed to
    /// their drive still, or no longer); or it is gone from the outbox and their file isn't
    /// what was sent (refused there unheard, as no socket told, or lost).
    NotTaken,
}

impl PeerRemote {
    /// Refuses a write where the app may not send to other identities: before anything is
    /// sent, as a send refused for it has uploaded its bytes for nothing.
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
        let header = self.their_header(target.at.file_id)?;
        self.change(&header, target.at.version, meta, payload)
    }

    /// Their files `files` moved to their Recently deleted, or back
    /// ([`hbsync_remote::Remote::set_removed`]): each a change of its header alone, one after
    /// another (a header read, a send and its word for each: there is no batch for a member).
    /// An answer that isn't one file's own (their server or this one not reached, not
    /// connected, a send still on its way) stops it there: the files left would each meet
    /// the same, after their own wait.
    pub(super) fn set_theirs_removed(
        &self,
        files: &[Target],
        removed: bool,
    ) -> Result<Answers<Written>> {
        self.may_send()?;
        let one = |target: &Target| -> Result<Written> {
            let header = self.their_header(target.at.file_id)?;
            let read = self
                .to_remote(&self.client(), header.clone())?
                .map_err(RemoteError::Fatal)?;
            match removal(read, target.at.version, removed)? {
                Removal::AsItIs(written) => Ok(written),
                Removal::Sent(meta) => self.change(&header, target.at.version, &meta, None),
            }
        };
        let mut answers = Vec::with_capacity(files.len());
        for target in files {
            match one(target) {
                Err(e) if stops(&e) => {
                    return Ok(Answers {
                        answers,
                        stopped: Some(e),
                    });
                }
                answer => answers.push(answer),
            }
        }
        Ok(Answers::all(answers))
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
        let file_id = RemoteFileId(header.file_id);
        let global_transit_id = stored.global_transit_id.ok_or_else(|| {
            RemoteError::Fatal(format!("file {}: no global transit id", header.file_id))
        })?;
        let bytes = payload.is_some();
        let known = payload.as_ref().and_then(|p| p.hash.known());
        // This very write, sent before: landed since, still on its way, or gone.
        if let Some(earlier) = self.writes.earlier(meta, (bytes, known), Some(version)) {
            let word = self.heard(earlier.sent);
            let landed = match &word {
                Some(told) => told.delivered(),
                None => stored.version_tag == earlier.sent.version,
            };
            if landed {
                self.writes.ended(meta.node);
                let version = VersionTag(earlier.sent.version);
                return Ok(written(file_id, version, None, earlier.content));
            }
            if word.is_none() && stored.version_tag == version.0 && self.pending(earlier.sent)? {
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
        let path = self.by_gtid(global_transit_id);
        let mut sending = Sending::of(payload, &key)?;
        // A header alone may go twice: with the key kept, and where their server holds it
        // sealed to their drive still, sealed (or the other way round). Bytes are read once:
        // they go again at the engine's next try.
        for _ in 0..2 {
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
            let sent = queued.sent(self.owner(), global_transit_id)?;
            let seen = std::cell::Cell::new(None);
            let theirs = || {
                let now = self.their_header(file_id)?.file_metadata.version_tag;
                seen.set(Some(now));
                Ok(Some(VersionTag(now)).filter(|now| now.0 == sent.version))
            };
            match self.became(sent, theirs)? {
                // (A change lands as the version its answer gave: a word without one says
                // no other.)
                Became::Told(told) => {
                    self.writes.ended(meta.node);
                    let version = told.unwrap_or(VersionTag(sent.version));
                    return Ok(written(file_id, version, None, content));
                }
                Became::Seen(version) => {
                    self.writes.ended(meta.node);
                    return Ok(written(file_id, version, None, content));
                }
                Became::OnItsWay => {
                    return Err(self.waits(sent, meta, Some(version), content, bytes));
                }
                // Their file is at another version than the one written over: that, unheard,
                // is why it wasn't taken (or it was, and changed again since).
                Became::NotTaken if seen.get().is_some_and(|now| now != version.0) => {
                    return Err(RemoteError::VersionMismatch);
                }
                // (Unheard, it may have been the key header too.)
                Became::NotTaken => self.other_key_header(global_transit_id, sealed),
            }
            if bytes {
                break;
            }
        }
        Err(RemoteError::Transient(format!(
            "{}'s server didn't take a change to file {file_id:?}: sent again",
            self.owner()
        )))
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
        let bytes = payload.is_some();
        let known = payload.as_ref().and_then(|p| p.hash.known());
        // This very write, sent before. Its file's id is in no word: their file is read
        // either way, after the outbox is asked (it may land between the two).
        if let Some(earlier) = self.writes.earlier(meta, (bytes, known), None) {
            let word = self.heard(earlier.sent);
            let pending = word.is_none() && self.pending(earlier.sent)?;
            match self.their_node(meta.node)? {
                None if pending => return Err(self.on_its_way()),
                None => self.writes.ended(meta.node),
                Some(theirs) => {
                    let landed = landed(word.as_ref());
                    let sent = (earlier.content, bytes);
                    return self.added(theirs, earlier.sent, landed, meta, sent);
                }
            }
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
            let (permissions, ends) = (row.permissions, row.expires_at);
            shares.push(wire::share_of_new(circle, permissions, ends, &locked));
        }
        // Without a share it would be its owner's alone: this identity couldn't read what
        // it added.
        if shares.is_empty() {
            return Err(not_here());
        }
        let key_header = key.sealed_to(&self.their_point()?).map_err(remote_error)?;
        let own = self.sender();
        let mut sending = Sending::of(payload, &key)?;
        let transfer_iv = sealed::strong().map_err(remote_error)?;
        let instructions = wire::deposit_instructions(
            &transfer_iv,
            sending.as_ref().map(|s| &s.descriptor),
            self.owner(),
            &self.their_drive(),
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
        let staged = files_route(wire::TRANSIENT_DRIVE);
        let queued: Queued = own
            .upload(UploadMethod::Post, &staged, form)
            .map_err(|e| self.send_error(e))?;
        let content = sending.as_ref().map(|s| finish(&s.plain).0);
        let sent = queued.sent(self.owner(), global_transit_id)?;
        // Sealed to their drive until a device of theirs locks its key with the drive's.
        self.writes.set_sealed(global_transit_id, true);
        let (theirs, landed) = match self.became(sent, || self.their_node(meta.node)) {
            Ok(Became::Seen(theirs)) => (Some(theirs), None),
            // No word carries the new file's id.
            Ok(Became::Told(landed)) => (self.their_node(meta.node)?, landed),
            Ok(Became::OnItsWay) => return Err(self.waits(sent, meta, None, content, bytes)),
            Ok(Became::NotTaken) => (None, None),
            // Their server refused it: where the node is there, an earlier send of it
            // landed (one from before a restart, or one this run stopped waiting for).
            Err(
                e @ (RemoteError::Fatal(_)
                | RemoteError::ReadOnly(_)
                | RemoteError::VersionMismatch),
            ) => {
                return Err(match self.their_node(meta.node)? {
                    Some(_) => RemoteError::AlreadyExists,
                    None => e,
                });
            }
            Err(e) => return Err(e),
        };
        match theirs {
            Some(theirs) => self.added(theirs, sent, landed, meta, (content, bytes)),
            None => {
                self.writes.ended(meta.node);
                self.other_key_header(global_transit_id, true);
                Err(RemoteError::Transient(format!(
                    "{}'s server didn't take a new file: sent again",
                    self.owner()
                )))
            }
        }
    }

    /// [`added`], their file read with the keys held now where it has to be. With its answer
    /// the send is over; where their file couldn't be read now (a key to be asked for again),
    /// it stays remembered, so the write asked again looks once more and isn't sent, bytes
    /// and all, a second time.
    fn added(
        &self,
        theirs: wire::FileHeader,
        sent: Sent,
        landed: Option<VersionTag>,
        meta: &NodeMeta,
        (content, bytes): (Option<ContentHash>, bool),
    ) -> Result<Written> {
        let read = |theirs| Ok(self.to_remote(&self.client(), theirs)?.ok());
        let answer = added(theirs, sent, landed, read, meta, content);
        match &answer {
            Ok(_) | Err(RemoteError::AlreadyExists) => self.writes.ended(meta.node),
            Err(_) => self.remembered(sent, meta, None, content, bytes),
        }
        answer
    }

    /// What became of `sent`: the socket's word, waited for; where none comes, `theirs` (their
    /// file now, if it is what was sent) looked at between the waits. After the last wait
    /// this identity's outbox is asked, and their file looked at after that: a write may land
    /// between the two.
    fn became<T>(&self, sent: Sent, theirs: impl Fn() -> Result<Option<T>>) -> Result<Became<T>> {
        let sends = self.sender().sends();
        for (n, wait) in WAITS.iter().enumerate() {
            if let Some(told) = sends.wait(sent.global_transit_id, sent.version, *wait) {
                return self.told(&told, sent);
            }
            if n + 1 == WAITS.len() {
                break;
            }
            // (A look that fails is none: the outbox is asked before anything is said.)
            if let Ok(Some(seen)) = theirs() {
                return Ok(Became::Seen(seen));
            }
        }
        if self.pending(sent)? {
            return Ok(Became::OnItsWay);
        }
        Ok(theirs()?.map_or(Became::NotTaken, Became::Seen))
    }

    /// The socket's word on `sent`, if it has come: no request.
    fn heard(&self, sent: Sent) -> Option<SendResult> {
        let sends = self.sender().sends();
        sends.wait(sent.global_transit_id, sent.version, Duration::ZERO)
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
    fn told<T>(&self, told: &SendResult, sent: Sent) -> Result<Became<T>> {
        if told.delivered() {
            return Ok(Became::Told(told.version_tag.map(VersionTag)));
        }
        tracing::debug!(
            "{}'s server didn't take a write of {}: {}",
            self.owner(),
            sent.global_transit_id,
            told.status
        );
        if told.is("recipientIdentityReturnedInvalidKeyHeader") {
            return Ok(Became::NotTaken);
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

    /// The route of their file with `global_transit_id`, on this identity's own server.
    fn by_gtid(&self, global_transit_id: Uuid) -> String {
        format!(
            "/api/v2/peer/{}/drives/{}/files/by-gtid/{global_transit_id}",
            self.owner(),
            self.sharer.drive_alias
        )
    }

    /// Whether `sent` still waits in this identity's outbox: one request.
    fn pending(&self, sent: Sent) -> Result<bool> {
        let path = format!("{}/send-status", self.by_gtid(sent.global_transit_id));
        let tag = sent.version.to_string();
        let status: SendStatus = self
            .sender()
            .get(&path, &[("sentVersionTag", tag.as_str())])
            .map_err(|e| self.send_error(e))?;
        Ok(status.pending)
    }

    /// `sent` is still on its way: remembered, so the write asked again isn't sent again.
    fn waits(
        &self,
        sent: Sent,
        meta: &NodeMeta,
        over: Option<VersionTag>,
        content: Option<ContentHash>,
        bytes: bool,
    ) -> RemoteError {
        self.remembered(sent, meta, over, content, bytes);
        self.on_its_way()
    }

    /// Remembers `sent` as a write whose end isn't known: asked again, it is looked for
    /// before anything is sent.
    fn remembered(
        &self,
        sent: Sent,
        meta: &NodeMeta,
        over: Option<VersionTag>,
        content: Option<ContentHash>,
        bytes: bool,
    ) {
        let on_its_way = OnItsWay {
            sent,
            meta: meta.clone(),
            bytes,
            over,
            content,
        };
        locked(&self.writes.on_their_way).insert(meta.node, on_its_way);
    }

    fn on_its_way(&self) -> RemoteError {
        RemoteError::OnItsWay {
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
            _ if not_connected(&e) => not_connected_to(self.owner(), "changed"),
            // The permission was taken away since it was last read ("Sending, updating or
            // deleting files on a peer requires UseTransitWrite"): not held, as of now, so
            // the next write refuses before it sends anything.
            _ if lacks_permission(&e) => {
                self.own.may_not_send();
                RemoteError::Fatal(MAY_NOT_SEND.into())
            }
            _ => remote_error(e),
        }
    }
}

impl Writes {
    /// The send of `meta`'s node not known to have ended, if it wrote the same: the same
    /// `meta` over the same version, with bytes or not, and where their hash is known
    /// before they are sent (`bytes.1`), the same bytes. (An edit that keeps the file's size
    /// within its time's grain is another write: answered as the earlier one, it was
    /// recorded as sent and never went up.)
    fn earlier(
        &self,
        meta: &NodeMeta,
        (bytes, known): (bool, Option<ContentHash>),
        over: Option<VersionTag>,
    ) -> Option<OnItsWay> {
        let same_bytes = |sent: &OnItsWay| {
            sent.bytes == bytes && known.is_none_or(|hash| sent.content == Some(hash))
        };
        locked(&self.on_their_way)
            .get(&meta.node)
            .filter(|sent| sent.meta == *meta && sent.over == over && same_bytes(sent))
            .cloned()
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
    use hbsync_types::NodeKind;
    use hbsync_types::schema::NodeContent;

    use super::super::tests::sam;
    use super::*;

    const SENT: Sent = Sent {
        global_transit_id: Uuid::from_u128(1),
        version: Uuid::from_u128(2),
    };

    fn result(status: &str, version: Option<Uuid>) -> SendResult {
        SendResult {
            global_transit_id: SENT.global_transit_id,
            sent_version_tag: SENT.version,
            status: status.into(),
            will_retry: false,
            version_tag: version,
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
    /// where the word tells one (a new file's is told so alone), a refused key header sends it the
    /// other way, a file changed since is the trait's version mismatch, a share that no
    /// longer gives write is read-only, and anything else says what their server said.
    #[test]
    fn the_sockets_word_is_what_became_of_a_write() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
        let told = |status: &str, version| sam.told::<()>(&result(status, version), SENT);
        let landed = |status: &str, version| match told(status, version) {
            Ok(Became::Told(version)) => version.map(|version| version.0),
            _ => panic!("{status}"),
        };
        assert_eq!(
            landed("delivered", Some(Uuid::from_u128(9))),
            Some(Uuid::from_u128(9))
        );
        assert_eq!(landed("Delivered", None), None);
        assert!(matches!(
            told("recipientIdentityReturnedInvalidKeyHeader", None),
            Ok(Became::NotTaken)
        ));
        assert!(matches!(
            told("recipientIdentityReturnedVersionTagMismatch", None),
            Err(RemoteError::VersionMismatch)
        ));
        match told("recipientIdentityReturnedAccessDenied", None) {
            Err(RemoteError::ReadOnly(why)) => assert!(why.contains("read-only"), "{why}"),
            _ => panic!("taken"),
        }
        match told("recipientIdentityReturnedBadRequest", None) {
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

    /// A write sent whose end isn't known is found again when the same is written again, not
    /// by another write of the node, and says it is waiting; the word that came for it
    /// meanwhile is heard without a request.
    #[test]
    fn a_write_on_its_way_is_found_again() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
        let a = folder("A", NodeId::ROOT);
        let over = Some(VersionTag(Uuid::from_u128(7)));
        let earlier = |meta: &NodeMeta, bytes, over| sam.writes.earlier(meta, (bytes, None), over);
        assert!(earlier(&a, false, over).is_none());
        let waiting = sam.waits(SENT, &a, over, None, false);
        assert!(
            matches!(waiting, RemoteError::OnItsWay { retry_after, .. } if retry_after == ASK_AGAIN)
        );
        let found = earlier(&a, false, over).expect("found");
        assert_eq!(found.sent.version, SENT.version);

        let (mut renamed, mut moved, mut removed) = (a.clone(), a.clone(), a.clone());
        renamed.content = NodeContent::folder("B");
        moved.parent = NodeId::new();
        removed.removed = true;
        for other in [&renamed, &moved, &removed] {
            assert!(earlier(other, false, over).is_none(), "{other:?}");
        }
        assert!(earlier(&a, true, over).is_none());
        assert!(earlier(&folder("A", NodeId::ROOT), false, over).is_none());
        // The same written over another version of their file (it changed, and back), or as
        // a new one, is another write: the earlier one's word isn't its.
        let later = Some(VersionTag(Uuid::from_u128(8)));
        assert!(earlier(&a, false, later).is_none());
        assert!(earlier(&a, false, None).is_none());

        assert!(sam.heard(SENT).is_none());
        sam.sender().sends().told(result("delivered", None));
        assert!(sam.heard(SENT).is_some_and(|told| told.delivered()));
        sam.writes.ended(a.node);
        assert!(earlier(&a, false, over).is_none());
        Ok(())
    }

    /// This identity's server's answer to a send, as the dev server gave it: queued for the
    /// owner is a send to wait for; not queued is none, and says what the server said. An
    /// answer that names no recipient is taken as queued.
    #[test]
    fn a_send_is_one_only_where_it_was_queued() -> std::result::Result<(), serde_json::Error> {
        let answer = |status: serde_json::Value| -> std::result::Result<Queued, serde_json::Error> {
            serde_json::from_value(serde_json::json!({
                "driveId": Uuid::nil(), "fileId": Uuid::nil(),
                "globalTransitId": SENT.global_transit_id,
                "newVersionTag": SENT.version,
                "payloads": [], "recipientStatus": status,
                "updated": null, "versionUpdated": null,
            }))
        };
        let owner = "pippin.dotyou.cloud";
        for queued in [
            serde_json::json!({ "pippin.dotyou.cloud": "enqueued" }),
            serde_json::json!({ "Pippin.DotYou.Cloud": "Enqueued" }),
            serde_json::json!({}),
        ] {
            let sent = answer(queued)?
                .sent(owner, SENT.global_transit_id)
                .expect("queued");
            assert_eq!(
                (sent.global_transit_id, sent.version),
                (SENT.global_transit_id, SENT.version)
            );
        }
        let failed = answer(serde_json::json!({ "pippin.dotyou.cloud": "enqueuedFailed" }))?;
        match failed.sent(owner, SENT.global_transit_id) {
            Err(RemoteError::Waiting { why, retry_after }) => {
                assert!(
                    why.contains(owner) && why.contains("enqueuedFailed"),
                    "{why}"
                );
                assert_eq!(retry_after, ASK_AGAIN);
            }
            other => panic!("{other:?}"),
        }
        Ok(())
    }

    fn hash(bytes: &[u8]) -> ContentHash {
        hbsync_fileio::hash_bytes(bytes, hbsync_types::HashAlgorithm::Blake3)
    }

    /// A new file as it is sent: named "mine", its bytes `x`.
    fn sent_file() -> (NodeMeta, Option<ContentHash>) {
        let meta = NodeMeta {
            node: NodeId(Uuid::from_u128(22)),
            parent: NodeId(Uuid::from_u128(23)),
            kind: NodeKind::File,
            content: NodeContent::file("mine", 5, 1, false),
            removed: false,
        };
        (meta, Some(hash(b"x")))
    }

    /// Their file of `meta` as it is read: at `version`, named `name`, with `bytes`.
    fn their_file(meta: &NodeMeta, version: u128, name: &str, bytes: &[u8]) -> RemoteHeader {
        RemoteHeader {
            file_id: RemoteFileId(Uuid::from_u128(20)),
            version: VersionTag(Uuid::from_u128(version)),
            parent: meta.parent,
            kind: meta.kind,
            body: RemoteBody::Live {
                node: meta.node,
                content: NodeContent::file(name, 5, 1, false),
                payload: Some(hbsync_remote::PayloadInfo {
                    hash: hash(bytes),
                    bytes: 16,
                }),
            },
            removed: false,
            changed_at: Some(7),
            token: None,
            expires: Some(9),
            writable: true,
        }
    }

    /// Their file as their server answers it to a member: at version 21, with
    /// `global_transit_id`, its key for a share to open (and no share row that names one).
    fn their_header(global_transit_id: Option<Uuid>) -> wire::FileHeader {
        serde_json::from_value(serde_json::json!({
            "fileId": Uuid::from_u128(20), "fileState": "active",
            "fileMetadata": {
                "isEncrypted": true, "versionTag": Uuid::from_u128(21),
                "globalTransitId": global_transit_id,
                "appData": { "uniqueId": Uuid::from_u128(22), "groupId": Uuid::from_u128(23),
                             "fileType": hbsync_types::schema::FILE_TYPE_FILE, "content": "" },
            },
            "sharedSecretEncryptedKeyHeader": {
                "encryptionVersion": 1, "type": "share", "iv": "", "encryptedAesKey": "",
            },
        }))
        .expect("a header")
    }

    /// A node found on their drive where a new one was sent is that send's only by its global
    /// transit id: with another's, or none, it was there already (an earlier send's, with
    /// bytes that may be others), which the engine is told as such. This send's is written
    /// at the version the socket told it landed as, whatever their file is at by now; with no
    /// word, at the version their file has, and only while that file is as it was sent: one
    /// its owner changed since (or that can't be read) is there already too.
    #[test]
    fn a_new_file_is_this_sends_at_the_version_it_landed_as() {
        let theirs = their_header;
        let (meta, content) = sent_file();
        let mine = || theirs(Some(SENT.global_transit_id));
        let unread = |_| -> Result<Option<RemoteHeader>> { panic!("their file was read") };
        let at = |answer: Result<Written>| answer.map(|written| written.target.at.version.0);

        // Told: at that version, their file not read, though it is at another by now.
        let told = Some(VersionTag(Uuid::from_u128(40)));
        let written = added(mine(), SENT, told, unread, &meta, content).expect("this send's");
        assert_eq!(written.target.at.file_id, RemoteFileId(Uuid::from_u128(20)));
        assert_eq!(written.target.at.version, VersionTag(Uuid::from_u128(40)));
        assert_eq!(
            (written.content, written.target.token, written.changed_at),
            (content, None, None)
        );

        // No word: their file's version, while it is as it was sent.
        let as_it_was = |_| Ok(Some(their_file(&meta, 21, "mine", b"x")));
        let answer = at(added(mine(), SENT, None, as_it_was, &meta, content));
        assert_eq!(answer.ok(), Some(Uuid::from_u128(21)));
        // Changed by its owner since (other bytes, another name), or not to be read: there
        // already, not written at the owner's version.
        for theirs_now in [
            Some(their_file(&meta, 41, "mine", b"theirs")),
            Some(their_file(&meta, 41, "renamed", b"x")),
            None,
        ] {
            let answer = added(mine(), SENT, None, |_| Ok(theirs_now), &meta, content);
            assert!(
                matches!(answer, Err(RemoteError::AlreadyExists)),
                "{answer:?}"
            );
        }
        // Told the version their file has now, where the file is in their Recently deleted
        // and wasn't sent so: a word told again after its owner moved it there, and not this
        // write's version. Their file is read, and found changed.
        let told_again = Some(VersionTag(Uuid::from_u128(21)));
        let mut moved = mine();
        moved.file_metadata.app_data.archival_status = ARCHIVAL_REMOVED;
        let mut read_as = their_file(&meta, 21, "mine", b"x");
        read_as.removed = true;
        let answer = added(
            moved,
            SENT,
            told_again,
            |_| Ok(Some(read_as)),
            &meta,
            content,
        );
        assert!(
            matches!(answer, Err(RemoteError::AlreadyExists)),
            "{answer:?}"
        );
        // (Told their file's version now, the file as it was sent: that version, unread.)
        let answer = at(added(mine(), SENT, told_again, unread, &meta, content));
        assert_eq!(answer.ok(), Some(Uuid::from_u128(21)));

        // A read that failed is the answer: asked again.
        let failed = |_| Err(RemoteError::Transient("cut".into()));
        let answer = added(mine(), SENT, None, failed, &meta, content);
        assert!(
            matches!(answer, Err(RemoteError::Transient(_))),
            "{answer:?}"
        );

        // Another send's, or none's: there already, whatever was told, their file not read.
        for others in [Some(Uuid::from_u128(99)), None] {
            let answer = added(theirs(others), SENT, told, unread, &meta, content);
            assert!(
                matches!(answer, Err(RemoteError::AlreadyExists)),
                "{answer:?}"
            );
        }
    }

    /// The same file written again with other bytes is another write, though its name, its
    /// size and its time are the same: their hash is known before they are sent, and it
    /// isn't the one that was. (Hashed as it is sent, it isn't known: the write found is
    /// the one asked, as far as can be told.)
    #[test]
    fn a_write_of_other_bytes_isnt_the_one_on_its_way() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
        let (meta, content) = sent_file();
        assert!(content.is_some());
        let over = Some(VersionTag(Uuid::from_u128(7)));
        let _ = sam.waits(SENT, &meta, over, content, true);
        let other = hash(b"other bytes");
        let earlier = |known| sam.writes.earlier(&meta, (true, known), over);
        assert!(earlier(content).is_some());
        assert!(earlier(None).is_some());
        assert!(earlier(Some(other)).is_none());
        Ok(())
    }

    /// With the answer to a new file the send is over. Where their file couldn't be read
    /// (here: its header names no circle whose key opens it), the send stays remembered: the
    /// write asked again looks once more, and isn't sent a second time.
    #[test]
    fn a_new_files_send_is_over_with_its_answer() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
        let (meta, content) = sent_file();
        let remembers = || sam.writes.earlier(&meta, (true, None), None).is_some();
        let mine = || their_header(Some(SENT.global_transit_id));
        // Their file not read: remembered, as the send it was. (The whole share's wait: their
        // key isn't opened yet.)
        let unread = sam.added(mine(), SENT, None, &meta, (content, true));
        assert!(
            matches!(unread, Err(RemoteError::Waiting { .. })),
            "{unread:?}"
        );
        assert!(remembers());
        let kept = sam
            .writes
            .earlier(&meta, (true, None), None)
            .expect("remembered");
        assert_eq!(
            (kept.sent.global_transit_id, kept.sent.version, kept.content),
            (SENT.global_transit_id, SENT.version, content)
        );
        // Answered (told the version; another send's): over.
        let told = Some(VersionTag(Uuid::from_u128(40)));
        sam.added(mine(), SENT, told, &meta, (content, true))
            .expect("written");
        assert!(!remembers());
        sam.remembered(SENT, &meta, None, content, true);
        let others = sam.added(their_header(None), SENT, told, &meta, (content, true));
        assert!(
            matches!(others, Err(RemoteError::AlreadyExists)),
            "{others:?}"
        );
        assert!(!remembers());
        Ok(())
    }

    /// Their file is as it was sent by its node, folder, kind, header and bytes, and by
    /// whether it is in their Recently deleted: any of them changed, it is not.
    #[test]
    fn a_file_is_as_it_was_sent_by_all_of_it() {
        let (meta, sent) = sent_file();
        let theirs = || their_file(&meta, 4, "mine", b"x");
        assert!(as_sent(&theirs(), &meta, sent));
        let other = |h: &mut RemoteHeader, node: u128, name: &str| {
            let RemoteBody::Live { payload, .. } = h.body.clone() else {
                panic!("{h:?}");
            };
            h.body = RemoteBody::Live {
                node: NodeId(Uuid::from_u128(node)),
                content: NodeContent::file(name, 5, 1, false),
                payload,
            };
        };
        let changed: [&dyn Fn(&mut RemoteHeader); 7] = [
            &|h| other(h, 99, "mine"),
            &|h| other(h, 22, "renamed"),
            &|h| h.parent = NodeId(Uuid::from_u128(8)),
            &|h| h.removed = true,
            &|h| h.kind = NodeKind::Folder,
            &|h| h.body = RemoteBody::Deleted,
            &|h| {
                if let RemoteBody::Live { payload, .. } = &mut h.body {
                    *payload = None;
                }
            },
        ];
        for change in changed {
            let mut now = theirs();
            change(&mut now);
            assert!(!as_sent(&now, &meta, sent), "{now:?}");
        }
        assert!(!as_sent(&theirs(), &meta, Some(hash(b"y"))));
        assert!(!as_sent(&theirs(), &meta, None));
        // A folder has no bytes on either side.
        let folder = NodeMeta {
            kind: NodeKind::Folder,
            content: NodeContent::folder("ours"),
            ..meta.clone()
        };
        let mut theirs_folder = theirs();
        theirs_folder.kind = NodeKind::Folder;
        theirs_folder.body = RemoteBody::Live {
            node: folder.node,
            content: folder.content.clone(),
            payload: None,
        };
        assert!(as_sent(&theirs_folder, &folder, None));
    }

    /// The version a send landed as is the one the socket told of its delivery: a word of a
    /// refusal, or one without a version, tells none.
    #[test]
    fn only_a_delivery_tells_the_version_landed_as() {
        let version = Uuid::from_u128(40);
        assert_eq!(
            landed(Some(&result("delivered", Some(version)))),
            Some(VersionTag(version))
        );
        assert_eq!(landed(Some(&result("delivered", None))), None);
        let refused = result("recipientIdentityReturnedAccessDenied", Some(version));
        assert_eq!(landed(Some(&refused)), None);
        assert_eq!(landed(None), None);
    }

    /// Moving their file to their Recently deleted, or back: nothing is sent where it is so
    /// already at the version asked; at another version it is sent (and refused there as
    /// changed, or found to be this run's own removal, landed); a tombstone is gone.
    #[test]
    fn a_removal_looks_at_the_version_first() {
        let asked = VersionTag(Uuid::from_u128(30));
        let read = |version: VersionTag, removed: bool, body: Option<RemoteBody>| RemoteHeader {
            file_id: RemoteFileId(Uuid::from_u128(31)),
            version,
            parent: NodeId::ROOT,
            kind: NodeKind::Folder,
            body: body.unwrap_or(RemoteBody::Live {
                node: NodeId::ROOT,
                content: NodeContent::folder("A"),
                payload: None,
            }),
            removed,
            changed_at: Some(5),
            token: None,
            expires: None,
            writable: false,
        };
        match removal(read(asked, true, None), asked, true) {
            Ok(Removal::AsItIs(written)) => {
                assert_eq!(
                    (written.target.at.version, written.changed_at),
                    (asked, Some(5))
                )
            }
            other => panic!("{other:?}"),
        }
        let other_version = VersionTag(Uuid::from_u128(32));
        for (now, was_removed, removed) in [
            (other_version, true, true),
            (asked, false, true),
            (asked, true, false),
            (other_version, false, true),
        ] {
            match removal(read(now, was_removed, None), asked, removed) {
                Ok(Removal::Sent(meta)) => {
                    assert_eq!((meta.removed, meta.content.name.as_str()), (removed, "A"))
                }
                other => panic!("{other:?}"),
            }
        }
        assert!(matches!(
            removal(read(asked, true, Some(RemoteBody::Deleted)), asked, true),
            Err(RemoteError::Deleted)
        ));
    }

    /// A file's key header is sent sealed once their server refused it kept, and kept again
    /// once it refused it sealed, which also asks for their drive's key anew.
    #[test]
    fn a_refused_key_header_goes_the_other_way() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
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

    /// This identity's server's refusal of a send for want of the permission (a 403 with the
    /// code `missingPermissionKey`) is taken as that: the permission counts as not held from
    /// then on, and the write says why. Another 403, or that code on another status, is not
    /// that.
    #[test]
    fn a_send_refused_for_the_permission_is_remembered() -> std::result::Result<(), HomebaseError> {
        use crate::drive::tests::api;
        let sam = sam()?;
        sam.own.drive_key.lock().sends = true;
        match sam.send_error(api(403, Some("missingPermissionKey"))) {
            RemoteError::Fatal(why) => assert_eq!(why, MAY_NOT_SEND),
            other => panic!("{other:?}"),
        }
        assert!(!sam.own.drive_key.lock().sends);
        // Another refusal says what it is, and the permission counts as held still.
        for other in [
            api(403, None),
            api(403, Some("notReviewedByOwner")),
            api(400, Some("missingPermissionKey")),
            api(401, None),
        ] {
            sam.own.drive_key.lock().sends = true;
            let answer = sam.send_error(other);
            assert!(
                !matches!(&answer, RemoteError::Fatal(why) if why == MAY_NOT_SEND),
                "{answer:?}"
            );
            assert!(sam.own.drive_key.lock().sends);
        }
        // An earlier send of the node staged still: this write's own wait. Not connected:
        // the whole share's.
        let staged = sam.send_error(api(400, Some("existingFileWithUniqueId")));
        assert!(matches!(staged, RemoteError::OnItsWay { .. }), "{staged:?}");
        let apart = sam.send_error(api(400, Some("notAConnectedIdentity")));
        assert!(matches!(apart, RemoteError::Waiting { .. }), "{apart:?}");
        Ok(())
    }

    /// Of the answers to several files, the ones that aren't a file's own stop the rest.
    #[test]
    fn what_isnt_one_files_own_stops_the_rest() -> std::result::Result<(), HomebaseError> {
        let sam = sam()?;
        for stopping in [
            sam.on_its_way(),
            not_connected_to("sam.invalid", "changed"),
            RemoteError::Transient("no route".into()),
        ] {
            assert!(stops(&stopping), "{stopping}");
        }
        for its_own in [
            RemoteError::VersionMismatch,
            RemoteError::Deleted,
            RemoteError::NotFound,
            sam.read_only(),
        ] {
            assert!(!stops(&its_own), "{its_own}");
        }
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
                Err(RemoteError::ReadOnly(why)) => assert!(why.contains("read-only"), "{why}"),
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
