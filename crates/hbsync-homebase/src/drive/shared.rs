//! What another identity shares with this one, read straight from their server: a read-only
//! [`Remote`] over the files of theirs shared through a circle this identity is in, as their
//! server answers a member, to the token it made for this identity's connection
//! (`docs/spec/homebase-api-notes.md`, "Shared with you"). Each file's key is locked with the
//! circle's key, which their devices seal to this identity's drive
//! (`docs/spec/encryption.md`, "Files shared with you").

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use hbsync_remote::{
    Answers, ChangesPage, Deletes, ForGood, Holds, NodeMeta, RefsPage, Refusal, Remote,
    RemoteError, RemoteHeader, Result, Upload,
};
use hbsync_types::schema::PAYLOAD_KEY;
use hbsync_types::{Doomed, NodeId, RemoteFileId, Target, Written};
use uuid::Uuid;

use super::markers::Sharer;
use super::sealed::CipherRange;
use super::share_keys::CircleKey;
use super::wire::{self, FileHeader, Keys};
use super::{
    HOLD_BACK, HomebaseRemote, SHARER_WAIT, TargetDrive, changes_page, decrypt, feed_page, fetch,
    file_route, holding, payload_key, plain_size, query_batch, raw_header, refs_page, remote_error,
};
use crate::app::SYNC_DRIVE;
use crate::notify::{Events, Notifier};
use crate::peer::{self, PeerToken};
use crate::{Client, HomebaseError};

/// How long after a new token was asked for, in place of one their server refused, the next
/// is: each stays a row on their server, so a server that refuses every one it makes gets one
/// an hour, not one a request.
const RENEW_AFTER: Duration = Duration::from_secs(3600);

/// Where a new token comes from when theirs is refused ([`PeerRemote::renewed`]): asks for
/// one, and keeps it ([`new_client`]).
struct Renewal(Box<dyn Fn() -> Result<Client> + Send + Sync>);

impl std::fmt::Debug for Renewal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Renewal(..)")
    }
}

/// Their server with the token held, and when a new one was last asked for in place of one
/// it refused.
#[derive(Debug)]
struct Held {
    client: Arc<Client>,
    renewed: Option<Instant>,
}

/// One sharer's files shared with this identity, on their server.
#[derive(Debug)]
pub struct PeerRemote {
    /// Their server, with the token it made for this identity: replaced by one with a new
    /// token when they refuse this one's ([`PeerRemote::renewed`]). An answer is opened with
    /// the client that asked: its token's secret is the key.
    held: RwLock<Held>,
    /// This identity's own drive, whose server asks theirs for a circle's key, and whose
    /// write-only key opens it.
    own: Arc<HomebaseRemote>,
    sharer: Sharer,
    /// The keys of their circles opened so far: one request each, the first time a file shared
    /// through it is read. A circle's key is set once (odin-core `V2CircleShareKeyController`,
    /// **[verified]**), so one kept stays good; only its developer reset (`ResetShareKey`)
    /// replaces it, and a key kept from before that opens the files locked since only after a
    /// restart.
    circles: Mutex<HashMap<Uuid, Arc<CircleKey>>>,
    /// `None`: a refused token stays refused ([`PeerRemote::new`]).
    renewal: Option<Renewal>,
    /// Whether this identity writes to what they share ([`PeerRemote::writing`]): where it
    /// doesn't, every write refuses before a request.
    writing: bool,
    /// What its writes keep between them.
    writes: writes::Writes,
}

/// `sharer`'s server with a new token asked for through `own`
/// ([`HomebaseRemote::peer_token`]), kept at `kept`: one that makes no client too (a secret
/// that isn't a key), which says when it was asked for ([`PeerRemote::open`]). One that
/// couldn't be kept is used all the same (told): refusing it would ask for another at the
/// next try, a row on their server each time.
fn new_client(own: &HomebaseRemote, sharer: &str, kept: &Path) -> Result<Client> {
    let token = own.peer_token(sharer)?;
    if let Err(e) = token.keep(own.client(), kept) {
        tracing::warn!(
            "the token {sharer}'s server gave couldn't be kept ({e}): a new one is asked for \
             at the next start"
        );
    }
    Client::for_peer(sharer, &token).map_err(remote_error)
}

/// Whether the file at `kept` was written within [`RENEW_AFTER`].
fn kept_lately(kept: &Path) -> bool {
    let written = std::fs::metadata(kept).and_then(|file| file.modified());
    written
        .ok()
        .and_then(|at| at.elapsed().ok())
        .is_some_and(|age| age < RENEW_AFTER)
}

impl PeerRemote {
    /// `sharer`'s files shared with `own`'s identity, read with `token` (made by their server
    /// for this identity: [`PeerToken::new`]). No request yet.
    pub fn new(
        own: Arc<HomebaseRemote>,
        sharer: Sharer,
        token: &PeerToken,
    ) -> Result<PeerRemote, HomebaseError> {
        let client = Client::for_peer(&sharer.odin_id, token)?;
        Ok(PeerRemote::with(own, sharer, client, None))
    }

    /// As [`PeerRemote::new`], on `client`; `renewed`: when its token was asked for, if just
    /// now.
    fn with(
        own: Arc<HomebaseRemote>,
        sharer: Sharer,
        client: Client,
        renewed: Option<Instant>,
    ) -> PeerRemote {
        PeerRemote {
            held: RwLock::new(Held {
                client: Arc::new(client),
                renewed,
            }),
            own,
            sharer,
            circles: Mutex::default(),
            renewal: None,
            writing: false,
            writes: writes::Writes::default(),
        }
    }

    /// `sharer`'s files shared with `own`'s identity, read with the token kept at `kept`
    /// ([`PeerToken::kept`]) or, where none is that opens under this login, with a new one
    /// asked for and kept there ([`new_client`]): one request then, none otherwise. A token
    /// their server refuses later is replaced the same way ([`PeerRemote::renewed`]); one
    /// asked for here counts as asked for just now, so one refused at once isn't replaced
    /// within [`RENEW_AFTER`]. One kept that makes no client fails the opening for that long
    /// after it was asked for, and is asked for again then: their server is given no row a
    /// try for the tokens it gets wrong.
    pub fn open(own: Arc<HomebaseRemote>, sharer: Sharer, kept: &Path) -> Result<PeerRemote> {
        let token = PeerToken::kept(own.client(), kept).map_err(remote_error)?;
        let held = match token.map(|token| Client::for_peer(&sharer.odin_id, &token)) {
            Some(Ok(client)) => Some(client),
            Some(Err(e)) if kept_lately(kept) => return Err(remote_error(e)),
            Some(Err(_)) | None => None,
        };
        let (client, renewed) = match held {
            Some(client) => (client, None),
            None => (
                new_client(&own, &sharer.odin_id, kept)?,
                Some(Instant::now()),
            ),
        };
        let ask = {
            let (own, sharer, kept) = (Arc::clone(&own), sharer.odin_id.clone(), kept.to_owned());
            move || new_client(&own, &sharer, &kept)
        };
        let mut remote = PeerRemote::with(own, sharer, client, renewed);
        remote.renewal = Some(Renewal(Box::new(ask)));
        Ok(remote)
    }

    /// The same share, written to as well: a change to a file of theirs, a new one in a
    /// folder of theirs, a move to their Recently deleted or back, each where they share it
    /// for editing (`drive/shared/writes.rs`). Nothing is deleted there from here.
    pub fn writing(mut self) -> PeerRemote {
        self.writing = true;
        self
    }

    /// Which token it reads with, saying nothing of it (a fingerprint, as
    /// [`crate::Credentials::login_id`]): what it sealed (the files' tokens) opens only under
    /// the same one.
    pub fn login_id(&self) -> String {
        self.client().login_id().to_owned()
    }

    /// What their server tells of the files shared, sent to `events` as an identity's own
    /// drive's is ([`Notifier::spawn_shared`]). Each connection is made with the token held
    /// then: one their server refuses is replaced by the next read ([`PeerRemote::renewed`]),
    /// never by the connection, which waits and asks again.
    pub fn notifier(self: &Arc<Self>, events: impl Into<Events>) -> std::io::Result<Notifier> {
        let remote = Arc::clone(self);
        let theirs = move || (*remote.client()).clone();
        Notifier::spawn_shared(theirs, self.their_drive(), events)
    }

    /// Their Filesync drive.
    fn their_drive(&self) -> TargetDrive {
        TargetDrive {
            alias: self.sharer.drive_alias,
            drive_type: SYNC_DRIVE.drive_type,
        }
    }

    /// Their file `file_id`'s header as their server sends it: one request.
    fn their_header(&self, file_id: RemoteFileId) -> Result<FileHeader> {
        self.with_token(|client| self.raw_header(client, file_id))
    }

    /// Their server, with the token held now.
    fn client(&self) -> Arc<Client> {
        Arc::clone(&self.held.read().unwrap_or_else(|e| e.into_inner()).client)
    }

    /// What `ask` answers with the token held; where their server refuses that token
    /// ([`Refusal::Token`]), once more with a new one, if one is had ([`PeerRemote::renewed`]).
    /// Where none is, the refusal stays what it is; but an ask for one that this identity's
    /// own server refused (the app's permission, this login) or put off (not connected to
    /// them) is the answer: that says what would lift it. Every ask here only reads, and a refusal comes before any of its
    /// answer.
    fn with_token<T>(&self, mut ask: impl FnMut(&Client) -> Result<T>) -> Result<T> {
        let client = self.client();
        match ask(&client) {
            Err(RemoteError::Denied(why, Refusal::Token)) => match self.renewed(&client) {
                Ok(Some(new)) => ask(&new),
                Ok(None) => Err(RemoteError::Denied(why, Refusal::Token)),
                Err(ours @ (RemoteError::Denied(..) | RemoteError::Waiting { .. })) => Err(ours),
                Err(e) => {
                    tracing::warn!(
                        "no new token for {}'s server, which refused the one it gave: {e}",
                        self.sharer.odin_id
                    );
                    Err(RemoteError::Denied(why, Refusal::Token))
                }
            },
            answer => answer,
        }
    }

    /// Their server with a new token, after it refused `refused`'s: the one another thread
    /// got meanwhile, or one asked for now and held from here on. `None` where none is asked
    /// for: this remote has nowhere to ask ([`PeerRemote::new`]), or it asked within
    /// [`RENEW_AFTER`] (in this run: a start doesn't know of the last one's). An ask that
    /// this identity's own server refused ([`RemoteError::Denied`]: this login, or the app's
    /// permission) or put off ([`RemoteError::Waiting`]: not connected to them) made no
    /// token, and doesn't count as one asked for; any other failure
    /// (their server's refusal to give one too) may have left a row there, and does. Held
    /// while asking: the others wait for the answer, refused as they would be.
    fn renewed(&self, refused: &Arc<Client>) -> Result<Option<Arc<Client>>> {
        let mut held = self.held.write().unwrap_or_else(|e| e.into_inner());
        if !Arc::ptr_eq(&held.client, refused) {
            return Ok(Some(Arc::clone(&held.client)));
        }
        let Some(Renewal(ask)) = &self.renewal else {
            return Ok(None);
        };
        if held.renewed.is_some_and(|at| at.elapsed() < RENEW_AFTER) {
            return Ok(None);
        }
        let asked = ask();
        let unasked = matches!(
            asked,
            Err(RemoteError::Denied(..) | RemoteError::Waiting { .. })
        );
        if !unasked {
            held.renewed = Some(Instant::now());
        }
        let client = asked?;
        tracing::info!(
            "{}'s server refused the token it gave: reading with a new one",
            self.sharer.odin_id
        );
        held.client = Arc::new(client);
        Ok(Some(Arc::clone(&held.client)))
    }

    /// Their server's answer as the trait's error: refusing the token (401) is
    /// [`Refusal::Token`], which a new token may lift ([`peer::refused`]).
    fn error(&self, e: HomebaseError) -> RemoteError {
        if peer::refused(&e) {
            tracing::debug!("{}'s server refused the token: {e}", self.sharer.odin_id);
            let why = format!(
                "refused by {}'s server: it no longer accepts this computer for what they \
                 share with you (your connection with them may have ended)",
                self.sharer.odin_id
            );
            return RemoteError::Denied(why, Refusal::Token);
        }
        remote_error(e)
    }

    fn query_batch(
        &self,
        client: &Client,
        body: &serde_json::Value,
    ) -> Result<wire::QueryBatchResponse> {
        query_batch(client, self.sharer.drive_alias, body).map_err(|e| self.error(e))
    }

    fn raw_header(&self, client: &Client, file_id: RemoteFileId) -> Result<FileHeader> {
        raw_header(client, self.sharer.drive_alias, file_id).map_err(|e| self.error(e))
    }

    /// The key of their `circle`, opened once ([`HomebaseRemote::circle_key`]). Not held while
    /// asked (two requests, theirs through this identity's server): readers of circles already
    /// open don't wait, and two first readers of one may each ask once.
    fn circle_key(&self, circle: Uuid) -> Result<Arc<CircleKey>> {
        let circles = || self.circles.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(key) = circles().get(&circle) {
            return Ok(Arc::clone(key));
        }
        let key = Arc::new(self.own.circle_key(&self.sharer, circle)?);
        circles().insert(circle, Arc::clone(&key));
        Ok(key)
    }

    /// The circle key `header` needs, if a live node's: a page with a file whose circle's key
    /// isn't to be had yet (or that names no circle) fails whole, so its cursor doesn't pass the
    /// file by.
    fn circle_for(&self, header: &FileHeader) -> Result<Option<Arc<CircleKey>>> {
        if !header.needs_circle_key() {
            return Ok(None);
        }
        // (Theirs to mend, and lasting until they do: waited for, in words, not a busy
        // server's "come back later".)
        let circle = header.share_circle().ok_or_else(|| RemoteError::Waiting {
            why: format!(
                "waiting for {}: a file of theirs is shared with you, but through no circle \
                 it names (file {})",
                self.sharer.odin_id, header.file_id
            ),
            retry_after: SHARER_WAIT,
        })?;
        self.circle_key(circle).map(Some)
    }

    /// The keys of what `client` was answered.
    fn keys<'a>(client: &'a Client, circle: Option<&'a CircleKey>) -> Keys<'a> {
        Keys {
            shared: client.key(),
            drive: None,
            circle,
            write_only: None,
        }
    }

    /// `header`, as `client` was answered it, as a [`RemoteHeader`], opened with its circle's
    /// key.
    fn to_remote(
        &self,
        client: &Client,
        header: FileHeader,
    ) -> Result<std::result::Result<RemoteHeader, String>> {
        let circle = self.circle_for(&header)?;
        Ok(wire::to_remote(
            header,
            Self::keys(client, circle.as_deref()),
        ))
    }

    /// Writes nothing: what they share is theirs to change.
    /// [`Remote::read`], asking as `client`.
    fn read_with(
        &self,
        client: &Client,
        file_id: RemoteFileId,
        range: Option<Range<u64>>,
        sink: &mut dyn Write,
    ) -> Result<u64> {
        let header = self.raw_header(client, file_id)?;
        let circle = self.circle_for(&header)?;
        let keys = Self::keys(client, circle.as_deref());
        let key = payload_key(&header, keys)?;
        let range = match range {
            Some(r) => r,
            // The engine always names a range: this opens the content too.
            None => 0..plain_size(&wire::to_remote(header, keys).map_err(RemoteError::Fatal)?)?,
        };
        if range.is_empty() {
            return Ok(0);
        }
        let len = range.end - range.start;
        let path = format!(
            "{}/payload/{PAYLOAD_KEY}",
            file_route(self.sharer.drive_alias, file_id)
        );
        let at = CipherRange::of(&range);
        let response = fetch(client, &path, at.fetch.clone()).map_err(|e| self.error(e))?;
        if !response.encrypted {
            return Err(RemoteError::Fatal(format!("{path}: not encrypted")));
        }
        decrypt(response, &key, &at, len, &path, sink)
    }

    fn read_only(&self) -> RemoteError {
        RemoteError::read_only(&self.sharer.odin_id)
    }
}

mod writes;

impl Remote for PeerRemote {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        self.with_token(|client| {
            let ask = |b: &serde_json::Value| self.query_batch(client, b);
            let (page, cursor) = feed_page(ask, cursor, limit, wire::Shape::Full)?;
            changes_page(page, cursor, |h| self.to_remote(client, h))
        })
    }

    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        self.with_token(|client| {
            let ask = |b: &serde_json::Value| self.query_batch(client, b);
            let (page, cursor) = feed_page(ask, cursor, limit, wire::Shape::Minimal)?;
            refs_page(page, cursor)
        })
    }

    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        wire::cursor_time(cursor)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        self.with_token(|client| {
            self.to_remote(client, self.raw_header(client, file_id)?)?
                .map_err(RemoteError::Fatal)
        })
    }

    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> Result<HashSet<NodeId>> {
        self.with_token(|client| holding(|body| self.query_batch(client, body), folders, holds))
    }

    fn hold_back(&self) -> std::time::Duration {
        HOLD_BACK
    }

    fn shared(&self) -> bool {
        true
    }

    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> Result<Written> {
        match self.writing {
            true => self.add_to_theirs(node, payload),
            false => Err(self.read_only()),
        }
    }

    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> Result<Written> {
        match self.writing {
            true => self.change_theirs(target, meta, payload),
            false => Err(self.read_only()),
        }
    }

    fn delete(&self, _: &[Doomed]) -> Result<Deletes> {
        Err(self.read_only())
    }

    fn delete_for_good(&self, _: &[Doomed]) -> Result<ForGood> {
        Err(self.read_only())
    }

    fn set_removed(&self, files: &[Target], removed: bool) -> Result<Answers<Written>> {
        match self.writing {
            true => self.set_theirs_removed(files, removed),
            false => Err(self.read_only()),
        }
    }

    /// Plaintext `range` of the payload. Their server sends a shared file's payload without its
    /// key (the client locked it): the header is read first (one request), which gives the key,
    /// the payload's IV and, without a range, its size.
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<Range<u64>>,
        sink: &mut dyn Write,
    ) -> Result<u64> {
        self.with_token(|client| self.read_with(client, file_id, range.clone(), &mut *sink))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;

    use super::*;
    use serde_json::json;

    use crate::drive::tests::{api, unreachable_remote};

    pub(super) fn sam() -> Result<PeerRemote, HomebaseError> {
        let token: PeerToken = serde_json::from_value(serde_json::json!({
            "authenticationToken64": "dG9rZW4=", "sharedSecret": "AAAAAAAAAAAAAAAAAAAAAA==",
        }))?;
        let sharer = Sharer {
            odin_id: "sam.invalid".into(),
            drive_alias: Uuid::new_v4(),
        };
        PeerRemote::new(
            Arc::new(unreachable_remote(Default::default())?),
            sharer,
            &token,
        )
    }

    /// Nothing is written to what another identity shares: every write refuses, before any
    /// request (their server is unreachable here).
    #[test]
    fn what_is_shared_is_read_only() -> Result<(), HomebaseError> {
        let sam = sam()?;
        let meta = NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: hbsync_types::NodeKind::Folder,
            content: hbsync_types::schema::NodeContent::folder("Photos"),
            removed: false,
        };
        let refused = |r: Result<()>| match r {
            Err(RemoteError::Fatal(why)) => assert!(why.contains("read-only"), "{why}"),
            other => panic!("{other:?}"),
        };
        refused(sam.create(&meta, None).map(drop));
        let target = Target {
            at: hbsync_types::RemoteRef {
                file_id: RemoteFileId(Uuid::new_v4()),
                version: hbsync_types::VersionTag(Uuid::new_v4()),
            },
            token: None,
        };
        refused(sam.update(&target, &meta, None).map(drop));
        refused(sam.delete(&[]).map(drop));
        refused(sam.delete_for_good(&[]).map(drop));
        refused(sam.set_removed(&[], true).map(drop));
        Ok(())
    }

    /// A live shared file naming no circle waits, before any request: its page isn't passed by.
    #[test]
    fn a_shared_file_naming_no_circle_waits() -> Result<(), Box<dyn std::error::Error>> {
        let sam = sam()?;
        for shares in [json!(null), json!([]), json!(["junk"])] {
            let header: FileHeader = serde_json::from_value(json!({
                "fileId": Uuid::new_v4(), "fileState": "active",
                "fileMetadata": {
                    "isEncrypted": true, "versionTag": Uuid::new_v4(),
                    "appData": { "uniqueId": Uuid::new_v4(), "groupId": Uuid::new_v4(),
                                 "fileType": hbsync_types::schema::FILE_TYPE_FILE, "content": "" },
                },
                "sharedSecretEncryptedKeyHeader": {
                    "encryptionVersion": 1, "type": "share", "iv": "", "encryptedAesKey": "",
                },
                "shares": shares,
            }))?;
            match sam.to_remote(&sam.client(), header) {
                Err(RemoteError::Waiting { why, .. }) => {
                    assert!(why.contains("no circle"), "{why}")
                }
                other => panic!("{other:?}"),
            }
        }
        Ok(())
    }

    /// Their server refusing the token is a refusal of the token (a new one may lift it, not a
    /// login); refusing a file isn't.
    #[test]
    fn a_refused_token_is_a_refusal_of_the_token() -> Result<(), HomebaseError> {
        let sam = sam()?;
        match sam.error(api(401, None)) {
            RemoteError::Denied(why, Refusal::Token) => {
                assert!(why.contains("sam.invalid"), "{why}");
            }
            other => panic!("{other:?}"),
        }
        assert!(sam.error(api(403, None)).refusal().is_none());
        assert!(matches!(sam.error(api(404, None)), RemoteError::NotFound));
        Ok(())
    }

    /// `sam`, a refused token of whose is replaced by `new` tokens: how often one was asked
    /// for.
    fn renewing(mut sam: PeerRemote) -> (PeerRemote, Arc<AtomicUsize>) {
        let asked = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&asked);
        sam.renewal = Some(Renewal(Box::new(move || {
            count.fetch_add(1, SeqCst);
            let new = PeerToken {
                token: "bmV3".into(),
                shared_secret: "AQEBAQEBAQEBAQEBAQEBAQ==".into(),
            };
            Client::for_peer("sam.invalid", &new).map_err(remote_error)
        })));
        (sam, asked)
    }

    fn refused<T>() -> Result<T> {
        Err(RemoteError::Denied("401".into(), Refusal::Token))
    }

    /// A token their server refuses is replaced once, and what was asked is asked again with
    /// the new one, which is held from then on.
    #[test]
    fn a_refused_token_is_replaced_and_the_ask_repeated() -> Result<(), HomebaseError> {
        let (sam, asked) = renewing(sam()?);
        let old = sam.login_id();
        let answer = sam.with_token(|client| match client.login_id() == old {
            true => refused(),
            false => Ok(client.login_id().to_owned()),
        });
        let new = answer.expect("answered with the new token");
        assert_ne!(new, old);
        assert_eq!(sam.login_id(), new);
        assert_eq!(asked.load(SeqCst), 1);
        // Held: the next ask goes out with it, nothing asked for.
        let again = sam.with_token(|client| Ok(client.login_id().to_owned()));
        assert_eq!(again.expect("answered"), new);
        assert_eq!(asked.load(SeqCst), 1);
        Ok(())
    }

    /// A new token refused too is the refusal, and no other is asked for within the hour: a
    /// server that refuses every token it makes isn't given a row a request.
    #[test]
    fn a_new_token_refused_too_isnt_replaced_again_at_once() -> Result<(), HomebaseError> {
        let (sam, asked) = renewing(sam()?);
        for _ in 0..3 {
            let answer: Result<()> = sam.with_token(|_| refused());
            assert!(matches!(
                answer,
                Err(RemoteError::Denied(_, Refusal::Token))
            ));
        }
        assert_eq!(asked.load(SeqCst), 1);
        Ok(())
    }

    /// Only a refusal of the token asks for a new one; and a remote with nowhere to ask
    /// leaves the refusal as it is.
    #[test]
    fn only_a_refused_token_is_replaced_and_only_where_one_can_be_asked_for()
    -> Result<(), HomebaseError> {
        let (sam, asked) = renewing(sam()?);
        let answer: Result<()> = sam.with_token(|_| Err(RemoteError::NotFound));
        assert!(matches!(answer, Err(RemoteError::NotFound)));
        let answer: Result<()> =
            sam.with_token(|_| Err(RemoteError::Denied("403".into(), Refusal::Permission)));
        assert!(matches!(
            answer,
            Err(RemoteError::Denied(_, Refusal::Permission))
        ));
        assert_eq!(asked.load(SeqCst), 0);
        let plain = self::sam()?;
        let (old, mut asks) = (plain.login_id(), 0);
        let answer: Result<()> = plain.with_token(|_| {
            asks += 1;
            refused()
        });
        assert!(matches!(
            answer,
            Err(RemoteError::Denied(_, Refusal::Token))
        ));
        assert_eq!((asks, plain.login_id()), (1, old));
        Ok(())
    }

    /// An ask for a new token that this identity's server puts off (not connected to them) is
    /// the answer, in its words, and made no token: the next refusal asks again.
    #[test]
    fn an_ask_put_off_is_the_answer_and_no_token_asked_for() -> Result<(), HomebaseError> {
        let mut sam = sam()?;
        let asked = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&asked);
        sam.renewal = Some(Renewal(Box::new(move || {
            count.fetch_add(1, SeqCst);
            Err(RemoteError::Waiting {
                why: "not connected to sam.invalid".into(),
                retry_after: Duration::from_secs(3600),
            })
        })));
        for _ in 0..2 {
            let answer: Result<()> = sam.with_token(|_| refused());
            match answer {
                Err(RemoteError::Waiting { why, retry_after }) => {
                    assert_eq!(why, "not connected to sam.invalid");
                    assert_eq!(retry_after, Duration::from_secs(3600));
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(asked.load(SeqCst), 2);
        Ok(())
    }

    /// An ask for a new token that is refused made none, and the next refusal asks again;
    /// one that failed otherwise may have left a row on their server, and none is asked for
    /// within the hour.
    #[test]
    fn only_an_ask_that_may_have_made_a_token_holds_the_next_back() -> Result<(), HomebaseError> {
        let failing = |with: fn() -> RemoteError| -> Result<usize, HomebaseError> {
            let mut sam = sam()?;
            let asked = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&asked);
            sam.renewal = Some(Renewal(Box::new(move || {
                count.fetch_add(1, SeqCst);
                Err(with())
            })));
            for _ in 0..3 {
                let answer: Result<()> = sam.with_token(|_| refused());
                assert!(answer.is_err());
            }
            Ok(asked.load(SeqCst))
        };
        let not_permitted = || RemoteError::Denied("no".into(), Refusal::Permission);
        assert_eq!(failing(not_permitted)?, 3);
        assert_eq!(failing(|| RemoteError::Transient("cut".into()))?, 1);
        assert_eq!(failing(|| RemoteError::Fatal("not a token".into()))?, 1);
        Ok(())
    }

    /// A token kept that makes no client fails the opening while it is new (asked for
    /// within the hour: their server isn't asked again); once it is older a new one is asked
    /// for (of an identity that can't be reached here, which fails the opening too: nothing
    /// made, the file as it was). One kept that makes one opens with no request.
    #[test]
    fn a_kept_token_that_makes_no_client_is_asked_for_again_in_an_hour() -> Result<(), HomebaseError>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("token");
        let own = Arc::new(unreachable_remote(Default::default())?);
        let sharer = || Sharer {
            odin_id: "sam.invalid".into(),
            drive_alias: Uuid::new_v4(),
        };
        let token = |secret: &str| PeerToken {
            token: "dG9rZW4=".into(),
            shared_secret: secret.into(),
        };
        // (Eight bytes: not a key.)
        token("AAAAAAAAAAA=").keep(own.client(), &path)?;
        let before = std::fs::read(&path)?;
        match PeerRemote::open(Arc::clone(&own), sharer(), &path) {
            Err(RemoteError::Fatal(_)) => {}
            other => panic!("asked for again at once: {other:?}"),
        }
        let old = std::time::SystemTime::now() - 2 * RENEW_AFTER;
        std::fs::File::options()
            .write(true)
            .open(&path)?
            .set_modified(old)?;
        match PeerRemote::open(Arc::clone(&own), sharer(), &path) {
            Err(RemoteError::Fatal(why)) => panic!("not asked for again: {why}"),
            Err(_) => {}
            Ok(_) => panic!("opened with a token that makes no client"),
        }
        assert_eq!(std::fs::read(&path)?, before);
        token("AAAAAAAAAAAAAAAAAAAAAA==").keep(own.client(), &path)?;
        let opened = PeerRemote::open(own, sharer(), &path).expect("opened, nothing asked");
        // Kept from before: one refused may be replaced at once.
        assert!(
            opened
                .held
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .renewed
                .is_none()
        );
        Ok(())
    }

    /// Where no new token is had, the refusal stays the token's; but this identity's own
    /// server refusing the ask (the app's permission) is the answer, which the user can lift.
    #[test]
    fn a_failed_ask_for_a_new_token_leaves_the_refusal_unless_it_is_ours()
    -> Result<(), HomebaseError> {
        let failing = |with: fn() -> RemoteError| -> Result<RemoteError, HomebaseError> {
            let mut sam = sam()?;
            sam.renewal = Some(Renewal(Box::new(move || Err(with()))));
            let answer: Result<()> = sam.with_token(|_| refused());
            Ok(answer.expect_err("refused"))
        };
        assert!(matches!(
            failing(|| RemoteError::Fatal("not connected".into()))?,
            RemoteError::Denied(_, Refusal::Token)
        ));
        assert!(matches!(
            failing(|| RemoteError::Transient("cut".into()))?,
            RemoteError::Denied(_, Refusal::Token)
        ));
        assert!(matches!(
            failing(|| RemoteError::Denied("no".into(), Refusal::Permission))?,
            RemoteError::Denied(_, Refusal::Permission)
        ));
        Ok(())
    }

    /// A refusal met by two readers at once asks for one token: the second takes the
    /// first's.
    #[test]
    fn a_token_another_reader_replaced_is_taken() -> Result<(), HomebaseError> {
        let (sam, asked) = renewing(sam()?);
        let refused_client = sam.client();
        let first = sam
            .renewed(&refused_client)
            .expect("asked")
            .expect("a new one");
        let second = sam
            .renewed(&refused_client)
            .expect("taken")
            .expect("the same");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(asked.load(SeqCst), 1);
        Ok(())
    }
}
