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
    HOLD_BACK, HomebaseRemote, SHARER_WAIT, changes_page, decrypt, feed_page, fetch, file_route,
    holding, payload_key, plain_size, query_batch, raw_header, refs_page, remote_error,
};
use crate::peer::{self, PeerToken};
use crate::{Client, HomebaseError};

/// One sharer's files shared with this identity, on their server.
/// How long after a new token was asked for, in place of one their server refused, the next
/// is: each stays a row on their server, so a server that refuses every one it makes gets one
/// an hour, not one a request.
const RENEW_AFTER: Duration = Duration::from_secs(3600);

/// Where a new token comes from when theirs is refused ([`PeerRemote::renewed`]).
struct Renewal {
    /// Asks for one, and keeps it.
    ask: Box<dyn Fn() -> Result<PeerToken> + Send + Sync>,
    /// When one was last asked for.
    asked: Mutex<Option<Instant>>,
}

impl std::fmt::Debug for Renewal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Renewal(..)")
    }
}

#[derive(Debug)]
pub struct PeerRemote {
    /// Their server, with the token it made for this identity: replaced by one with a new
    /// token when they refuse this one's ([`PeerRemote::renewed`]). An answer is opened with
    /// the client that asked: its token's secret is the key.
    client: RwLock<Arc<Client>>,
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
}

/// A new token for `sharer`'s server through `own` ([`HomebaseRemote::peer_token`]), kept at
/// `kept`. One that couldn't be kept is used all the same (told): refusing it would ask for
/// another at the next try, a row on their server each time.
fn new_token(own: &HomebaseRemote, sharer: &str, kept: &Path) -> Result<PeerToken> {
    let token = own.peer_token(sharer)?;
    if let Err(e) = token.keep(own.client(), kept) {
        tracing::warn!(
            "the token {sharer}'s server gave couldn't be kept ({e}): a new one is asked for \
             at the next start"
        );
    }
    Ok(token)
}

impl PeerRemote {
    /// `sharer`'s files shared with `own`'s identity, read with `token` (made by their server
    /// for this identity: [`PeerToken::new`]). No request yet.
    pub fn new(
        own: Arc<HomebaseRemote>,
        sharer: Sharer,
        token: &PeerToken,
    ) -> Result<PeerRemote, HomebaseError> {
        Ok(PeerRemote {
            client: RwLock::new(Arc::new(Client::for_peer(&sharer.odin_id, token)?)),
            own,
            sharer,
            circles: Mutex::default(),
            renewal: None,
        })
    }

    /// `sharer`'s files shared with `own`'s identity, read with the token kept at `kept`
    /// ([`PeerToken::kept`]) or, where none is that opens under this login, with a new one
    /// asked for and kept there ([`new_token`]): one request then, none otherwise. A token
    /// their server refuses later is replaced the same way ([`PeerRemote::renewed`]).
    pub fn open(own: Arc<HomebaseRemote>, sharer: Sharer, kept: &Path) -> Result<PeerRemote> {
        let token = match PeerToken::kept(own.client(), kept).map_err(remote_error)? {
            Some(token) => token,
            None => new_token(&own, &sharer.odin_id, kept)?,
        };
        let ask = {
            let (own, sharer, kept) = (Arc::clone(&own), sharer.odin_id.clone(), kept.to_owned());
            move || new_token(&own, &sharer, &kept)
        };
        let mut remote = PeerRemote::new(own, sharer, &token).map_err(remote_error)?;
        remote.renewal = Some(Renewal {
            ask: Box::new(ask),
            asked: Mutex::default(),
        });
        Ok(remote)
    }

    /// Which token it reads with, saying nothing of it (a fingerprint, as
    /// [`crate::Credentials::login_id`]): what it sealed (the files' tokens) opens only under
    /// the same one.
    pub fn login_id(&self) -> String {
        self.client().login_id().to_owned()
    }

    /// Their server, with the token held now.
    fn client(&self) -> Arc<Client> {
        Arc::clone(&self.client.read().unwrap_or_else(|e| e.into_inner()))
    }

    /// What `ask` answers with the token held; where their server refuses that token
    /// ([`Refusal::Token`]), once more with a new one, if one is had ([`PeerRemote::renewed`]).
    /// Every ask here only reads, and a refusal comes before any of its answer.
    fn with_token<T>(&self, mut ask: impl FnMut(&Client) -> Result<T>) -> Result<T> {
        let client = self.client();
        match ask(&client) {
            Err(RemoteError::Denied(why, Refusal::Token)) => match self.renewed(&client)? {
                Some(new) => ask(&new),
                None => Err(RemoteError::Denied(why, Refusal::Token)),
            },
            answer => answer,
        }
    }

    /// Their server with a new token, after it refused `refused`'s: the one another thread
    /// got meanwhile, or one asked for now and held from here on. `None` where none is asked
    /// for: this remote has nowhere to ask ([`PeerRemote::new`]), or it asked within
    /// [`RENEW_AFTER`]. Held while asking: the others wait for the answer, refused as they
    /// would be.
    fn renewed(&self, refused: &Arc<Client>) -> Result<Option<Arc<Client>>> {
        let mut held = self.client.write().unwrap_or_else(|e| e.into_inner());
        if !Arc::ptr_eq(&held, refused) {
            return Ok(Some(Arc::clone(&held)));
        }
        let Some(renewal) = &self.renewal else {
            return Ok(None);
        };
        {
            let mut asked = renewal.asked.lock().unwrap_or_else(|e| e.into_inner());
            if asked.is_some_and(|at| at.elapsed() < RENEW_AFTER) {
                return Ok(None);
            }
            *asked = Some(Instant::now());
        }
        let token = (renewal.ask)()?;
        let client = Client::for_peer(&self.sharer.odin_id, &token).map_err(remote_error)?;
        tracing::info!(
            "{}'s server refused the token it gave: reading with a new one",
            self.sharer.odin_id
        );
        *held = Arc::new(client);
        Ok(Some(Arc::clone(&held)))
    }

    /// Their server's answer as the trait's error: refusing the token (401) is
    /// [`Refusal::Token`], which a new token may lift ([`peer::refused`]).
    fn error(&self, e: HomebaseError) -> RemoteError {
        if peer::refused(&e) {
            let why = format!(
                "{} refuses the token their server gave you: {e}",
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
        let circle = header.share_circle().ok_or_else(|| RemoteError::Later {
            why: format!(
                "file {}: shared with you, but through no circle it names",
                header.file_id
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
    fn read_only(&self) -> RemoteError {
        RemoteError::read_only(&self.sharer.odin_id)
    }
}

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

    fn create(&self, _: &NodeMeta, _: Option<Upload<'_>>) -> Result<Written> {
        Err(self.read_only())
    }

    fn update(&self, _: &Target, _: &NodeMeta, _: Option<Upload<'_>>) -> Result<Written> {
        Err(self.read_only())
    }

    fn delete(&self, _: &[Doomed]) -> Result<Deletes> {
        Err(self.read_only())
    }

    fn delete_for_good(&self, _: &[Doomed]) -> Result<ForGood> {
        Err(self.read_only())
    }

    fn set_removed(&self, _: &[Target], _: bool) -> Result<Answers<Written>> {
        Err(self.read_only())
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

impl PeerRemote {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::drive::tests::{api, unreachable_remote};

    fn sam() -> Result<PeerRemote, HomebaseError> {
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
                Err(RemoteError::Later { why, .. }) => assert!(why.contains("no circle"), "{why}"),
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
    fn renewing(mut sam: PeerRemote) -> (PeerRemote, Arc<std::sync::atomic::AtomicUsize>) {
        let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = Arc::clone(&asked);
        sam.renewal = Some(Renewal {
            ask: Box::new(move || {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(PeerToken {
                    token: "bmV3".into(),
                    shared_secret: "AQEBAQEBAQEBAQEBAQEBAQ==".into(),
                })
            }),
            asked: Mutex::default(),
        });
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
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1);
        // Held: the next ask goes out with it, nothing asked for.
        let again = sam.with_token(|client| Ok(client.login_id().to_owned()));
        assert_eq!(again.expect("answered"), new);
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1);
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
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1);
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
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 0);
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
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1);
        Ok(())
    }
}
