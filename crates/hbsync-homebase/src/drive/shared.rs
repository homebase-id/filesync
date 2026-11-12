//! What another identity shares with this one, read straight from their server: a read-only
//! [`Remote`] over the files of theirs shared through a circle this identity is in, as their
//! server answers a member, to the token it made for this identity's connection
//! (`docs/spec/homebase-api-notes.md`, "Shared with you"). Each file's key is locked with the
//! circle's key, which their devices seal to this identity's drive
//! (`docs/spec/encryption.md`, "Files shared with you").

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::ops::Range;
use std::sync::{Arc, Mutex};

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
    HOLD_BACK, HomebaseRemote, changes_page, decrypt, feed_page, fetch, file_route, holding,
    payload_key, plain_size, query_batch, raw_header, refs_page, remote_error,
};
use crate::peer::{self, PeerToken};
use crate::{Client, HomebaseError};

/// One sharer's files shared with this identity, on their server.
#[derive(Debug)]
pub struct PeerRemote {
    /// Their server, with the token it made for this identity.
    client: Client,
    /// This identity's own drive, whose server asks theirs for a circle's key, and whose
    /// write-only key opens it.
    own: Arc<HomebaseRemote>,
    sharer: Sharer,
    /// The keys of their circles opened so far: one request each, the first time a file shared
    /// through it is read.
    circles: Mutex<HashMap<Uuid, Arc<CircleKey>>>,
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
            client: Client::for_peer(&sharer.odin_id, token)?,
            own,
            sharer,
            circles: Mutex::default(),
        })
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

    fn query_batch(&self, body: &serde_json::Value) -> Result<wire::QueryBatchResponse> {
        query_batch(&self.client, self.sharer.drive_alias, body).map_err(|e| self.error(e))
    }

    fn raw_header(&self, file_id: RemoteFileId) -> Result<FileHeader> {
        raw_header(&self.client, self.sharer.drive_alias, file_id).map_err(|e| self.error(e))
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
    /// isn't to be had yet fails whole, so its cursor doesn't pass the file by.
    fn circle_for(&self, header: &FileHeader) -> Result<Option<Arc<CircleKey>>> {
        header
            .needs_circle_key()
            .map(|circle| self.circle_key(circle))
            .transpose()
    }

    fn keys<'a>(&'a self, circle: Option<&'a CircleKey>) -> Keys<'a> {
        Keys {
            shared: self.client.key(),
            drive: None,
            circle,
        }
    }

    /// `header` as a [`RemoteHeader`], opened with its circle's key.
    fn to_remote(&self, header: FileHeader) -> Result<std::result::Result<RemoteHeader, String>> {
        let circle = self.circle_for(&header)?;
        Ok(wire::to_remote(header, self.keys(circle.as_deref())))
    }

    /// Writes nothing: what they share is theirs to change.
    fn read_only(&self) -> RemoteError {
        RemoteError::Fatal(format!(
            "what {} shares with you is read-only here",
            self.sharer.odin_id
        ))
    }
}

impl Remote for PeerRemote {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> Result<ChangesPage> {
        let (page, cursor) = feed_page(|b| self.query_batch(b), cursor, limit, wire::Shape::Full)?;
        changes_page(page, cursor, |h| self.to_remote(h))
    }

    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> Result<RefsPage> {
        let (page, cursor) =
            feed_page(|b| self.query_batch(b), cursor, limit, wire::Shape::Minimal)?;
        refs_page(page, cursor)
    }

    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        wire::cursor_time(cursor)
    }

    fn header(&self, file_id: RemoteFileId) -> Result<RemoteHeader> {
        self.to_remote(self.raw_header(file_id)?)?
            .map_err(RemoteError::Fatal)
    }

    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> Result<HashSet<NodeId>> {
        holding(|body| self.query_batch(body), folders, holds)
    }

    fn hold_back(&self) -> std::time::Duration {
        HOLD_BACK
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
        let header = self.raw_header(file_id)?;
        let circle = self.circle_for(&header)?;
        let keys = self.keys(circle.as_deref());
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
        let response = fetch(&self.client, &path, at.fetch.clone()).map_err(|e| self.error(e))?;
        if !response.encrypted {
            return Err(RemoteError::Fatal(format!("{path}: not encrypted")));
        }
        decrypt(response, &key, &at, len, &path, sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
