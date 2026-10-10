//! Against a live identity: `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live
//! -- --ignored`. Needs the credentials `hbsync-register` saves for it. Each run works in its
//! own top-level folder and deletes it at the end.

use std::time::Duration;

use hbsync_dev::{live_identity, run_name, saved_client};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_remote::contract::Bench;

type R = Result<(), Box<dyn std::error::Error>>;

fn live_remote() -> Result<HomebaseRemote, Box<dyn std::error::Error>> {
    Ok(HomebaseRemote::connect(saved_client(&live_identity()?)?)?)
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn the_contract_holds_on_a_live_drive() -> R {
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("contract"), Duration::from_millis(20))?;
    let failed = bench.run_all();
    let cleanup = bench.cleanup();
    assert!(failed.is_empty(), "{failed:#?}");
    cleanup?;
    let skipped = remote.take_skipped();
    assert!(skipped.is_empty(), "feed rows skipped: {skipped:#?}");
    Ok(())
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn a_change_on_the_drive_is_announced() -> R {
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("notify"), Duration::ZERO)?;
    let (tx, rx) = std::sync::mpsc::channel();
    let notifier = Notifier::spawn(remote.client().clone(), remote.drive(), tx);
    let wait = |what: Event| -> Result<(), String> {
        loop {
            match rx.recv_timeout(Duration::from_secs(15)) {
                Ok(e) if e == what => return Ok(()),
                Ok(Event::Lost(why) | Event::Refused(why)) => return Err(why),
                Ok(_) => {}
                Err(e) => return Err(format!("waiting for {what:?}: {e}")),
            }
        }
    };
    let result = (|| -> R {
        wait(Event::Connected)?;
        let start = std::time::Instant::now();
        bench.folder("inside")?;
        wait(Event::Changed)?;
        let took = start.elapsed();
        assert!(took < Duration::from_secs(2), "announced after {took:?}");
        println!("announced {took:?} after the write returned");
        Ok(())
    })();
    drop(notifier);
    bench.cleanup()?;
    result
}

/// What this client writes is stored encrypted: names, sizes and bytes are ciphertext on the
/// server, and another reader gets the plaintext back only with the key.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn nodes_are_stored_encrypted() -> R {
    use hbsync_homebase::drive::sealed::sealed_len;
    use hbsync_remote::contract::file;
    use serde_json::Value;

    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("sealed"), Duration::ZERO)?;
    let result = (|| -> R {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let (dir, dir_at) = bench.folder("Private folder")?;
        let f = file("Secret plan.txt", dir.node, &data);
        let at = bench.create(&f, Some(&data))?;
        let files = format!("/api/v2/drives/{}/files", remote.drive().alias.simple());
        let header = |id: uuid::Uuid| -> Result<Value, Box<dyn std::error::Error>> {
            Ok(remote.client().get(&format!("{files}/{id}/header"), &[])?)
        };
        for (id, name) in [
            (dir_at.file_id.0, "Private folder"),
            (at.file_id.0, "Secret plan.txt"),
        ] {
            let h = header(id)?;
            let meta = &h["fileMetadata"];
            assert_eq!(meta["isEncrypted"], true, "{h}");
            let content = meta["appData"]["content"].as_str().ok_or("content")?;
            assert!(
                !content.contains(name) && !content.contains("\"v\""),
                "{content}"
            );
        }
        let h = header(at.file_id.0)?;
        let payload = &h["fileMetadata"]["payloads"][0];
        assert!(
            payload["iv"].as_str().is_some_and(|iv| !iv.is_empty()),
            "{payload}"
        );
        let hash = &payload["hash"];
        assert_ne!(hash["storedHash"], hash["contentHash"], "{hash}");

        // The raw bytes are ciphertext, padded; through the remote they read back as written.
        let mut raw = Vec::new();
        let mut fetched = remote
            .client()
            .fetch(&format!("{files}/{}/payload/sync_data", at.file_id.0))?;
        assert!(fetched.encrypted && fetched.key_header.is_some());
        fetched.copy_to(&mut raw, u64::MAX)?;
        assert_eq!(raw.len() as u64, sealed_len(data.len() as u64));
        assert!(!raw.windows(64).any(|w| data.windows(64).next() == Some(w)));
        let mut back = Vec::new();
        hbsync_remote::Remote::read(&remote, at.file_id, None, &mut back)?;
        assert_eq!(back, data);
        Ok(())
    })();
    bench.cleanup()?;
    result
}

/// A file an earlier version stored unencrypted still reads and updates, and stays
/// unencrypted (the server can't change that on an update).
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn unencrypted_files_still_sync() -> R {
    use std::io::Cursor;

    use hbsync_homebase::Credentials;
    use hbsync_homebase::client::UploadMethod;
    use hbsync_homebase::drive::wire::{self, PayloadDescriptor};
    use hbsync_homebase::multipart::{Multipart, Part};
    use hbsync_remote::contract::{file, upload, with};
    use hbsync_remote::{Remote, RemoteHeader};
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{RemoteFileId, RemoteRef, VersionTag};
    use serde_json::Value;

    let identity = live_identity()?;
    let creds = Credentials::load(&hbsync_dev::credentials_path(&identity)?)?.ok_or("creds")?;
    let shared = creds.shared_secret_key()?;
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("legacy"), Duration::ZERO)?;
    let files = format!("/api/v2/drives/{}/files", remote.drive().alias.simple());
    let result = (|| -> R {
        // Written as the client did before encryption: no key, both hashes the plaintext's.
        let (dir, _) = bench.folder("legacy")?;
        let data = b"written before encryption".to_vec();
        let meta = file("old.txt", dir.node, &data);
        let hash = hbsync_fileio::hash_bytes(&data, hbsync_fileio::HashAlgorithm::Blake3);
        let iv = hbsync_homebase::drive::sealed::strong()?;
        let mut source = &data[..];
        let parts = vec![
            Part::bytes(
                "instructions",
                "application/json",
                wire::instructions(&iv, Some(&PayloadDescriptor::plain(&hash)), false)
                    .to_string()
                    .into_bytes(),
            ),
            Part::bytes(
                "metadata",
                wire::CONTENT_TYPE,
                wire::metadata(&meta, None, &shared, &iv, None),
            ),
            Part::reader(
                "payload",
                "sync_data",
                wire::CONTENT_TYPE,
                &mut source,
                data.len() as u64,
            ),
        ];
        let written: Value = remote.client().upload(
            UploadMethod::Post,
            &files,
            Multipart::new(uuid::Uuid::new_v4().simple().to_string(), parts),
        )?;
        let id = RemoteFileId(written["fileId"].as_str().ok_or("fileId")?.parse()?);
        let encrypted = |id: RemoteFileId| -> Result<bool, Box<dyn std::error::Error>> {
            let h: Value = remote
                .client()
                .get(&format!("{files}/{}/header", id.0), &[])?;
            Ok(h["fileMetadata"]["isEncrypted"] == true)
        };
        assert!(!encrypted(id)?);
        let state = |h: RemoteHeader| h.state().and_then(|s| s.content).ok_or("no content");
        assert_eq!(state(remote.header(id)?)?.hash, hash);
        let mut back = Vec::new();
        remote.read(id, None, &mut back)?;
        assert_eq!(back, data);

        // An update of its bytes and name keeps it unencrypted.
        let new = b"updated after encryption".to_vec();
        let renamed = with(
            &meta,
            NodeContent::file("renamed.txt", 2, new.len() as u64, false),
        );
        let version = VersionTag(written["newVersionTag"].as_str().ok_or("tag")?.parse()?);
        let at: RemoteRef = remote.update(
            id,
            version,
            &renamed,
            Some(upload(&mut Cursor::new(&new[..]))),
        )?;
        assert!(!encrypted(id)?);
        let mut back = Vec::new();
        remote.read(id, None, &mut back)?;
        assert_eq!(back, new);
        remote.delete(id, at.version)?;
        Ok(())
    })();
    bench.cleanup()?;
    result
}
