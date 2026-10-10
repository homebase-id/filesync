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
    use hbsync_remote::contract::{file, upload, with};
    use hbsync_remote::{Remote, RemoteHeader};
    use hbsync_types::RemoteRef;
    use hbsync_types::schema::NodeContent;
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
        let written = write_plain(&remote, &shared, &meta, &mut Cursor::new(&data[..]), &hash)?;
        let id = written.file_id;
        let encrypted =
            |id: hbsync_types::RemoteFileId| -> Result<bool, Box<dyn std::error::Error>> {
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
        let at: RemoteRef = remote.update(
            id,
            written.version,
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

/// Writes `meta` with `data` unencrypted, as the client did before encryption: no key, both
/// hashes the plaintext's.
fn write_plain(
    remote: &HomebaseRemote,
    shared: &hbsync_homebase::crypto::Key,
    meta: &hbsync_remote::NodeMeta,
    data: &mut dyn std::io::Read,
    hash: &hbsync_fileio::ContentHash,
) -> Result<hbsync_types::RemoteRef, Box<dyn std::error::Error>> {
    use hbsync_homebase::client::UploadMethod;
    use hbsync_homebase::drive::wire::{self, PayloadDescriptor};
    use hbsync_homebase::multipart::{Multipart, Part};
    use hbsync_types::{RemoteFileId, RemoteRef, VersionTag};

    let size = meta.content.size.ok_or("size")?;
    let iv = hbsync_homebase::drive::sealed::strong()?;
    let parts = vec![
        Part::bytes(
            "instructions",
            "application/json",
            wire::instructions(&iv, Some(&PayloadDescriptor::plain(hash)), false)
                .to_string()
                .into_bytes(),
        ),
        Part::bytes(
            "metadata",
            wire::CONTENT_TYPE,
            wire::metadata(meta, None, shared, &iv, None),
        ),
        Part::reader("payload", "sync_data", wire::CONTENT_TYPE, data, size),
    ];
    let files = format!("/api/v2/drives/{}/files", remote.drive().alias.simple());
    let written: serde_json::Value = remote.client().upload(
        UploadMethod::Post,
        &files,
        Multipart::new(uuid::Uuid::new_v4().simple().to_string(), parts),
    )?;
    let field = |name: &str| written[name].as_str().ok_or(name.to_owned());
    Ok(RemoteRef {
        file_id: RemoteFileId(field("fileId")?.parse()?),
        version: VersionTag(field("newVersionTag")?.parse()?),
    })
}

/// Upload and download speed, encrypted against unencrypted: one large file, then many small
/// ones. `HBSYNC_LIVE=… cargo test --release -p hbsync-dev --test live throughput -- --ignored
/// --nocapture` (`HBSYNC_BIG_MB` sets the large file's size, 1024 by default).
#[test]
#[ignore = "a measurement against a live identity (HBSYNC_LIVE)"]
fn throughput() -> R {
    use std::io::{Cursor, Seek, SeekFrom, Write};
    use std::time::Instant;

    use hbsync_fileio::{HashAlgorithm, Hasher, hash_bytes, hash_reader};
    use hbsync_homebase::Credentials;
    use hbsync_remote::contract::{file, upload, with};
    use hbsync_remote::{Remote, Upload};
    use hbsync_types::schema::NodeContent;

    struct Hashing(Hasher);
    impl Write for Hashing {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.update(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let identity = live_identity()?;
    let creds = Credentials::load(&hbsync_dev::credentials_path(&identity)?)?.ok_or("creds")?;
    let shared = creds.shared_secret_key()?;
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("throughput"), Duration::ZERO)?;
    let mut made = Vec::new();
    let result = (|| -> R {
        let (dir, _) = bench.folder("throughput")?;
        let alg = HashAlgorithm::Blake3;

        // One large file, from a file on disk as the engine sends it.
        let mb: u64 = std::env::var("HBSYNC_BIG_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        let size = mb << 20;
        let tmp = tempfile::tempfile()?;
        let mut w = std::io::BufWriter::new(&tmp);
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..size / 8 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            w.write_all(&x.to_le_bytes())?;
        }
        drop(w);
        let mut source = &tmp;
        source.seek(SeekFrom::Start(0))?;
        let (hash, _) = hash_reader(&mut source, alg)?;
        let rate = |what: &str, bytes: u64, t: Instant| {
            let secs = t.elapsed().as_secs_f64();
            println!(
                "{what:<28} {:>7.0} MB/s ({secs:.1} s)",
                bytes as f64 / secs / 1e6
            );
        };
        for encrypted in [false, true] {
            let mode = if encrypted { "encrypted" } else { "plain" };
            let name = format!("big {mode}");
            let meta = with(
                &file(&name, dir.node, &[]),
                NodeContent::file(&name, 1, size, false),
            );
            let mut source = &tmp;
            source.seek(SeekFrom::Start(0))?;
            let t = Instant::now();
            let at = if encrypted {
                let mut reader = source;
                let upload = Upload {
                    hash,
                    size,
                    reader: &mut reader,
                };
                remote.create(&meta, Some(upload))?
            } else {
                write_plain(&remote, &shared, &meta, &mut source, &hash)?
            };
            made.push(at);
            rate(&format!("upload {mb} MiB, {mode}"), size, t);
            let t = Instant::now();
            let mut sink = Hashing(Hasher::new(alg));
            remote.read(at.file_id, Some(0..size), &mut sink)?;
            assert_eq!(sink.0.finalize(), hash);
            rate(&format!("download {mb} MiB, {mode}"), size, t);
        }

        // Many small files: create, read, update.
        const N: usize = 200;
        for encrypted in [false, true] {
            let mode = if encrypted { "encrypted" } else { "plain" };
            let datas: Vec<Vec<u8>> = (0..N)
                .map(|i| format!("small file {i} {mode}").repeat(100).into_bytes())
                .collect();
            let t = Instant::now();
            let mut refs = Vec::new();
            for (i, data) in datas.iter().enumerate() {
                let meta = file(&format!("{mode}-{i}"), dir.node, data);
                let at = if encrypted {
                    bench.create(&meta, Some(data))?
                } else {
                    write_plain(
                        &remote,
                        &shared,
                        &meta,
                        &mut Cursor::new(&data[..]),
                        &hash_bytes(data, alg),
                    )?
                };
                refs.push((meta, at));
            }
            let per = |t: Instant| t.elapsed().as_secs_f64() * 1000.0 / N as f64;
            println!("create {N} small, {mode:<9}      {:>5.1} ms each", per(t));
            let t = Instant::now();
            for ((_, at), data) in refs.iter().zip(&datas) {
                let mut back = Vec::new();
                remote.read(at.file_id, Some(0..data.len() as u64), &mut back)?;
                assert_eq!(&back, data);
            }
            println!("read {N} small, {mode:<9}        {:>5.1} ms each", per(t));
            let t = Instant::now();
            for ((meta, at), data) in refs.iter_mut().zip(&datas) {
                let new = [data.as_slice(), b"!"].concat();
                let renamed = with(
                    meta,
                    NodeContent::file(&meta.content.name, 2, new.len() as u64, false),
                );
                *at = remote.update(
                    at.file_id,
                    at.version,
                    &renamed,
                    Some(upload(&mut Cursor::new(&new[..]))),
                )?;
            }
            println!("update {N} small, {mode:<9}      {:>5.1} ms each", per(t));
            if !encrypted {
                made.extend(refs.iter().map(|(_, at)| *at));
            }
        }
        Ok(())
    })();
    for at in made {
        let _ = remote.delete(at.file_id, at.version);
    }
    bench.cleanup()?;
    result
}
