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
    use hbsync_remote::Remote;
    use hbsync_remote::contract::file;

    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("sealed"), Duration::ZERO)?;
    let result = (|| -> R {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let (dir, dir_at) = bench.folder("Private folder")?;
        let f = file("Secret plan.txt", dir.node, &data);
        let at = bench.create(&f, Some(&data))?;
        for (id, name) in [
            (dir_at.file_id, "Private folder"),
            (at.file_id, "Secret plan.txt"),
        ] {
            let meta = remote.raw_header(id)?.file_metadata;
            assert!(meta.is_encrypted, "{name}");
            let content = meta.app_data.content.ok_or("content")?;
            assert!(
                !content.contains(name) && !content.contains("\"v\""),
                "{content}"
            );
        }
        let meta = remote.raw_header(at.file_id)?.file_metadata;
        let payload = meta.sync_payload().ok_or("payload")?;
        assert!(payload.iv.as_deref().is_some_and(|iv| !iv.is_empty()));
        let hash = payload.hash.as_ref().ok_or("hash")?;
        assert_ne!(hash.stored_hash, hash.content_hash);

        // The raw bytes are ciphertext, padded; through the remote they read back as written.
        let stored = sealed_len(data.len() as u64);
        assert_eq!(payload.bytes_written, stored);
        let path = format!("{}/payload/sync_data/0/{stored}", remote.file(at.file_id));
        let mut fetched = remote.client().fetch(&path, 0, stored - 1)?;
        assert!(fetched.encrypted && fetched.key_header.is_some());
        let mut raw = Vec::new();
        fetched.copy_to(&mut raw, stored)?;
        assert!(!raw.windows(64).any(|w| w == &data[..64]));
        let mut back = Vec::new();
        remote.read(at.file_id, None, &mut back)?;
        assert_eq!(back, data);
        Ok(())
    })();
    bench.cleanup()?;
    result
}

/// Upload and download speed: one large file, then many small ones. `HBSYNC_LIVE=… cargo test
/// --release -p hbsync-dev --test live throughput -- --ignored --nocapture` (`HBSYNC_BIG_MB`
/// sets the large file's size, 1024 by default).
#[test]
#[ignore = "a measurement against a live identity (HBSYNC_LIVE)"]
fn throughput() -> R {
    use std::io::{Cursor, Seek, SeekFrom, Write};
    use std::time::Instant;

    use hbsync_fileio::{HashAlgorithm, Hasher, hash_reader};
    use hbsync_remote::contract::{file, upload, with};
    use hbsync_remote::{NodeMeta, Remote, RemoteError, Upload};
    use hbsync_types::RemoteRef;
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

    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("throughput"), Duration::ZERO)?;
    // A create, remembered for cleanup.
    let put = |meta: &NodeMeta, upload: Upload<'_>| -> Result<RemoteRef, RemoteError> {
        let at = remote.create(meta, Some(upload))?;
        bench.track(at.file_id);
        Ok(at)
    };
    let result = (|| -> R {
        let (dir, _) = bench.folder("throughput")?;
        let alg = HashAlgorithm::Blake3;

        // One large file, from a file on disk as the engine sends it.
        let mb: u64 = std::env::var("HBSYNC_BIG_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        let size = mb << 20;
        let mut tmp = tempfile::tempfile()?;
        let mut w = std::io::BufWriter::new(&tmp);
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..size / 8 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            w.write_all(&x.to_le_bytes())?;
        }
        drop(w);
        tmp.seek(SeekFrom::Start(0))?;
        let (hash, _) = hash_reader(&mut tmp, alg)?;
        let rate = |what: &str, bytes: u64, t: Instant| {
            let secs = t.elapsed().as_secs_f64();
            println!(
                "{what:<28} {:>7.0} MB/s ({secs:.1} s)",
                bytes as f64 / secs / 1e6
            );
        };
        let meta = with(
            &file("big", dir.node, &[]),
            NodeContent::file("big", 1, size, false),
        );
        tmp.seek(SeekFrom::Start(0))?;
        let t = Instant::now();
        let reader = &mut tmp;
        let at = put(
            &meta,
            Upload {
                hash,
                size,
                reader,
                pace: None,
            },
        )?;
        rate(&format!("upload {mb} MiB"), size, t);
        let t = Instant::now();
        let mut sink = Hashing(Hasher::new(alg));
        remote.read(at.file_id, Some(0..size), &mut sink)?;
        assert_eq!(sink.0.finalize(), hash);
        rate(&format!("download {mb} MiB"), size, t);

        // Many small files: create, read, update.
        const N: usize = 200;
        let datas: Vec<Vec<u8>> = (0..N)
            .map(|i| format!("small file {i}").repeat(100).into_bytes())
            .collect();
        let per = |t: Instant| t.elapsed().as_secs_f64() * 1000.0 / N as f64;
        let t = Instant::now();
        let mut refs = Vec::new();
        for (i, data) in datas.iter().enumerate() {
            let meta = file(&format!("small-{i}"), dir.node, data);
            let at = put(&meta, upload(&mut Cursor::new(&data[..])))?;
            refs.push((meta, at));
        }
        println!("create {N} small, {:>5.1} ms each", per(t));
        let t = Instant::now();
        for ((_, at), data) in refs.iter().zip(&datas) {
            let mut back = Vec::new();
            remote.read(at.file_id, Some(0..data.len() as u64), &mut back)?;
            assert_eq!(&back, data);
        }
        println!("read {N} small,   {:>5.1} ms each", per(t));
        let t = Instant::now();
        for ((meta, at), data) in refs.iter().zip(&datas) {
            let new = [data.as_slice(), b"!"].concat();
            let content = NodeContent::file(&meta.content.name, 2, new.len() as u64, false);
            let mut source = Cursor::new(&new[..]);
            remote.update(
                at.file_id,
                at.version,
                &with(meta, content),
                Some(upload(&mut source)),
            )?;
        }
        println!("update {N} small, {:>5.1} ms each", per(t));
        Ok(())
    })();
    bench.cleanup()?;
    result
}
