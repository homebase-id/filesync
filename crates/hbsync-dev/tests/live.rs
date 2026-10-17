//! Against a live identity: `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live
//! -- --ignored`. Needs the credentials `hbsync-register` saves for it. Each run works in its
//! own top-level folder and deletes it at the end.

use std::time::Duration;

use hbsync_dev::{live_identity, run_name, saved_client};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_remote::Remote;
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
    Ok(())
}

/// Every registration finds the app's one circle by its slug: asking twice, or from a second
/// registration of the app on the same identity (its credentials in `<identity>.b.json` next to
/// the first's, client "b"), gives the same id; nothing is created when the app holds the slug.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn the_app_circle_is_found_by_its_slug() -> R {
    let identity = live_identity()?;
    let circle = hbsync_homebase::app::app_circle(&live_remote()?)?;
    assert_eq!(hbsync_homebase::app::app_circle(&live_remote()?)?, circle);
    let path = hbsync_dev::credentials_path(&format!("{identity}.b"))?;
    let Some(second) = hbsync_homebase::Credentials::load(&path)? else {
        println!(
            "skipped the second registration: no credentials at {}",
            path.display()
        );
        return Ok(());
    };
    assert_eq!(second.identity, identity);
    let other = HomebaseRemote::connect(hbsync_homebase::Client::new(&second)?)?;
    assert_eq!(hbsync_homebase::app::app_circle(&other)?, circle);
    println!("both registrations find the circle {circle}");
    Ok(())
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn a_change_on_the_drive_is_announced() -> R {
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("notify"), Duration::ZERO)?;
    let (tx, rx) = std::sync::mpsc::channel();
    let notifier = Notifier::spawn(remote.client().clone(), remote.drive(), tx);
    let wait = |what: &dyn Fn(&Event) -> bool| -> Result<(), String> {
        loop {
            match rx.recv_timeout(Duration::from_secs(15)) {
                Ok(e) if what(&e) => return Ok(()),
                Ok(Event::Lost(why) | Event::Refused(why)) => return Err(why),
                Ok(_) => {}
                Err(e) => return Err(format!("waiting: {e}")),
            }
        }
    };
    let result = (|| -> R {
        wait(&|e| *e == Event::Connected)?;
        let start = std::time::Instant::now();
        let (_, at) = bench.folder("inside")?;
        // The write is announced with the file and version written (a minimal event).
        wait(&|e| matches!(e, Event::Changed(Some(r)) if r.at() == at && !r.deleted))?;
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
    use hbsync_remote::Upload;
    use hbsync_remote::contract::{file, upload, with};
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
        let at = remote.create(
            &meta,
            Some(Upload {
                hash,
                size,
                reader,
                pace: None,
            }),
        )?;
        bench.track(at.at.file_id);
        rate(&format!("upload {mb} MiB"), size, t);
        let t = Instant::now();
        let mut sink = Hashing(Hasher::new(alg));
        remote.read(at.at.file_id, Some(0..size), &mut sink)?;
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
            let at = bench.create(&meta, Some(data))?;
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
                &bench.target(*at),
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

/// A token that doesn't open (sealed under an earlier login's shared secret) leaves the update
/// to read the header first, as without one: it goes through, and the token it returns opens.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn a_token_that_doesnt_open_falls_back_to_the_header() -> R {
    use hbsync_remote::RemoteBody;
    use hbsync_remote::contract::{file, with};
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{FileToken, Target};
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("stale-token"), Duration::ZERO)?;
    let result = (|| -> R {
        let (folder, _) = bench.folder("d")?;
        let meta = file("f.txt", folder.node, b"one");
        let at = bench.create(&meta, Some(b"one"))?;
        let stale = Target {
            at,
            token: Some(FileToken(vec![0x5a; 64].into())),
        };
        let renamed = with(&meta, NodeContent::file("g.txt", 1, 3, false));
        let written = remote.update(&stale, &renamed, None)?;
        let again = with(&meta, NodeContent::file("h.txt", 1, 3, false));
        remote.update(&written, &again, None)?;
        match remote.header(at.file_id)?.body {
            RemoteBody::Live { content, .. } => assert_eq!(content.name, "h.txt"),
            RemoteBody::Deleted => panic!("deleted"),
        }
        Ok(())
    })();
    bench.cleanup()?;
    result
}

/// What the feed costs whole and slim (odin-core #1967): `n` files (`FEED_SIZES_FILES`, 2,000 by
/// default) created and then removed, read back as this device's own writes coming back (the
/// echo: slim only, its versions are known) and as a catch-up from before the creates (every
/// row new). Bytes are what this process's connections received ([`socket_bytes`]), so nothing
/// else may run in it meanwhile. Prints a table.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE); a measurement"]
fn feed_sizes() -> R {
    use hbsync_remote::contract::{file, removed};
    use std::sync::Mutex;
    let remote = live_remote()?;
    let n = std::env::var("FEED_SIZES_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000usize);
    let bench = Bench::new(&remote, &run_name("feed-sizes"), Duration::from_millis(20))?;
    let result = (|| -> R {
        let before = bench.cursor_now()?;
        let (folder, _) = bench.folder("files")?;
        let made = Mutex::new(Vec::with_capacity(n));
        let created = on_threads(n, |i| {
            let data = format!("file {i}");
            let meta = file(&format!("f{i:05}.txt"), folder.node, data.as_bytes());
            let at = bench.create(&meta, Some(data.as_bytes()))?;
            made.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((meta, at));
            Ok(())
        })?;
        let made = made.into_inner().unwrap_or_else(|e| e.into_inner());
        assert_eq!((created, made.len()), (n, n), "every create went through");
        let after_creates = bench.cursor_now()?;
        let removed_count = on_threads(n, |i| {
            let (meta, at) = &made[i];
            remote.update(&bench.target(*at), &removed(meta), None)?;
            Ok(())
        })?;
        assert_eq!(removed_count, n, "every removal went through");
        std::thread::sleep(Duration::from_millis(50));
        println!("{n} files created, then removed; feed pages of {PAGE} rows");
        println!(
            "{:<32} {:>7} {:>9} {:>12} {:>10}",
            "read", "rows", "requests", "bytes", "bytes/row"
        );
        let cases: [(&str, &str, Read); 5] = [
            ("echo: whole", &after_creates, Read::Whole),
            ("echo: slim", &after_creates, Read::Slim),
            ("catch-up: whole", &before, Read::Whole),
            (
                "catch-up: each page slim+whole",
                &before,
                Read::SlimThenWhole,
            ),
            ("catch-up: as the engine reads", &before, Read::Engine),
        ];
        for (name, from, how) in cases {
            let start = socket_bytes()?;
            let (rows, requests) = read_feed(&remote, from, how)?;
            // A connection that closed meanwhile takes its count along: never below zero.
            let bytes = socket_bytes()?.saturating_sub(start);
            println!(
                "{name:<32} {rows:>7} {requests:>9} {bytes:>12} {:>10}",
                bytes / rows.max(1) as u64
            );
        }
        Ok(())
    })();
    let cleanup = bench.cleanup();
    result?;
    cleanup?;
    Ok(())
}

const PAGE: usize = 500;

/// `work` for each of `0..n` on 8 threads: how many went through, or the first error.
fn on_threads(
    n: usize,
    work: impl Fn(usize) -> Result<(), hbsync_remote::RemoteError> + Sync,
) -> Result<usize, hbsync_remote::RemoteError> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (next, done) = (AtomicUsize::new(0), AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            return Ok(());
                        }
                        work(i)?;
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        threads.into_iter().try_for_each(|t| {
            t.join().unwrap_or_else(|_| {
                Err(hbsync_remote::RemoteError::Fatal(
                    "a thread panicked".into(),
                ))
            })
        })
    })?;
    Ok(done.into_inner())
}

#[derive(Clone, Copy)]
enum Read {
    Whole,
    Slim,
    /// Every page slim first, then again whole (it holds something new).
    SlimThenWhole,
    /// As the engine reads a run of news: the first page slim, then again whole, and whole
    /// pages after it (the rest of this run is new too).
    Engine,
}

/// Reads the feed from `from` to its end `how`: the rows and the requests it took.
fn read_feed(
    remote: &HomebaseRemote,
    from: &str,
    how: Read,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let (mut cursor, mut rows, mut requests) = (from.to_owned(), 0, 0);
    let mut how = how;
    loop {
        let (count, next, more) = match how {
            Read::Engine => {
                // The first page slim, then whole; the rest whole.
                how = Read::Whole;
                let r = remote.change_refs(Some(&cursor), PAGE)?;
                requests += 1;
                if r.refs.is_empty() {
                    (0, r.cursor, r.has_more)
                } else {
                    let p = remote.changes(Some(&cursor), PAGE)?;
                    requests += 1;
                    (p.entries.len() + p.skipped.len(), p.cursor, p.has_more)
                }
            }
            Read::Whole => {
                let p = remote.changes(Some(&cursor), PAGE)?;
                requests += 1;
                (p.entries.len() + p.skipped.len(), p.cursor, p.has_more)
            }
            Read::Slim | Read::SlimThenWhole => {
                let r = remote.change_refs(Some(&cursor), PAGE)?;
                requests += 1;
                if matches!(how, Read::SlimThenWhole) && !r.refs.is_empty() {
                    let p = remote.changes(Some(&cursor), PAGE)?;
                    requests += 1;
                    (p.entries.len() + p.skipped.len(), p.cursor, p.has_more)
                } else {
                    (r.refs.len(), r.cursor, r.has_more)
                }
            }
        };
        rows += count;
        cursor = next;
        if !more {
            return Ok((rows, requests));
        }
    }
}

/// Bytes this process's open TCP connections have received (`ss -tip`: TLS, after gzip). The
/// HTTP client keeps its connections open, so the counts carry on between two readings.
fn socket_bytes() -> Result<u64, Box<dyn std::error::Error>> {
    let out = std::process::Command::new("ss").arg("-tinpH").output()?;
    let me = format!("pid={},", std::process::id());
    let (mut total, mut mine) = (0u64, false);
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if !line.starts_with(char::is_whitespace) {
            mine = line.contains(&me);
        } else if mine
            && let Some(n) = line
                .split_whitespace()
                .find_map(|w| w.strip_prefix("bytes_received:"))
        {
            total += n.parse::<u64>()?;
        }
    }
    Ok(total)
}
