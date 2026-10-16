//! Against a live identity: `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live
//! -- --ignored`. Needs the credentials `hbsync-register` saves for it. Each run works in its
//! own top-level folder and deletes it at the end.

use std::time::Duration;

use hbsync_dev::{OTHER_CLIENT, live_identity, run_name, saved_client, saved_client_as};
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
/// registration of the app on the same identity (`hbsync-register <identity> --as b`), gives
/// the same id; nothing is created when the app holds the slug.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn the_app_circle_is_found_by_its_slug() -> R {
    let identity = live_identity()?;
    let circle = hbsync_homebase::app::app_circle(&live_remote()?)?;
    assert_eq!(hbsync_homebase::app::app_circle(&live_remote()?)?, circle);
    let path = hbsync_dev::credentials_path_as(&identity, Some(OTHER_CLIENT))?;
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

/// Another client's write is announced with the file and version written (a minimal event);
/// this client's own isn't, unless it asks to hear its own writes (`excludeOwnWrites`,
/// odin-core #1971). The other client is a registration of its own (`hbsync-register <identity>
/// --as b`), as another computer is.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE) and a second client (hbsync-register --as b)"]
fn a_change_on_the_drive_is_announced() -> R {
    use hbsync_homebase::notify::OwnWrites;
    use hbsync_remote::contract::folder;
    use std::sync::mpsc::Receiver;
    use std::time::Instant;

    let identity = live_identity()?;
    let remote = live_remote()?;
    let other = HomebaseRemote::connect(saved_client_as(&identity, Some(OTHER_CLIENT))?)?;
    let bench = Bench::new(&remote, &run_name("notify"), Duration::ZERO)?;
    let listen = |own| -> Result<(Notifier, Receiver<Event>), Box<dyn std::error::Error>> {
        let (tx, rx) = std::sync::mpsc::channel();
        let n = Notifier::spawn_with(remote.client().clone(), remote.drive(), own, tx)?;
        Ok((n, rx))
    };
    let (quiet, quiet_rx) = listen(OwnWrites::Excluded)?;
    let (loud, loud_rx) = listen(OwnWrites::Heard)?;
    // The first event `what` takes within `patience`: `None` if none came.
    let wait = |rx: &Receiver<Event>,
                patience: Duration,
                what: &dyn Fn(&Event) -> bool|
     -> Result<Option<Duration>, String> {
        let start = Instant::now();
        loop {
            let left = patience.saturating_sub(start.elapsed());
            match rx.recv_timeout(left) {
                Ok(e) if what(&e) => return Ok(Some(start.elapsed())),
                Ok(Event::Lost(why) | Event::Refused(why)) => return Err(why),
                Ok(_) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return Ok(None),
                Err(e) => return Err(format!("waiting: {e}")),
            }
        }
    };
    let connected = |e: &Event| *e == Event::Connected;
    let result = (|| -> R {
        for rx in [&quiet_rx, &loud_rx] {
            wait(rx, Duration::from_secs(15), &connected)?.ok_or("never connected")?;
        }
        let (inside, _) = bench.folder("inside")?;

        // Another client's write: announced.
        let theirs = other.create(&folder("theirs", inside.node), None)?.at;
        bench.track(theirs.file_id);
        let is = |at: hbsync_types::RemoteRef| move |e: &Event| matches!(e, Event::Changed(Some(r)) if r.at() == at && !r.deleted);
        let took = wait(&quiet_rx, Duration::from_secs(15), &is(theirs))?
            .ok_or("another client's write wasn't announced")?;
        assert!(took < Duration::from_secs(2), "announced after {took:?}");
        println!("another client's write announced {took:?} after it returned");

        // This client's own: heard only by the subscription that asks for it.
        let mine = bench.folder("mine")?.1;
        wait(&loud_rx, Duration::from_secs(15), &is(mine))?
            .ok_or("an own write wasn't heard with OwnWrites::Heard")?;
        let echo = wait(
            &quiet_rx,
            Duration::from_secs(3),
            &|e| matches!(e, Event::Changed(Some(r)) if r.file_id == mine.file_id),
        )?;
        assert!(
            echo.is_none(),
            "an own write came back with excludeOwnWrites"
        );
        Ok(())
    })();
    drop((quiet, loud));
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

/// What this computer's own writes cost it on the notification socket, with and without
/// `excludeOwnWrites` (odin-core #1971): one folder synced as the daemon syncs it (the engine,
/// the notifications taken by `Notifications` and the `Puller`), through `n` files
/// (`ECHO_FILES`, 2,000 by default) created, deleted (to Recently deleted), then deleted for
/// good. Per phase: the bytes this process received (all its connections, [`socket_bytes`]),
/// the notification frames (every server frame, pongs too) and their bytes (text frames as
/// received), and the feed pages read
/// slim and whole (the engine's own reads and those the announcements asked for). Prints a
/// table. Works in a top-level folder of its own and deletes it for good at the end.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE); a measurement"]
fn own_write_echoes() -> R {
    use hbsync_homebase::notify::OwnWrites;
    let n = std::env::var("ECHO_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000usize);
    println!("{n} files created, deleted, then deleted for good by this computer");
    println!(
        "{:<9} {:<8} {:>12} {:>7} {:>11} {:>9} {:>6} {:>6}",
        "own", "phase", "bytes in", "frames", "frame B", "B/frame", "slim", "whole"
    );
    for own in [OwnWrites::Heard, OwnWrites::Excluded] {
        for row in echo_run(own, n)? {
            println!("{row}");
        }
    }
    Ok(())
}

/// One run of [`own_write_echoes`]: a table row per phase.
fn echo_run(
    own: hbsync_homebase::notify::OwnWrites,
    n: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    use hbsync_app::daemon::{Notifications, Puller};
    use hbsync_engine::EngineConfig;
    use hbsync_engine::testkit::TestClient;
    use hbsync_local::Filter;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    let remote = Counted::new(live_remote()?);
    let run = run_name("echoes");
    let mut a = TestClient::new(remote.clone(), "a", |cfg: &mut EngineConfig| {
        cfg.allow_mass_delete = true;
        cfg.exec.filter = Arc::new(Filter::load(&cfg.exec.root, cfg.exec.case_insensitive)?);
        Ok(())
    })?;
    // Only the run's folder: nothing else on the drive is downloaded.
    let ignore = hbsync_local::layout::ignore_file(&a.root);
    std::fs::create_dir_all(ignore.parent().ok_or("ignore file")?)?;
    std::fs::write(&ignore, format!("/*\n!/{run}/\n"))?;
    a.restart()?;
    let sync = |a: &mut TestClient<Counted>| -> R {
        loop {
            let report = a.sync_until_idle(&hbsync_app::today(), || {
                std::thread::sleep(Duration::from_millis(20));
            })?;
            assert!(report.failed.is_empty(), "failed {:?}", report.failed);
            if report.ops_done == 0 {
                return Ok(());
            }
        }
    };
    a.write(&format!("{run}/readme.txt"), "echoes")?;
    sync(&mut a)?;

    // The notifications, as the daemon's account takes them.
    let (tx, events) = std::sync::mpsc::channel();
    let notifier =
        Notifier::spawn_with(remote.inner.client().clone(), remote.inner.drive(), own, tx)?;
    let stop = Arc::new(AtomicBool::new(false));
    let taking = {
        let (puller, stop) = (Puller::spawn(Arc::clone(a.engine.drive())), stop.clone());
        std::thread::spawn(move || -> Result<(), String> {
            let mut notes = Notifications::new(puller, Instant::now());
            while !stop.load(Ordering::Relaxed) {
                match events.recv_timeout(Duration::from_millis(100)) {
                    Ok(Event::Lost(why) | Event::Refused(why)) => return Err(why),
                    Ok(event) => drop(notes.event(event, Instant::now(), false)),
                    Err(_) => {}
                }
                notes.tick(Instant::now(), false);
            }
            Ok(())
        })
    };
    // Announcements still on their way once a phase's work is done, and taken (1 s quiet).
    let settle = || std::thread::sleep(Duration::from_secs(4));
    settle();
    let mut rows = Vec::new();
    let result = (|| -> R {
        let mut phase = |name: &str, work: &mut dyn FnMut() -> R| -> R {
            let (bytes, frames, frame_bytes) = (
                socket_bytes()?,
                notifier.received().frames.load(Ordering::Relaxed),
                notifier.received().bytes.load(Ordering::Relaxed),
            );
            let (slim, whole) = remote.pages();
            work()?;
            settle();
            let frames = notifier.received().frames.load(Ordering::Relaxed) - frames;
            let frame_bytes = notifier.received().bytes.load(Ordering::Relaxed) - frame_bytes;
            let (slim2, whole2) = remote.pages();
            rows.push(format!(
                "{:<9} {name:<8} {:>12} {frames:>7} {frame_bytes:>11} {:>9} {:>6} {:>6}",
                format!("{own:?}"),
                socket_bytes()?.saturating_sub(bytes),
                frame_bytes / frames.max(1),
                slim2 - slim,
                whole2 - whole,
            ));
            Ok(())
        };
        phase("create", &mut || {
            for i in 0..n {
                a.write(&format!("{run}/files/f{i:05}.txt"), format!("file {i}"))?;
            }
            sync(&mut a)
        })?;
        phase("delete", &mut || {
            std::fs::remove_dir_all(a.root.join(&run).join("files"))?;
            sync(&mut a)
        })?;
        phase("empty", &mut || {
            let nodes: Vec<_> = a
                .recently_deleted(10_000)?
                .into_iter()
                .filter(|d| d.path == format!("{run}/files"))
                .map(|d| d.node)
                .collect();
            assert_eq!(nodes.len(), 1, "{run}/files in Recently deleted");
            let gone = a.engine.drive().empty(&nodes)?;
            assert_eq!(gone, n + 1, "every file and their folder");
            Ok(())
        })?;
        Ok(())
    })();
    stop.store(true, Ordering::Relaxed);
    let taken = taking
        .join()
        .map_err(|_| "the notifications' thread panicked")?;
    drop(notifier);
    // The run's folder, deleted for good.
    std::fs::remove_dir_all(a.root.join(&run))?;
    sync(&mut a)?;
    let top: Vec<_> = a
        .recently_deleted(10_000)?
        .into_iter()
        .filter(|d| d.path == run)
        .map(|d| d.node)
        .collect();
    a.engine.drive().empty(&top)?;
    result?;
    taken?;
    Ok(rows)
}

/// A remote that counts the feed pages read through it, slim and whole.
#[derive(Clone)]
struct Counted {
    inner: std::sync::Arc<HomebaseRemote>,
    slim: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    whole: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Counted {
    fn new(inner: HomebaseRemote) -> Self {
        Counted {
            inner: std::sync::Arc::new(inner),
            slim: Default::default(),
            whole: Default::default(),
        }
    }

    /// Feed pages read so far: slim, whole.
    fn pages(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.slim.load(Relaxed), self.whole.load(Relaxed))
    }
}

impl Remote for Counted {
    fn changes(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::ChangesPage> {
        self.whole
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.changes(cursor, limit)
    }

    fn change_refs(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> hbsync_remote::Result<hbsync_remote::RefsPage> {
        self.slim.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.change_refs(cursor, limit)
    }

    fn header(
        &self,
        file_id: hbsync_types::RemoteFileId,
    ) -> hbsync_remote::Result<hbsync_remote::RemoteHeader> {
        self.inner.header(file_id)
    }

    fn create(
        &self,
        node: &hbsync_remote::NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.inner.create(node, payload)
    }

    fn update(
        &self,
        target: &hbsync_types::Target,
        meta: &hbsync_remote::NodeMeta,
        payload: Option<hbsync_remote::Upload<'_>>,
    ) -> hbsync_remote::Result<hbsync_types::Target> {
        self.inner.update(target, meta, payload)
    }

    fn delete(
        &self,
        file_id: hbsync_types::RemoteFileId,
        expected: hbsync_types::VersionTag,
    ) -> hbsync_remote::Result<()> {
        self.inner.delete(file_id, expected)
    }

    fn read(
        &self,
        file_id: hbsync_types::RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn std::io::Write,
    ) -> hbsync_remote::Result<u64> {
        self.inner.read(file_id, range, sink)
    }
}
