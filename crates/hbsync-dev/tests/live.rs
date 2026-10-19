//! Against a live identity: `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live
//! -- --ignored`. Needs the credentials `hbsync-register` saves for it. Each run works in its
//! own top-level folder and deletes it at the end.

use std::time::Duration;

use hbsync_dev::{
    OTHER_CLIENT, RunFolder, live_identity, on_threads, run_name, saved_client, saved_client_as,
};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_remote::Remote;
use hbsync_remote::contract::{Bench, feed_end};

type R = Result<(), Box<dyn std::error::Error>>;

/// The contract's feed checks compare pages read moments apart, and another test's writes (the
/// 2,000-file ones above all) land between the reads: the contract runs alone, the rest of this
/// binary's tests side by side.
static DRIVE: std::sync::RwLock<()> = std::sync::RwLock::new(());

fn alone() -> std::sync::RwLockWriteGuard<'static, ()> {
    DRIVE.write().unwrap_or_else(|e| e.into_inner())
}

fn beside_others() -> std::sync::RwLockReadGuard<'static, ()> {
    DRIVE.read().unwrap_or_else(|e| e.into_inner())
}

fn live_remote() -> Result<HomebaseRemote, Box<dyn std::error::Error>> {
    Ok(HomebaseRemote::connect(saved_client(&live_identity()?)?)?)
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn the_contract_holds_on_a_live_drive() -> R {
    let _drive = alone();
    let remote = live_remote()?;
    let run_folder = run_name("contract");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::from_millis(20))?;
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
    let _drive = beside_others();
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
    let _drive = beside_others();
    use hbsync_homebase::notify::OwnWrites;
    use hbsync_remote::contract::folder;
    use std::sync::mpsc::Receiver;
    use std::time::Instant;

    let identity = live_identity()?;
    let remote = live_remote()?;
    let other = HomebaseRemote::connect(saved_client_as(&identity, Some(OTHER_CLIENT))?)?;
    let run_folder = run_name("notify");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::ZERO)?;
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
    let _drive = beside_others();
    use hbsync_homebase::drive::sealed::sealed_len;
    use hbsync_remote::contract::file;

    let remote = live_remote()?;
    let run_folder = run_name("sealed");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::ZERO)?;
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
    let _drive = beside_others();
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
    let run_folder = run_name("throughput");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::ZERO)?;
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
    let _drive = beside_others();
    use hbsync_remote::RemoteBody;
    use hbsync_remote::contract::{file, with};
    use hbsync_types::schema::NodeContent;
    use hbsync_types::{FileToken, Target};
    let remote = live_remote()?;
    let run_folder = run_name("stale-token");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::ZERO)?;
    let result = (|| -> R {
        let (folder, _) = bench.folder("d")?;
        let meta = file("f.txt", folder.node, b"one");
        let at = bench.create(&meta, Some(b"one"))?;
        let stale = Target {
            at,
            token: Some(FileToken(vec![0x5a; 64].into())),
            changed_at: None,
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
    let _drive = beside_others();
    use hbsync_remote::contract::{file, removed};
    use std::sync::Mutex;
    let remote = live_remote()?;
    let n = std::env::var("FEED_SIZES_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000usize);
    let run_folder = run_name("feed-sizes");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::from_millis(20))?;
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
            Ok::<_, hbsync_remote::RemoteError>(())
        })?;
        let made = made.into_inner().unwrap_or_else(|e| e.into_inner());
        assert_eq!((created, made.len()), (n, n), "every create went through");
        let after_creates = bench.cursor_now()?;
        let removed_count = on_threads(n, |i| {
            let (meta, at) = &made[i];
            remote.update(&bench.target(*at), &removed(meta), None)?;
            Ok::<_, hbsync_remote::RemoteError>(())
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
/// counts carry on between two readings only while the connections stay open: the HTTP client
/// closes them once no call has run for 15 s, so a phase with such a pause undercounts.
fn socket_bytes() -> Result<u64, Box<dyn std::error::Error>> {
    socket_count("bytes_received:")
}

/// [`socket_bytes`] for another of `ss`'s counters (`bytes_sent:`).
fn socket_count(counter: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let out = std::process::Command::new("ss").arg("-tinpH").output()?;
    let me = format!("pid={},", std::process::id());
    let (mut total, mut mine) = (0u64, false);
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if !line.starts_with(char::is_whitespace) {
            mine = line.contains(&me);
        } else if mine
            && let Some(n) = line
                .split_whitespace()
                .find_map(|w| w.strip_prefix(counter))
        {
            total += n.parse::<u64>()?;
        }
    }
    Ok(total)
}

/// What this computer's own writes cost it on the notification socket, with and without
/// `excludeOwnWrites` (odin-core #1971): one folder synced as the daemon syncs it (the engine,
/// the notifications taken by `Notifications` and the `Puller`; a local change's sync reads
/// the feed only while the picture isn't current), through `n` files
/// (`ECHO_FILES`, 2,000 by default) created, deleted (to Recently deleted), then deleted for
/// good. Per phase: the bytes this process received (all its connections, [`socket_bytes`]),
/// the notification frames (every server frame, pongs too) and their bytes (text frames as
/// received), and the feed pages read
/// slim and whole (the engine's own reads and those the announcements asked for). Prints a
/// table. Works in a top-level folder of its own and deletes it for good at the end.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE); a measurement"]
fn own_write_echoes() -> R {
    let _drive = beside_others();
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
    let _guard = RunFolder::new(&run)?;
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
    // Until idle, reading the feed first (`read`) or as the daemon syncs a local change: only
    // if the account's picture isn't current (the notifications connected, as `Notifications`
    // sets it).
    let sync = |a: &mut TestClient<Counted>, read: bool| -> R {
        loop {
            let report = a.sync_until_idle(&hbsync_app::today(), read, || {
                std::thread::sleep(Duration::from_millis(20));
            })?;
            assert!(report.failed.is_empty(), "failed {:?}", report.failed);
            if report.ops_done == 0 {
                return Ok(());
            }
        }
    };
    a.write(&format!("{run}/readme.txt"), "echoes")?;
    sync(&mut a, true)?;

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
            sync(&mut a, false)
        })?;
        phase("delete", &mut || {
            std::fs::remove_dir_all(a.root.join(&run).join("files"))?;
            sync(&mut a, false)
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
    sync(&mut a, true)?;
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

    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }

    fn holding(
        &self,
        folders: &[hbsync_types::NodeId],
        holds: hbsync_remote::Holds<'_>,
    ) -> hbsync_remote::Result<std::collections::HashSet<hbsync_types::NodeId>> {
        self.inner.holding(folders, holds)
    }

    fn hold_back(&self) -> Duration {
        self.inner.hold_back()
    }

    fn delete_for_good(
        &self,
        files: &[hbsync_types::RemoteRef],
    ) -> hbsync_remote::Result<hbsync_remote::ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(
        &self,
        files: &[hbsync_types::Target],
        removed: bool,
    ) -> hbsync_remote::Result<hbsync_remote::Answers<hbsync_types::Target>> {
        self.inner.set_removed(files, removed)
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
        files: &[hbsync_types::RemoteRef],
    ) -> hbsync_remote::Result<hbsync_remote::Deletes> {
        self.inner.delete(files)
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

/// Each write's answer carries `updated`, the time the feed then shows for it (odin-core
/// #1971): a create, a new version, a removal and a restore (PATCH), the archival-status batch,
/// and the soft-delete batch (the tombstone's, odin-core #1977). One file per kind, as the feed shows only a file's last write; their rows
/// are read minimal from a cursor taken before. Prints a table.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn write_answers_carry_the_feeds_time() -> R {
    let _drive = beside_others();
    use hbsync_homebase::drive::wire::{Shape, feed_query};
    use hbsync_remote::contract::{file, removed};
    use std::collections::HashMap;

    let remote = live_remote()?;
    let run_folder = run_name("answer-times");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::ZERO)?;
    let result = (|| -> R {
        let cursor = bench.cursor_now()?;
        let (dir, _) = bench.folder("d")?;
        let made = |name: &str| -> Result<_, Box<dyn std::error::Error>> {
            let meta = file(name, dir.node, name.as_bytes());
            let written = remote.create(
                &meta,
                Some(hbsync_remote::contract::upload(&mut std::io::Cursor::new(
                    name.as_bytes(),
                ))),
            )?;
            bench.track(written.at.file_id);
            Ok((meta, written))
        };
        // (what, file, version, the answer's updated)
        let mut answers = Vec::new();
        let (_, created) = made("created")?;
        answers.push(("create (POST)", created.at, created.changed_at));
        let (meta, w) = made("edited")?;
        let data = b"a new version";
        let edited = remote.update(
            &w,
            &hbsync_remote::contract::with(
                &meta,
                hbsync_types::schema::NodeContent::file("edited", 1, data.len() as u64, false),
            ),
            Some(hbsync_remote::contract::upload(&mut std::io::Cursor::new(
                &data[..],
            ))),
        )?;
        answers.push(("version (PATCH)", edited.at, edited.changed_at));
        let (meta, w) = made("removed")?;
        let gone = remote.update(&w, &removed(&meta), None)?;
        answers.push(("removal (PATCH)", gone.at, gone.changed_at));
        let (meta, w) = made("restored")?;
        let gone = remote.update(&w, &removed(&meta), None)?;
        let back = remote.update(&gone, &meta, None)?;
        answers.push(("restore (PATCH)", back.at, back.changed_at));
        let (_, w) = made("archived")?;
        let batch: serde_json::Value = remote.client().post(
            &format!("{}/update-archival-status-batch", remote.files()),
            &serde_json::json!({"archivalStatus": 2, "files": [
                {"fileId": w.at.file_id.0, "versionTag": w.at.version.0}
            ]}),
        )?;
        let r = &batch["results"][0];
        assert!(r["errorCode"].is_null(), "{batch}");
        let version = hbsync_types::VersionTag(
            r["newVersionTag"]
                .as_str()
                .ok_or("newVersionTag")?
                .parse()?,
        );
        answers.push((
            "archival-status batch",
            hbsync_types::RemoteRef {
                file_id: w.at.file_id,
                version,
            },
            r["updated"].as_i64(),
        ));
        let (_, w) = made("deleted")?;
        let deleted = remote.delete(&[w.at])?.single()?;
        let tomb = remote.header(w.at.file_id)?;
        answers.push((
            "soft-delete batch",
            hbsync_types::RemoteRef {
                file_id: w.at.file_id,
                version: tomb.version,
            },
            deleted,
        ));

        // The feed's rows since the cursor: each file's last write.
        std::thread::sleep(Duration::from_millis(50));
        let mut rows: HashMap<String, (String, Option<i64>)> = HashMap::new();
        let mut cursor = Some(cursor);
        loop {
            let page: serde_json::Value = remote.client().post(
                &format!("{}/query-batch", remote.files()),
                &feed_query(cursor.as_deref(), 1000, Shape::Minimal),
            )?;
            for row in page["fileRefs"].as_array().into_iter().flatten() {
                rows.insert(
                    row["fileId"].as_str().unwrap_or_default().replace('-', ""),
                    (
                        row["versionTag"]
                            .as_str()
                            .unwrap_or_default()
                            .replace('-', ""),
                        row["updated"].as_i64(),
                    ),
                );
            }
            cursor = page["cursorState"].as_str().map(str::to_owned).or(cursor);
            if page["hasMoreRows"].as_bool() != Some(true) {
                break;
            }
        }
        println!(
            "{:<24} {:>15} {:>15} {:>8}",
            "write", "answer updated", "feed updated", "equal"
        );
        let mut unequal = Vec::new();
        for (what, at, answered) in &answers {
            let (version, fed) = rows
                .get(&at.file_id.0.simple().to_string())
                .cloned()
                .ok_or(format!("{what}: no feed row"))?;
            let same = answered.is_some()
                && *answered == fed
                && version == at.version.0.simple().to_string();
            println!(
                "{what:<24} {:>15} {:>15} {same:>8}",
                answered.map_or("null".into(), |t| t.to_string()),
                fed.map_or("null".into(), |t| t.to_string()),
            );
            if !same {
                unequal.push(*what);
            }
        }
        assert!(unequal.is_empty(), "not the feed's time: {unequal:?}");
        Ok(())
    })();
    bench.cleanup()?;
    result
}

/// Whether another client's write is in the feed as soon as its answer came: client b creates a
/// folder, and this client reads the feed from the cursor it had before, at once; when the row
/// isn't there yet, it reads again every 20 ms until it is. Odd rounds read from the cursor once
/// first (an empty answer the server may keep: `QueryBatchCached`), as a client that just read
/// to the end has. Prints, per late round, the row's `updated` against the time b's answer
/// came (Unix ms, this machine's clock, the server's too on the dev host), the first reads'
/// answers, and how long the row was missing; fails only when a read said no more rows
/// without it (the client reads on only while there are: odin-core #1906, #1909).
/// `FEED_AT_ONCE_ROUNDS` sets the rounds (40).
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE) and a second client (hbsync-register --as b)"]
fn another_clients_write_is_in_the_feed_at_once() -> R {
    let _drive = beside_others();
    use hbsync_remote::contract::folder;
    use std::time::Instant;
    let rounds = std::env::var("FEED_AT_ONCE_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40usize);
    let identity = live_identity()?;
    let remote = live_remote()?;
    let other = HomebaseRemote::connect(saved_client_as(&identity, Some(OTHER_CLIENT))?)?;
    let run_folder = run_name("feed-at-once");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::ZERO)?;
    let result = (|| -> R {
        let (dir, _) = bench.folder("d")?;
        let mut cursor = bench.cursor_now()?;
        let mut late = Vec::new();
        for i in 0..rounds {
            // Other tests may write meanwhile: from the feed's end, and primed only where the
            // read there came back empty.
            cursor = feed_end(&remote, Some(&cursor))?;
            let primed = i % 2 == 1 && {
                let page = remote.change_refs(Some(&cursor), 100)?;
                page.refs.is_empty() && !page.has_more
            };
            let w = other.create(&folder(&format!("f{i}"), dir.node), None)?;
            bench.track(w.at.file_id);
            let (written, answered_ms) = (Instant::now(), hbsync_types::unix_now_ms());
            let mut misses = Vec::new();
            loop {
                let page = remote.change_refs(Some(&cursor), 100)?;
                if let Some(r) = page.refs.iter().find(|r| r.file_id == w.at.file_id) {
                    if !misses.is_empty() {
                        late.push(format!(
                            "round {i} primed={primed}: missing for {:?} ({} reads: {misses:?}); \
                             row updated {:?}, answer's updated {:?}, answer came at {answered_ms}",
                            written.elapsed(),
                            misses.len(),
                            r.changed_at,
                            w.changed_at
                        ));
                    }
                    cursor = page.cursor;
                    break;
                }
                // Other rows first: on through them.
                if !page.refs.is_empty() && page.has_more {
                    cursor = page.cursor;
                    continue;
                }
                misses.push(format!(
                    "{} rows more={} at +{}ms",
                    page.refs.len(),
                    page.has_more,
                    written.elapsed().as_millis()
                ));
                assert!(
                    written.elapsed() < Duration::from_secs(10),
                    "round {i}: not in the feed after 10 s: {misses:?}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        println!("{} of {rounds} rounds read the write late", late.len());
        for l in &late {
            println!("  {l}");
        }
        // Held back (more rows to come) costs a read; hidden (no more rows) would miss it.
        let hidden: Vec<&String> = late.iter().filter(|l| l.contains("more=false")).collect();
        assert!(hidden.is_empty(), "hidden, not held back: {hidden:#?}");
        Ok(())
    })();
    bench.cleanup()?;
    result
}

/// A regression check of odin-core #1909 (fixed by its #1973, 4eb50e5db): a query-batch
/// answer computed while another client's write commits must not be kept (`QueryBatchCached`,
/// keyed by the query, not by who asks) and hide that write. The client relies on it: every
/// read asks the same query, and an announced change is read for once. A thread reads the feed
/// from a cursor at the end over and over (pages of 100, as a client caught up would), while
/// client b creates a folder; once b's answer came, a read from the same cursor (the same
/// cache key) and one with pages of 99 (another key) say whether the row is there, reading on
/// while more rows follow (other tests' writes may come first). Fails on a round where the
/// same query hid it. `CACHE_RACE_ROUNDS` sets the rounds (60).
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE) and a second client (hbsync-register --as b)"]
fn a_page_read_during_a_write_may_be_kept_stale() -> R {
    let _drive = beside_others();
    use hbsync_remote::contract::folder;
    use std::sync::atomic::{AtomicBool, Ordering};
    let rounds = std::env::var("CACHE_RACE_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60usize);
    let identity = live_identity()?;
    let remote = live_remote()?;
    let other = HomebaseRemote::connect(saved_client_as(&identity, Some(OTHER_CLIENT))?)?;
    let run_folder = run_name("cache-race");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::ZERO)?;
    let result = (|| -> R {
        let (dir, _) = bench.folder("d")?;
        let mut stale = Vec::new();
        let mut end = bench.cursor_now()?;
        for i in 0..rounds {
            let cursor = end.clone();
            let stop = AtomicBool::new(false);
            let (seen_same, seen_other) = std::thread::scope(|scope| -> Result<_, String> {
                let reader = scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        let _ = remote.change_refs(Some(&cursor), 100);
                    }
                });
                std::thread::sleep(Duration::from_millis(5));
                let w = other
                    .create(&folder(&format!("f{i}"), dir.node), None)
                    .map_err(|e| e.to_string())?;
                bench.track(w.at.file_id);
                stop.store(true, Ordering::Relaxed);
                let _ = reader.join();
                // Past the hold-back window: a row not shown now is hidden, not held back.
                std::thread::sleep(Duration::from_millis(1100));
                // Whether the first page from the cursor has the row and says more rows follow,
                // and whether reading on, as the client does (other tests' writes may come
                // first), finds it.
                let has = |limit| -> Result<(bool, bool, bool), String> {
                    let read = |at: &str| {
                        remote
                            .change_refs(Some(at), limit)
                            .map_err(|e| e.to_string())
                    };
                    let mut p = read(&cursor)?;
                    let seen = |p: &hbsync_remote::RefsPage| {
                        p.refs.iter().any(|r| r.file_id == w.at.file_id)
                    };
                    let (first, more) = (seen(&p), p.has_more);
                    let mut found = first;
                    while !found && p.has_more {
                        p = read(&p.cursor.clone())?;
                        found = seen(&p);
                    }
                    Ok((first, more, found))
                };
                let (same, other_key) = (has(100)?, has(99)?);
                // How long it stays hidden without another write: the first round looks again
                // ten seconds on.
                if i == 0 && !same.2 {
                    std::thread::sleep(Duration::from_secs(10));
                    println!(
                        "round 0, 11 s after the write: same key (seen, more, read on) {:?}",
                        has(100)?
                    );
                }
                Ok((same, other_key))
            })?;
            // The feed's end again, for the next round.
            end = feed_end(&remote, Some(&end))?;
            if !seen_same.2 {
                stale.push(format!(
                    "round {i}: same key {seen_same:?}, another key {seen_other:?} \
                     (seen, more, read on)"
                ));
            } else if !seen_same.0 {
                println!("round {i}: past the first page: {seen_same:?} (seen, more, read on)");
            }
        }
        println!(
            "{} of {rounds} rounds: the write hidden from the same query",
            stale.len()
        );
        for s in &stale {
            println!("  {s}");
        }
        assert!(
            stale.is_empty(),
            "a kept answer hid a write (odin-core #1909)"
        );
        Ok(())
    })();
    bench.cleanup()?;
    result
}

/// A purge on the server's hard delete (`hard-delete-batch`, odin-core #1968): `n` files
/// (`HARD_PURGE_FILES`, 2,100 by default) created, removed, then deleted for good a batch of
/// 1,000 at a time, as the purge sends them. Prints each batch's time and bytes (this process's
/// connections, the notification socket included), and checks that the notifier announces
/// every hard delete as deleted (its `previousFileRef`), and none of the feed's rows is left.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE); a measurement"]
fn hard_purge() -> R {
    let _drive = beside_others();
    use hbsync_homebase::drive::BATCH_FILES;
    use hbsync_remote::contract::{file, removed};
    use hbsync_types::RemoteRef;
    use std::collections::HashSet;
    use std::sync::Mutex;
    let remote = live_remote()?;
    let n = std::env::var("HARD_PURGE_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2100usize);
    let run_folder = run_name("hard-purge");
    let _guard = RunFolder::new(&run_folder)?;
    let bench = Bench::new(&remote, &run_folder, Duration::from_millis(20))?;
    let result = (|| -> R {
        let (folder, _) = bench.folder("files")?;
        let made = Mutex::new(Vec::with_capacity(n));
        let done = on_threads(n, |i| {
            let data = format!("file {i}");
            let meta = file(&format!("f{i:05}.txt"), folder.node, data.as_bytes());
            let at = bench.create(&meta, Some(data.as_bytes()))?;
            let gone = remote.update(&bench.target(at), &removed(&meta), None)?;
            made.lock().unwrap_or_else(|e| e.into_inner()).push(gone.at);
            Ok::<_, hbsync_remote::RemoteError>(())
        })?;
        let made: Vec<RemoteRef> = made.into_inner().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            (done, made.len()),
            (n, n),
            "every create and removal went through"
        );
        let after = bench.cursor_now()?;

        let (tx, events) = std::sync::mpsc::channel();
        // This client's own deletes: heard (`excludeOwnWrites` would leave them out).
        let notifier = Notifier::spawn_with(
            remote.client().clone(),
            remote.drive(),
            hbsync_homebase::notify::OwnWrites::Heard,
            tx,
        )?;
        // Subscribed before the deletes.
        std::thread::sleep(Duration::from_secs(3));
        println!("{n} removed files deleted for good, {BATCH_FILES} a request");
        println!(
            "{:<8} {:>6} {:>10} {:>12} {:>12}",
            "batch", "files", "ms", "bytes sent", "bytes recv"
        );
        let mut total = Duration::ZERO;
        for (i, batch) in made.chunks(BATCH_FILES).enumerate() {
            let (sent, recv) = (socket_count("bytes_sent:")?, socket_bytes()?);
            let start = std::time::Instant::now();
            let results = remote.delete_for_good(batch)?.into_answers()?;
            let took = start.elapsed();
            total += took;
            // A socket closed meanwhile takes its counts along: none then.
            let (sent, recv) = (
                socket_count("bytes_sent:")?.saturating_sub(sent),
                socket_bytes()?.saturating_sub(recv),
            );
            assert!(results.iter().all(Result::is_ok), "{results:?}");
            println!(
                "{:<8} {:>6} {:>10} {sent:>12} {recv:>12}",
                i + 1,
                batch.len(),
                took.as_millis()
            );
        }
        println!("total    {n:>6} {:>10}", total.as_millis());
        // The announcements trail the deletes.
        std::thread::sleep(Duration::from_secs(15));
        drop(notifier);
        let gone: HashSet<_> = events
            .try_iter()
            .filter_map(|e| match e {
                Event::Changed(Some(r)) if r.deleted => Some(r.file_id),
                _ => None,
            })
            .collect();
        let missed = made.iter().filter(|r| !gone.contains(&r.file_id)).count();
        println!("announced as deleted: {}", n - missed);
        assert_eq!(missed, 0, "every hard delete announced, as deleted");
        let ids: HashSet<String> = made.iter().map(|r| r.file_id.0.to_string()).collect();
        let left = bench.changes_after(Some(&after))?.0;
        assert!(
            !left.iter().any(|h| ids.contains(&h.file_id.0.to_string())),
            "nothing of them in the feed"
        );
        Ok(())
    })();
    bench.cleanup()?;
    result
}
