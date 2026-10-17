//! Who hears what on the notification socket, against a live identity: computers on one
//! identity, each its own client registration, each subscribed as the daemon subscribes
//! (minimal events, `excludeOwnWrites`: odin-core #1971). Every scenario's writes are logged with
//! the route that made them; afterwards each computer's socket events are checked against the
//! feed read from a cursor the harness saved before the scenario (its own copy: no client reads
//! with it) and against the writes themselves:
//! - a feed row (or a write) of another computer that its socket didn't announce is a **missed
//!   event**, the risky failure; one made while that computer was disconnected is expected (the
//!   client catches up from its cursor on reconnecting) and counted apart;
//! - an event for one of the computer's own writes is an **echo**.
//!
//! ```text
//! HBSYNC_LIVE=frodo.dotyou.cloud cargo test --release -p hbsync-dev --test live_dual -- \
//!     --ignored --nocapture --test-threads 1
//! ```
//!
//! Computer A uses the credentials `hbsync-register <identity>` saved, B those of `--as b`, and
//! C (a third app client, scenario 6; skipped without it) those of `--as c`. `DUAL_FILES` sets
//! the large scenario's size (2,000 by default). Every scenario works in a top-level folder of
//! the run's, which is deleted for good at the end (hard deletes: no rows left in the feed).

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hbsync_dev::{
    OTHER_CLIENT, credentials_path_as, cursor_at_end, live_identity, on_threads, run_name,
    saved_client_as,
};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier, OwnWrites};
use hbsync_remote::contract::{file, folder, removed, upload, with};
use hbsync_remote::{NodeMeta, Remote, RemoteError};
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, RemoteFileId, Target, VersionTag};

type Res<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// The third client, for writes from neither A nor B.
const THIRD_CLIENT: &str = "c";

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A scenario: its name, and what it does.
type Scenario<'a> = (&'a str, &'a dyn Fn(&Harness) -> Res);

/// A file as written: its key on the drive, the write's version.
type Key = (RemoteFileId, VersionTag);

// ---- the computers ----

/// What a computer's socket said, and when it arrived.
struct Heard {
    at: Instant,
    event: Event,
}

struct Computer {
    name: String,
    remote: HomebaseRemote,
    heard: Arc<Mutex<Vec<Heard>>>,
    events: Sender<Event>,
    notifier: Mutex<Option<Notifier>>,
    /// When it was disconnected, and when its socket was subscribed again.
    gaps: Mutex<Vec<(Instant, Instant)>>,
}

impl Computer {
    /// Computer `name` with the credentials of `client`, listening.
    fn start(name: &str, identity: &str, client: Option<&str>) -> Res<Computer> {
        let remote = HomebaseRemote::connect(saved_client_as(identity, client)?)?;
        let (events, rx) = channel::<Event>();
        let heard = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&heard);
        std::thread::spawn(move || {
            while let Ok(event) = rx.recv() {
                lock(&log).push(Heard {
                    at: Instant::now(),
                    event,
                });
            }
        });
        let c = Computer {
            name: name.to_owned(),
            remote,
            heard,
            events,
            notifier: Mutex::new(None),
            gaps: Mutex::new(Vec::new()),
        };
        c.listen()?;
        Ok(c)
    }

    /// Subscribes as the daemon does, and waits until the server confirmed it.
    fn listen(&self) -> Res<Instant> {
        let since = Instant::now();
        let n = Notifier::spawn_with(
            self.remote.client().clone(),
            self.remote.drive(),
            OwnWrites::Excluded,
            self.events.clone(),
        )?;
        *lock(&self.notifier) = Some(n);
        let until = since + Duration::from_secs(20);
        loop {
            if let Some(at) = lock(&self.heard)
                .iter()
                .find(|h| h.at >= since && h.event == Event::Connected)
                .map(|h| h.at)
            {
                return Ok(at);
            }
            if Instant::now() > until {
                return Err(format!("{}: never subscribed", self.name).into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Drops the connection for `pause`, then subscribes again (a new connection).
    fn reconnect_after(&self, pause: Duration) -> Res {
        let from = Instant::now();
        drop(lock(&self.notifier).take());
        std::thread::sleep(pause);
        let to = self.listen()?;
        lock(&self.gaps).push((from, to));
        Ok(())
    }

    /// Whether a write that returned at `at` may have gone unannounced because this computer
    /// was disconnected: from a second before it let go (an event on its way) until its socket
    /// was subscribed again.
    fn in_gap(&self, at: Instant) -> bool {
        lock(&self.gaps)
            .iter()
            .any(|&(from, to)| at + Duration::from_secs(1) >= from && at <= to)
    }

    fn heard_count(&self) -> usize {
        lock(&self.heard).len()
    }
}

// ---- the writes ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Create,
    Content,
    Rename,
    Move,
    Remove,
    Delete,
    HardDeleteBatch,
}

impl Route {
    fn http(self) -> &'static str {
        match self {
            Route::Create => "POST /api/v2/drives/{d}/files (create)",
            Route::Content => "PATCH /api/v2/drives/{d}/files/{f} (new payload)",
            Route::Rename => "PATCH /api/v2/drives/{d}/files/{f} (header only: rename)",
            Route::Move => "PATCH /api/v2/drives/{d}/files/{f} (header only: groupId)",
            Route::Remove => "PATCH /api/v2/drives/{d}/files/{f} (header only: archivalStatus 2)",
            Route::Delete => "POST /api/v2/drives/{d}/files/{f}/delete (tombstone)",
            Route::HardDeleteBatch => "POST /api/v2/drives/{d}/files/hard-delete-batch",
        }
    }
}

/// One write that landed: by whom, what it left (`key`: the version written; a tombstone's own;
/// for a hard delete the version deleted, as its event's `previousFileRef` has it), when its
/// answer came.
#[derive(Debug, Clone)]
struct Write {
    by: usize,
    key: Key,
    route: Route,
    at: Instant,
}

/// A node of the run, as its last write left it.
#[derive(Debug, Clone)]
struct Doc {
    meta: NodeMeta,
    target: Target,
}

struct Harness {
    computers: Vec<Computer>,
    top: NodeMeta,
    log: Mutex<Vec<Write>>,
    docs: Mutex<HashMap<NodeId, Doc>>,
    /// Every file the run created, for cleaning up.
    created: Mutex<Vec<RemoteFileId>>,
    /// Each computer's copy of the feed's cursor, from before the scenario being checked.
    cursors: Mutex<Vec<String>>,
    /// Each computer's events checked so far: a scenario's check takes every event since,
    /// as its feed rows are every row since the cursor (an event of a write made before a
    /// scenario, as the run's folder, may arrive before it starts).
    checked: Mutex<Vec<usize>>,
    conflicts: AtomicUsize,
    report: Mutex<String>,
    problems: Mutex<Vec<String>>,
}

/// Where a scenario started: the log's and every computer's events' lengths.
struct Mark {
    log: usize,
    heard: Vec<usize>,
    started: Instant,
}

impl Harness {
    fn new(computers: Vec<Computer>, run: &str) -> Res<Harness> {
        // The feed's end, once: each computer's check reads on from a copy of it.
        let cursor = cursor_at_end(&computers[0].remote, None)?;
        let top = folder(run, NodeId::ROOT);
        let h = Harness {
            cursors: Mutex::new(vec![cursor; computers.len()]),
            checked: Mutex::new(vec![0; computers.len()]),
            computers,
            top: top.clone(),
            log: Mutex::new(Vec::new()),
            docs: Mutex::new(HashMap::new()),
            created: Mutex::new(Vec::new()),
            conflicts: AtomicUsize::new(0),
            report: Mutex::new(String::new()),
            problems: Mutex::new(Vec::new()),
        };
        h.create(0, &top, None)?;
        // The run's folder is checked as the first scenario's.
        Ok(h)
    }

    fn remote(&self, by: usize) -> &HomebaseRemote {
        &self.computers[by].remote
    }

    fn logged(&self, by: usize, key: Key, route: Route) {
        lock(&self.log).push(Write {
            by,
            key,
            route,
            at: Instant::now(),
        });
    }

    fn create(&self, by: usize, meta: &NodeMeta, data: Option<&[u8]>) -> Res<Target> {
        let written = match data {
            Some(d) => self
                .remote(by)
                .create(meta, Some(upload(&mut Cursor::new(d))))?,
            None => self.remote(by).create(meta, None)?,
        };
        self.logged(by, (written.at.file_id, written.at.version), Route::Create);
        lock(&self.created).push(written.at.file_id);
        lock(&self.docs).insert(
            meta.node,
            Doc {
                meta: meta.clone(),
                target: written.clone(),
            },
        );
        Ok(written)
    }

    fn doc(&self, node: NodeId) -> Res<Doc> {
        Ok(lock(&self.docs)
            .get(&node)
            .cloned()
            .ok_or("not a node of the run")?)
    }

    /// Rewrites `node` as `meta` (with a new payload `data`), from the version last known to
    /// this run; a version changed meanwhile (another computer's write: a conflict) is read and
    /// written over, as a client does after planning again (a few times: the other computer may
    /// write it again meanwhile).
    fn update(
        &self,
        by: usize,
        node: NodeId,
        meta: &NodeMeta,
        data: Option<&[u8]>,
        route: Route,
    ) -> Res<Target> {
        let mut target = self.doc(node)?.target;
        const TRIES: usize = 5;
        for attempt in 0..TRIES {
            let result = match data {
                Some(d) => self
                    .remote(by)
                    .update(&target, meta, Some(upload(&mut Cursor::new(d)))),
                None => self.remote(by).update(&target, meta, None),
            };
            match result {
                Ok(written) => {
                    self.logged(by, (written.at.file_id, written.at.version), route);
                    lock(&self.docs).insert(
                        node,
                        Doc {
                            meta: meta.clone(),
                            target: written.clone(),
                        },
                    );
                    return Ok(written);
                }
                Err(RemoteError::VersionMismatch) if attempt + 1 < TRIES => {
                    self.conflicts.fetch_add(1, Ordering::Relaxed);
                    let header = self.remote(by).header(target.at.file_id)?;
                    target = Target {
                        at: hbsync_types::RemoteRef {
                            file_id: header.file_id,
                            version: header.version,
                        },
                        token: header.token,
                        changed_at: None,
                    };
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(format!("conflicted {TRIES} times").into())
    }

    /// Deletes `node` (a tombstone, the client's delete for good) at its last known version.
    /// `Ok(false)`: changed meanwhile (a conflict), left alone.
    fn delete(&self, by: usize, node: NodeId) -> Res<bool> {
        let at = self.doc(node)?.target.at;
        match self.remote(by).delete(at.file_id, at.version) {
            Ok(()) => {}
            Err(RemoteError::VersionMismatch) => {
                self.conflicts.fetch_add(1, Ordering::Relaxed);
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        let answered = Instant::now();
        // The tombstone's version, which the delete's answer doesn't give.
        let tomb = self.remote(by).header(at.file_id)?;
        lock(&self.log).push(Write {
            by,
            key: (at.file_id, tomb.version),
            route: Route::Delete,
            at: answered,
        });
        let meta = self.doc(node)?.meta;
        lock(&self.docs).insert(
            node,
            Doc {
                meta,
                target: Target {
                    at: hbsync_types::RemoteRef {
                        file_id: at.file_id,
                        version: tomb.version,
                    },
                    token: None,
                    changed_at: None,
                },
            },
        );
        Ok(true)
    }

    /// Deletes these nodes for good, leaving no row in the feed, each at its last known
    /// version (a tombstone's included), in batches of 1,000 (the route's limit).
    fn hard_delete(&self, by: usize, nodes: &[NodeId]) -> Res {
        let ats: Vec<_> = nodes
            .iter()
            .map(|n| self.doc(*n).map(|d| d.target.at))
            .collect::<Res<_>>()?;
        for batch in ats.chunks(1000) {
            self.hard_delete_files(by, batch.iter().map(|a| (a.file_id, a.version)), true)?;
        }
        let mut docs = lock(&self.docs);
        for n in nodes {
            docs.remove(n);
        }
        Ok(())
    }

    fn hard_delete_files(&self, by: usize, files: impl Iterator<Item = Key>, log: bool) -> Res {
        let files: Vec<Key> = files.collect();
        let body = serde_json::json!({
            "files": files.iter().map(|(f, v)| serde_json::json!({
                "fileId": f.0, "versionTag": v.0
            })).collect::<Vec<_>>()
        });
        let remote = self.remote(by);
        let answer: serde_json::Value = remote
            .client()
            .post(&format!("{}/hard-delete-batch", remote.files()), &body)?;
        let results = answer["results"].as_array().ok_or("no results")?;
        for (key, result) in files.iter().zip(results) {
            if !result["errorCode"].is_null() {
                return Err(format!("hard delete of {:?}: {result}", key.0).into());
            }
            if log {
                self.logged(by, *key, Route::HardDeleteBatch);
            }
        }
        Ok(())
    }

    // ---- checking ----

    fn mark(&self) -> Mark {
        Mark {
            log: lock(&self.log).len(),
            heard: lock(&self.checked).clone(),
            started: Instant::now(),
        }
    }

    /// Waits until no socket has said anything for 3 s (60 s at most).
    fn settle(&self) {
        let until = Instant::now() + Duration::from_secs(60);
        let mut last = (Instant::now(), self.heard_total());
        while Instant::now() < until {
            std::thread::sleep(Duration::from_millis(200));
            let now = self.heard_total();
            if now != last.1 {
                last = (Instant::now(), now);
            } else if last.0.elapsed() > Duration::from_secs(3) {
                return;
            }
        }
    }

    fn heard_total(&self) -> usize {
        self.computers.iter().map(Computer::heard_count).sum()
    }

    /// Checks every computer's events since `mark` (see the module), and reports.
    fn check(&self, scenario: &str, mark: &Mark) -> Res {
        self.settle();
        let took = mark.started.elapsed();
        let log = lock(&self.log).clone();
        let writes = &log[mark.log..];
        let by_key: HashMap<Key, &Write> = log.iter().map(|w| (w.key, w)).collect();
        let ours: HashSet<RemoteFileId> = log.iter().map(|w| w.key.0).collect();
        let mut out = format!(
            "\n== {scenario}: {} writes ({} conflicts so far) in {took:.1?}\n",
            writes.len(),
            self.conflicts.load(Ordering::Relaxed)
        );
        let mut per_writer: HashMap<usize, usize> = HashMap::new();
        for w in writes {
            *per_writer.entry(w.by).or_default() += 1;
        }
        for (by, n) in &per_writer {
            let _ = writeln!(out, "   {} wrote {n}", self.computers[*by].name);
        }
        for (x, c) in self.computers.iter().enumerate() {
            let heard: Vec<(Instant, Event)> = {
                let all = lock(&c.heard);
                lock(&self.checked)[x] = all.len();
                all[mark.heard[x]..]
                    .iter()
                    .map(|h| (h.at, h.event.clone()))
                    .collect()
            };
            let mut announced: HashMap<Key, Instant> = HashMap::new();
            let (mut unsaid, mut foreign, mut lost) = (0, 0, Vec::new());
            for (at, e) in &heard {
                match e {
                    Event::Changed(Some(r)) => {
                        announced.entry((r.file_id, r.version)).or_insert(*at);
                        if !ours.contains(&r.file_id) {
                            foreign += 1;
                        }
                    }
                    Event::Changed(None) => unsaid += 1,
                    Event::Lost(why) | Event::Refused(why) => lost.push(why.clone()),
                    Event::Connected => {}
                }
            }
            // Others' writes, each announced? Own ones, each not?
            let (mut expected, mut late_ms, mut gap, mut echoes) = (0, Vec::new(), 0, Vec::new());
            let mut missed = Vec::new();
            for w in writes {
                let event = announced.get(&w.key);
                if w.by == x {
                    continue;
                }
                expected += 1;
                match event {
                    Some(at) => late_ms.push(ms(*at, w.at)),
                    None if c.in_gap(w.at) => gap += 1,
                    None => missed.push(format!(
                        "MISSED at {} (no event): {:?} v{} by {} via {}, {:.1?} before the check",
                        c.name,
                        w.key.0,
                        w.key.1.0,
                        self.computers[w.by].name,
                        w.route.http(),
                        w.at.elapsed()
                    )),
                }
            }
            // Any event for one of its own writes, this scenario's or an earlier one's.
            for (key, at) in &announced {
                if let Some(w) = by_key.get(key).filter(|w| w.by == x) {
                    echoes.push(format!(
                        "ECHO at {}: {:?} v{} by its own {} ({:+} ms)",
                        c.name,
                        key.0,
                        key.1.0,
                        w.route.http(),
                        ms(*at, w.at)
                    ));
                }
            }
            // The feed from the harness's own copy of the cursor: every row of the run's
            // files is this computer's own write or was announced.
            let (rows, rows_own, rows_other, rows_gap, rows_foreign) = {
                let mut cursors = lock(&self.cursors);
                let mut cursor = cursors[x].clone();
                let (mut rows, mut own, mut other, mut gap_rows, mut foreign_rows) =
                    (0, 0, 0, 0, 0);
                loop {
                    let page = c.remote.change_refs(Some(&cursor), 1000)?;
                    if page.unread > 0 {
                        missed.push(format!(
                            "{} rows {} couldn't read as nodes",
                            c.name, page.unread
                        ));
                    }
                    for r in &page.refs {
                        rows += 1;
                        if !ours.contains(&r.file_id) {
                            foreign_rows += 1;
                            continue;
                        }
                        let key = (r.file_id, r.version);
                        match by_key.get(&key) {
                            Some(w) if w.by == x => own += 1,
                            Some(w) => {
                                other += 1;
                                if !announced.contains_key(&key) {
                                    if c.in_gap(w.at) {
                                        gap_rows += 1;
                                    } else {
                                        missed.push(format!(
                                            "MISSED at {} (feed row): {:?} v{} by {} via {}",
                                            c.name,
                                            r.file_id,
                                            r.version.0,
                                            self.computers[w.by].name,
                                            w.route.http()
                                        ));
                                    }
                                }
                            }
                            None => missed.push(format!(
                                "UNKNOWN at {} (feed row of the run's file, no write logged): \
                                 {:?} v{} deleted={}",
                                c.name, r.file_id, r.version.0, r.deleted
                            )),
                        }
                    }
                    cursor = page.cursor;
                    if !page.has_more {
                        break;
                    }
                }
                cursors[x] = cursor;
                (rows, own, other, gap_rows, foreign_rows)
            };
            late_ms.sort_by(f64::total_cmp);
            let pick = |q: f64| -> String {
                if late_ms.is_empty() {
                    "-".into()
                } else {
                    let i = ((late_ms.len() - 1) as f64 * q).round() as usize;
                    format!("{:.0}", late_ms[i])
                }
            };
            let _ = writeln!(
                out,
                "   {}: heard {} events ({} of others' {expected} writes; {gap} while \
                 disconnected, caught up on reconnecting), {} echoes, {} missed, {foreign} \
                 foreign, {unsaid} unsaid; latency ms min/median/p95/max {}/{}/{}/{}; feed \
                 rows {rows}: {rows_own} own, {rows_other} others' ({rows_gap} while \
                 disconnected), {rows_foreign} not the run's",
                c.name,
                heard
                    .iter()
                    .filter(|(_, e)| matches!(e, Event::Changed(_)))
                    .count(),
                late_ms.len(),
                echoes.len(),
                missed.len(),
                pick(0.0),
                pick(0.5),
                pick(0.95),
                pick(1.0),
            );
            for why in &lost {
                let _ = writeln!(out, "     socket: {why}");
            }
            for line in echoes.iter().chain(&missed) {
                let _ = writeln!(out, "     {line}");
                lock(&self.problems).push(format!("{scenario}: {line}"));
            }
        }
        print!("{out}");
        lock(&self.report).push_str(&out);
        Ok(())
    }

    /// Deletes for good every file the run made (the cleanup's own events go unchecked).
    fn clean_up(&self) -> Res {
        let created = lock(&self.created).clone();
        let mut keys = Vec::new();
        for file_id in created.into_iter().rev() {
            match self.remote(0).header(file_id) {
                Ok(h) => keys.push((file_id, h.version)),
                Err(RemoteError::NotFound) => {}
                Err(e) => return Err(e.into()),
            }
        }
        for batch in keys.chunks(1000) {
            self.hard_delete_files(0, batch.iter().copied(), false)?;
        }
        Ok(())
    }
}

/// `at` after `since`, in ms (negative: before).
fn ms(at: Instant, since: Instant) -> f64 {
    if at >= since {
        (at - since).as_secs_f64() * 1e3
    } else {
        -((since - at).as_secs_f64() * 1e3)
    }
}

fn renamed(meta: &NodeMeta, name: &str) -> NodeMeta {
    let mut content = meta.content.clone();
    content.name = name.to_owned();
    with(meta, content)
}

fn moved(meta: &NodeMeta, parent: NodeId) -> NodeMeta {
    NodeMeta {
        parent,
        ..meta.clone()
    }
}

fn edited(meta: &NodeMeta, data: &[u8]) -> NodeMeta {
    with(
        meta,
        NodeContent::file(
            &meta.content.name,
            1_700_000_000_456,
            data.len() as u64,
            false,
        ),
    )
}

// ---- the scenarios ----

/// Scenario 1: Every kind of write, by `w`: create, a new payload, rename, move, removal to Recently
/// deleted, delete (a tombstone: the client's delete for good), hard deletes (of a live file,
/// a folder and a tombstone).
fn every_kind_of_write(h: &Harness, w: usize, name: &str) -> Res {
    let dir = folder(name, h.top.node);
    h.create(w, &dir, None)?;
    let sub = folder("sub", dir.node);
    h.create(w, &sub, None)?;
    let files: Vec<NodeMeta> = ["one.txt", "two.txt", "three.txt", "four.txt"]
        .iter()
        .map(|n| file(n, dir.node, n.as_bytes()))
        .collect();
    for f in &files {
        h.create(w, f, Some(f.content.name.as_bytes()))?;
    }
    let [one, two, three, four] = &files[..] else {
        return Err("four files".into());
    };
    let data = b"one, edited";
    h.update(w, one.node, &edited(one, data), Some(data), Route::Content)?;
    h.update(
        w,
        two.node,
        &renamed(two, "two renamed.txt"),
        None,
        Route::Rename,
    )?;
    h.update(w, three.node, &moved(three, sub.node), None, Route::Move)?;
    let one_now = h.doc(one.node)?.meta;
    h.update(w, one.node, &removed(&one_now), None, Route::Remove)?;
    h.delete(w, two.node)?;
    h.delete(w, four.node)?;
    // A live file, a folder, a tombstone.
    h.hard_delete(w, &[three.node, sub.node, four.node])?;
    Ok(())
}

/// Scenario 2: A and B (`a`, `b`) writing in turn and at once, the same files too: conflicts, which the
/// loser writes over once, and deletes racing edits.
fn interleaved_and_concurrent(h: &Harness, a: usize, b: usize) -> Res {
    let dir = folder("interleaved", h.top.node);
    h.create(a, &dir, None)?;
    let mut shared = Vec::new();
    for i in 0..10 {
        for (who, tag) in [(a, "a"), (b, "b")] {
            let f = file(&format!("{tag}{i}.txt"), dir.node, tag.as_bytes());
            h.create(who, &f, Some(tag.as_bytes()))?;
            shared.push(f);
        }
    }
    // In turn: each edits the other's.
    for pair in shared.chunks(2) {
        let (fa, fb) = (&pair[0], &pair[1]);
        h.update(
            b,
            fa.node,
            &edited(fa, b"by b"),
            Some(b"by b"),
            Route::Content,
        )?;
        h.update(
            a,
            fb.node,
            &edited(fb, b"by a"),
            Some(b"by a"),
            Route::Content,
        )?;
    }
    // At once, the same files: one wins each, the other writes over it.
    let same: Vec<NodeMeta> = shared[..10].to_vec();
    std::thread::scope(|scope| -> Res {
        let runs: Vec<_> = [(a, "A at once"), (b, "B at once")]
            .into_iter()
            .map(|(who, data)| {
                let same = &same;
                scope.spawn(move || -> Result<(), String> {
                    for f in same {
                        let meta = h.doc(f.node).map_err(|e| e.to_string())?.meta;
                        h.update(
                            who,
                            f.node,
                            &renamed(&meta, &format!("{} {}", meta.content.name, data)),
                            None,
                            Route::Rename,
                        )
                        .map_err(|e| e.to_string())?;
                    }
                    Ok(())
                })
            })
            .collect();
        for r in runs {
            r.join().map_err(|_| "panicked")??;
        }
        Ok(())
    })?;
    // A edits what B deletes, at once.
    let racing: Vec<NodeMeta> = shared[10..16].to_vec();
    std::thread::scope(|scope| -> Res {
        let edits = scope.spawn(|| -> Result<(), String> {
            for f in &racing {
                let meta = h.doc(f.node).map_err(|e| e.to_string())?.meta;
                match h.update(
                    a,
                    f.node,
                    &edited(&meta, b"edit"),
                    Some(b"edit"),
                    Route::Content,
                ) {
                    Ok(_) => {}
                    // Deleted first: the client takes it as gone.
                    Err(e) if matches!(e.downcast_ref(), Some(RemoteError::NotFound)) => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            Ok(())
        });
        let deletes = scope.spawn(|| -> Result<(), String> {
            for f in &racing {
                h.delete(b, f.node).map_err(|e| e.to_string())?;
            }
            Ok(())
        });
        edits.join().map_err(|_| "panicked")??;
        deletes.join().map_err(|_| "panicked")??;
        Ok(())
    })?;
    Ok(())
}

/// Scenario 3: `n` files by `w`: created, removed to Recently deleted, deleted (tombstones,
/// which stay for the feed check; the cleanup deletes them for good).
fn large_batch(h: &Harness, w: usize, n: usize, name: &str) -> Res {
    let dir = folder(name, h.top.node);
    h.create(w, &dir, None)?;
    let files: Vec<NodeMeta> = (0..n)
        .map(|i| file(&format!("f{i:05}.txt"), dir.node, b"x"))
        .collect();
    on_threads(n, |i| h.create(w, &files[i], Some(b"x")).map(drop))?;
    on_threads(n, |i| {
        h.update(w, files[i].node, &removed(&files[i]), None, Route::Remove)
            .map(drop)
    })?;
    on_threads(n, |i| h.delete(w, files[i].node).map(drop))?;
    Ok(())
}

/// Scenario 4: `w` writes while `watcher` drops its connection for 3 s and subscribes again.
fn reconnect_while_writing(h: &Harness, w: usize, watcher: usize, name: &str) -> Res {
    let dir = folder(name, h.top.node);
    h.create(w, &dir, None)?;
    let n = 300;
    let files: Vec<NodeMeta> = (0..n)
        .map(|i| file(&format!("r{i:04}.txt"), dir.node, b"r"))
        .collect();
    std::thread::scope(|scope| -> Res {
        let writing = scope.spawn(|| -> Result<(), String> {
            for f in &files {
                h.create(w, f, Some(b"r")).map_err(|e| e.to_string())?;
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(())
        });
        std::thread::sleep(Duration::from_secs(2));
        h.computers[watcher].reconnect_after(Duration::from_secs(3))?;
        writing.join().map_err(|_| "panicked")??;
        Ok(())
    })?;
    Ok(())
}

// ---- the tests ----

fn has_client(identity: &str, client: &str) -> Res<bool> {
    Ok(credentials_path_as(identity, Some(client))?.exists())
}

fn large() -> usize {
    std::env::var("DUAL_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

/// Runs `scenarios` on a harness of `computers`, checks each, cleans up; fails on any missed
/// event or echo, after reporting all of them.
fn run(computers: Vec<Computer>, run: &str, scenarios: &[Scenario<'_>]) -> Res {
    let _guard = hbsync_dev::RunFolder::new(run)?;
    let h = Harness::new(computers, run)?;
    let result = (|| -> Res {
        for (name, scenario) in scenarios {
            let mark = h.mark();
            scenario(&h)?;
            h.check(name, &mark)?;
        }
        Ok(())
    })();
    let cleaned = h.clean_up();
    result?;
    cleaned?;
    let problems = lock(&h.problems).clone();
    if !problems.is_empty() {
        return Err(format!("{} problems:\n{}", problems.len(), problems.join("\n")).into());
    }
    Ok(())
}

/// One client: none of its own writes comes back on its socket, and every feed row of the
/// run's is its own.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn one_client_hears_none_of_its_own() -> Res {
    let identity = live_identity()?;
    let a = Computer::start("A", &identity, None)?;
    let n = large();
    run(
        vec![a],
        &run_name("own-writes solo"),
        &[
            ("1. every kind of write by A", &|h| {
                every_kind_of_write(h, 0, "every kind by A")
            }),
            ("3. large batch by A", &|h| {
                large_batch(h, 0, n, "large by A")
            }),
        ],
    )
}

/// Two computers (and a third client's writes, if registered): each hears every write of the
/// others and none of its own.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE) and a second client (hbsync-register --as b)"]
fn computers_hear_each_other_and_not_themselves() -> Res {
    let identity = live_identity()?;
    if !has_client(&identity, OTHER_CLIENT)? {
        return Err(format!(
            "no second client: run hbsync-register {identity} --as {OTHER_CLIENT} first"
        )
        .into());
    }
    let third = has_client(&identity, THIRD_CLIENT)?;
    let mut computers = vec![
        Computer::start("A", &identity, None)?,
        Computer::start("B", &identity, Some(OTHER_CLIENT))?,
    ];
    if third {
        computers.push(Computer::start("C", &identity, Some(THIRD_CLIENT))?);
    } else {
        println!(
            "scenario 6 skipped: no third client (hbsync-register {identity} --as {THIRD_CLIENT})"
        );
    }
    let n = large();
    let large_a = move |h: &Harness| large_batch(h, 0, n, "large by A");
    let large_b = move |h: &Harness| large_batch(h, 1, n, "large by B");
    let mut scenarios: Vec<Scenario<'_>> = vec![
        ("1. every kind of write by A", &|h| {
            every_kind_of_write(h, 0, "every kind by A")
        }),
        ("1. every kind of write by B", &|h| {
            every_kind_of_write(h, 1, "every kind by B")
        }),
        ("2. A and B in turn and at once", &|h| {
            interleaved_and_concurrent(h, 0, 1)
        }),
        ("3. large batch by A", &large_a),
        ("3. large batch by B", &large_b),
        ("4. B reconnects while A writes", &|h| {
            reconnect_while_writing(h, 0, 1, "reconnect B")
        }),
        ("4. A reconnects while B writes", &|h| {
            reconnect_while_writing(h, 1, 0, "reconnect A")
        }),
    ];
    if third {
        scenarios.push(("6. every kind of write by a third client", &|h| {
            every_kind_of_write(h, 2, "every kind by C")
        }));
    }
    run(computers, &run_name("own-writes dual"), &scenarios)
}
