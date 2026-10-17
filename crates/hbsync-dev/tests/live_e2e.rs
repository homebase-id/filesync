//! Two clients syncing two folders through a live identity:
//! `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live_e2e -- --ignored`.
//! Needs the credentials `hbsync-register` saves. Each client syncs only the test's own
//! top-level folder (an ignore rule masks the rest of the drive: other tests' folders, those
//! left by runs that ended early, a gigabyte each at times); the test works and checks inside
//! it and deletes it at the end. The two share one client registration: they sync when the test says, never on a notification, so
//! that the socket leaves out a client's own writes (`excludeOwnWrites`) doesn't concern them.

use std::fs;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use hbsync_engine::testkit::{Res, TestClient};
use hbsync_engine::{EngineConfig, SyncReport};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_local::Filter;
use hbsync_platform::tags::{self, Tag, Tags};

struct Client(TestClient<Arc<HomebaseRemote>>, String);

impl Deref for Client {
    type Target = TestClient<Arc<HomebaseRemote>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The ignore rules that leave a client only the test's folder `run`: the drive is shared with
/// other tests and with what runs that ended early left behind.
fn only(run: &str) -> String {
    format!("/*\n!/{run}/\n")
}

impl Client {
    /// A client of `identity` that syncs only the folder `run`.
    fn new(identity: &str, name: &str, run: &str) -> Res<Self> {
        let remote = Arc::new(HomebaseRemote::connect(hbsync_dev::saved_client(
            identity,
        )?)?);
        let rules = only(run);
        let configure = move |cfg: &mut EngineConfig| -> Res<()> {
            cfg.allow_mass_delete = true; // the test deletes its whole folder at the end
            // Before the first start; a restart keeps the rules a scenario set.
            let ignore = hbsync_local::layout::ignore_file(&cfg.exec.root);
            if !ignore.exists() {
                fs::write(&ignore, &rules)?;
            }
            cfg.exec.filter = Arc::new(Filter::load(&cfg.exec.root, cfg.exec.case_insensitive)?);
            Ok(())
        };
        Ok(Client(
            TestClient::new(remote, name, configure)?,
            run.to_owned(),
        ))
    }

    /// The top-level entries of its root, the state folder left out: the test's folder at most.
    fn top_level(&self) -> Res<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if name != hbsync_local::STATE_DIR {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    /// A restart with the ignore rules `rules`, besides those that keep it to the test's folder.
    fn restart_ignoring(&mut self, rules: &str) -> Res<()> {
        let rules = format!("{}{rules}", only(&self.1));
        fs::write(hbsync_local::layout::ignore_file(&self.root), rules)?;
        self.restart()?;
        assert!(self.engine.apply_filter_change()?);
        Ok(())
    }

    /// Why this client's tree under `run` isn't `other`'s, for a failure message: the feed read
    /// again from its cursor (rows it would take now), and whether one more sync converges.
    fn why_not_like(&mut self, other: &Client, run: &str) -> String {
        use hbsync_remote::Remote;
        let cursor = hbsync_state::StateDb::open_account(&self.db_path())
            .and_then(|mut db| db.read(|s| s.account_kv(hbsync_state::keys::REMOTE_CURSOR)));
        let cursor = match cursor {
            Ok(c) => c,
            Err(e) => return format!("no cursor: {e}"),
        };
        let again = self.remote.change_refs(cursor.as_deref(), 500);
        let rows = again
            .as_ref()
            .map(|p| format!("{} rows, more: {}", p.refs.len(), p.has_more));
        std::thread::sleep(Duration::from_secs(1));
        // The rows from that cursor now, with their times (`updated`), to set against the
        // cursor's time.
        let later = self.remote.change_refs(cursor.as_deref(), 500).map(|p| {
            p.refs
                .iter()
                .map(|r| {
                    format!(
                        "{:?} deleted={} at={:?}",
                        r.file_id,
                        r.deleted,
                        r.changed_at.or(r.removed_at)
                    )
                })
                .collect::<Vec<_>>()
        });
        let synced = self.sync().map(|r| (r.feed, r.synced.len()));
        format!(
            "{}'s cursor {cursor:?}; read again from it: {rows:?}; a second later: {later:?}; \
             another sync: {synced:?}, now converged: {}",
            self.name,
            self.tree_text(run) == other.tree_text(run)
        )
    }

    /// Scan, then sync until idle: nothing may fail or be skipped. A failure names the test's
    /// line that synced, and each step that failed with its error.
    #[track_caller]
    fn sync(&mut self) -> Res<SyncReport> {
        // Homebase holds back feed rows of the current millisecond.
        let total = self.0.sync_until_idle(&hbsync_app::today(), true, || {
            std::thread::sleep(Duration::from_millis(20));
        })?;
        if !total.failed.is_empty() {
            let steps: Vec<String> = total
                .failed
                .iter()
                .map(|(op, e)| format!("  {op:?}\n    error: {e}"))
                .collect();
            panic!(
                "{}: {} sync steps failed:\n{}",
                self.name,
                steps.len(),
                steps.join("\n")
            );
        }
        let skipped = self.0.engine.drive().take_skipped();
        assert!(
            skipped.is_empty(),
            "{}: feed rows skipped {skipped:?}",
            self.name
        );
        Ok(total)
    }
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn two_clients_converge_through_homebase() -> Res<()> {
    let identity = hbsync_dev::live_identity()?;
    let run = hbsync_dev::run_name("e2e");
    // Deleted for good at the end, whatever happens; the test's own cleanup syncs a delete.
    let _guard = hbsync_dev::RunFolder::new(&run)?;
    let mut a = Client::new(&identity, "a", &run)?;
    let mut b = Client::new(&identity, "b", &run)?;
    let result = scenarios(&run, &mut a, &mut b);

    // Clean up whatever happened: delete the run folder on both sides.
    let _ = fs::remove_dir_all(a.root.join(&run));
    let cleanup = a.sync().and_then(|_| b.sync());
    result?;
    cleanup?;
    assert!(
        !b.root.join(&run).exists(),
        "the run folder is gone on b too"
    );
    Ok(())
}

/// The Photos example, with the server's removal times (a header's `updated`): on Monday
/// `Photos/a.jpg` is deleted on its own; on Tuesday the folder `Photos`, still holding `b.jpg`
/// and a subfolder with a file. Restoring `Photos` brings back `b.jpg` and the subfolder's
/// file, not `a.jpg`: that stays in Recently deleted, and comes back on its own, into `Photos`.
#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn photos_restore_without_what_was_deleted_from_it_before() -> Res<()> {
    let identity = hbsync_dev::live_identity()?;
    let run = hbsync_dev::run_name("photos");
    // Deleted for good at the end, whatever happens; the test's own cleanup syncs a delete.
    let _guard = hbsync_dev::RunFolder::new(&run)?;
    let mut a = Client::new(&identity, "a", &run)?;
    let mut b = Client::new(&identity, "b", &run)?;
    let result = photos(&run, &mut a, &mut b);
    let _ = fs::remove_dir_all(a.root.join(&run));
    let cleanup = a.sync().and_then(|_| b.sync());
    result?;
    cleanup?;
    Ok(())
}

fn photos(run: &str, a: &mut Client, b: &mut Client) -> Res<()> {
    let at = |rel: &str| format!("{run}/{rel}");
    a.write(&at("keep.txt"), "k")?;
    a.write(&at("Photos/a.jpg"), "a")?;
    a.write(&at("Photos/b.jpg"), "b")?;
    a.write(&at("Photos/2024/c.jpg"), "c")?;
    a.sync()?;
    b.sync()?;
    // Nothing else of the drive came down (other tests' folders, a run's leftovers).
    assert_eq!(
        (a.top_level()?, b.top_level()?),
        (vec![run.to_owned()], vec![run.to_owned()])
    );
    fs::remove_file(a.root.join(at("Photos/a.jpg")))?; // Monday
    a.sync()?;
    fs::remove_dir_all(a.root.join(at("Photos")))?; // Tuesday
    a.sync()?;
    b.sync()?;
    assert_eq!(b.tree_text(run), ["keep.txt = k"]);
    let listed = b.recently_deleted(1000)?;
    let item = |rel: &str| {
        listed
            .iter()
            .find(|i| i.path == at(rel))
            .cloned()
            .ok_or(format!("{rel} not listed"))
    };
    let (photos, a_jpg, b_jpg, c_jpg) = (
        item("Photos")?,
        item("Photos/a.jpg")?,
        item("Photos/b.jpg")?,
        item("Photos/2024/c.jpg")?,
    );
    assert_eq!(a_jpg.root, a_jpg.node, "deleted on its own");
    assert_eq!(
        (b_jpg.root, c_jpg.root),
        (photos.node, photos.node),
        "deleted with the folder"
    );
    // Confirmed by the end of a sync: the server's times.
    let (a_at, photos_at, b_at) = (
        a_jpg.removed_at_ms.ok_or("a.jpg's time open")?,
        photos.removed_at_ms.ok_or("Photos' time open")?,
        b_jpg.removed_at_ms.ok_or("b.jpg's time open")?,
    );
    assert!(
        a_at < photos_at && photos_at <= b_at,
        "server times: a.jpg {a_at}, Photos {photos_at}, b.jpg {b_at}"
    );

    // b restores Photos: b.jpg and 2024/c.jpg come back, on both; a.jpg doesn't.
    assert_eq!(b.engine.drive().restore(&[photos.node])?, 4);
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read(&at("Photos/b.jpg"))?, b"b");
    assert_eq!(a.read(&at("Photos/2024/c.jpg"))?, b"c");
    assert!(!a.root.join(at("Photos/a.jpg")).exists());
    assert!(
        a.recently_deleted(1000)?
            .iter()
            .any(|i| i.path == at("Photos/a.jpg")),
        "a.jpg stays in Recently deleted"
    );

    // a.jpg on its own: back into Photos.
    assert_eq!(a.engine.drive().restore(&[a_jpg.node])?, 1);
    a.sync()?;
    b.sync()?;
    assert_eq!(b.read(&at("Photos/a.jpg"))?, b"a");
    assert_eq!(a.tree_text(run), b.tree_text(run));
    Ok(())
}

fn scenarios(run: &str, a: &mut Client, b: &mut Client) -> Res<()> {
    let at = |rel: &str| format!("{run}/{rel}");
    // A large file goes up streamed and comes down in ranged chunks.
    let big: Vec<u8> = (0..50_000_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();

    // Create.
    a.write(&at("doc.txt"), "v1")?;
    a.write(&at("sub/x.txt"), "x")?;
    a.write(&at("Résumé 履歴書.txt"), "unicode")?;
    a.write(&at("big.bin"), &big)?;
    fs::create_dir_all(a.root.join(at("empty")))?;
    a.sync()?;
    b.sync()?;
    // Nothing else of the drive came down (other tests' folders, a run's leftovers).
    assert_eq!(
        (a.top_level()?, b.top_level()?),
        (vec![run.to_owned()], vec![run.to_owned()])
    );
    assert_eq!(b.tree_text(run), a.tree_text(run));
    assert!(
        b.read(&at("big.bin"))? == big,
        "the large file arrived intact"
    );

    // Edit on one side, rename and move on the other.
    a.write(&at("doc.txt"), "v2")?;
    fs::rename(b.root.join(at("sub")), b.root.join(at("sub2")))?;
    fs::rename(
        b.root.join(at("sub2/x.txt")),
        b.root.join(at("x-moved.txt")),
    )?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    if a.tree_text(run) != b.tree_text(run) {
        let why = a.why_not_like(b, run);
        panic!(
            "after a rename and move on b: a {:?}, b {:?}\n{why}",
            a.tree_text(run),
            b.tree_text(run)
        );
    }
    assert_eq!(b.read(&at("doc.txt"))?, b"v2");
    assert!(a.root.join(at("x-moved.txt")).is_file());

    // Delete.
    fs::remove_file(a.root.join(at("x-moved.txt")))?;
    let ra = a.sync()?;
    let rb = b.sync()?;
    assert!(
        !b.root.join(at("x-moved.txt")).exists(),
        "a: {ra:?}\nb: {rb:?}"
    );

    // Edit/edit: both versions survive, one as a conflict copy.
    a.write(&at("doc.txt"), "from a")?;
    b.write(&at("doc.txt"), "from b")?;
    a.sync()?;
    let report = b.sync()?;
    assert_eq!(report.conflicts.len(), 1, "{:?}", report.conflicts);
    let ra = a.sync()?;
    if a.tree_text(run) != b.tree_text(run) {
        let why = a.why_not_like(b, run);
        panic!(
            "after the conflict: a {:?}, b {:?}\nb: {:?} feed {:?}\na: {:?} feed {:?}\n{why}",
            a.tree_text(run),
            b.tree_text(run),
            report.synced,
            report.feed,
            ra.synced,
            ra.feed
        );
    }
    let versions: Vec<String> = a
        .tree_text(run)
        .into_iter()
        .filter(|l| l.starts_with("doc"))
        .collect();
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert!(versions.iter().any(|l| l.ends_with("= from a")));
    assert!(versions.iter().any(|l| l.ends_with("= from b")));

    // Edit/delete: the edit wins.
    fs::remove_file(a.root.join(at("Résumé 履歴書.txt")))?;
    b.write(&at("Résumé 履歴書.txt"), "edited while deleted")?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(a.read(&at("Résumé 履歴書.txt"))?, b"edited while deleted");
    assert_eq!(a.tree_text(run), b.tree_text(run));

    // Finder tags (Linux's here, names only) go both ways, on a file and a folder.
    if tags::CAN_TAG && tags::write(&a.root.join(run), &Tags::default())?.is_some() {
        let names = |c: &Client, rel: &str| -> Vec<String> {
            tags::read(&c.root.join(rel))
                .unwrap_or_default()
                .iter()
                .map(|t| t.name.clone())
                .collect()
        };
        std::thread::sleep(Duration::from_millis(20)); // past a coarse clock tick
        tags::write(
            &a.root.join(at("doc.txt")),
            &Tags::new([Tag::new("Red", 6)]),
        )?;
        tags::write(&a.root.join(at("empty")), &Tags::new([Tag::new("Work", 0)]))?;
        a.sync()?;
        b.sync()?;
        assert_eq!(names(b, &at("doc.txt")), ["Red"]);
        assert_eq!(names(b, &at("empty")), ["Work"]);
        std::thread::sleep(Duration::from_millis(20));
        tags::write(&b.root.join(at("doc.txt")), &Tags::default())?;
        b.sync()?;
        a.sync()?;
        assert!(
            names(a, &at("doc.txt")).is_empty(),
            "the removal came back to a"
        );
    }

    // A restart picks up what changed while it was down.
    b.restart()?;
    a.write(&at("while-b-was-down.txt"), "hi")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(a.tree_text(run), b.tree_text(run));

    filters(run, a, b)
}

/// Sync semantics §10: junk never syncs, and what b starts ignoring stays on b only.
fn filters(run: &str, a: &mut Client, b: &mut Client) -> Res<()> {
    let at = |rel: &str| format!("{run}/proj/{rel}");
    let proj = format!("{run}/proj");
    a.write(&at(".DS_Store"), "finder")?;
    a.write(&at("main.c"), "int main")?;
    a.write(&at("build/out.o"), "obj")?;
    a.sync()?;
    b.sync()?;
    assert_eq!(
        b.tree_text(&proj),
        ["build/", "build/out.o = obj", "main.c = int main"]
    );

    // b starts ignoring build/: it leaves the drive (a's copy goes to a's trash), stays on b.
    b.restart_ignoring("build/\n")?;
    b.write(&at("build/local.o"), "b only")?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        a.tree_text(&proj),
        [".DS_Store = finder", "main.c = int main"]
    );
    assert_eq!(
        b.tree_text(&proj),
        [
            "build/",
            "build/local.o = b only",
            "build/out.o = obj",
            "main.c = int main"
        ]
    );

    // a deletes the folder: b keeps what it ignores and the folder holding it, which goes
    // back up, empty.
    fs::remove_dir_all(a.root.join(&proj))?;
    a.sync()?;
    b.sync()?;
    a.sync()?;
    assert_eq!(
        b.tree_text(&proj),
        ["build/", "build/local.o = b only", "build/out.o = obj"]
    );
    assert!(a.root.join(&proj).is_dir() && a.tree_text(&proj).is_empty());

    // Clean up b's own files; the empty folder goes everywhere.
    fs::remove_dir_all(b.root.join(&proj))?;
    b.sync()?;
    a.sync()?;
    assert!(!a.root.join(&proj).exists());
    Ok(())
}
