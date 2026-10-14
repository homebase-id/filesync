//! Two clients syncing two folders through a live identity:
//! `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-dev --test live_e2e -- --ignored`.
//! Needs the credentials `hbsync-register` saves. Both clients sync the whole drive; the test
//! works and checks inside one top-level folder of its own and deletes it at the end.

use std::fs;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use hbsync_engine::testkit::{Res, TestClient};
use hbsync_engine::{EngineConfig, SyncReport};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_local::Filter;
use hbsync_platform::tags::{self, Tag, Tags};

struct Client(TestClient<Arc<HomebaseRemote>>);

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

impl Client {
    fn new(identity: &str, name: &str) -> Res<Self> {
        let remote = Arc::new(HomebaseRemote::connect(hbsync_dev::saved_client(
            identity,
        )?)?);
        let configure = |cfg: &mut EngineConfig| -> Res<()> {
            cfg.allow_mass_delete = true; // the test deletes its whole folder at the end
            cfg.exec.filter = Arc::new(Filter::load(&cfg.exec.root, cfg.exec.case_insensitive)?);
            Ok(())
        };
        Ok(Client(TestClient::new(remote, name, configure)?))
    }

    /// A restart with the ignore rules `rules`.
    fn restart_ignoring(&mut self, rules: &str) -> Res<()> {
        fs::write(hbsync_local::layout::ignore_file(&self.root), rules)?;
        self.restart()?;
        assert!(self.engine.apply_filter_change()?);
        Ok(())
    }

    /// Scan, then sync until idle: nothing may fail or be skipped.
    fn sync(&mut self) -> Res<SyncReport> {
        // Homebase holds back feed rows of the current millisecond.
        let total = self.0.sync_until_idle(&hbsync_app::today(), || {
            std::thread::sleep(Duration::from_millis(20));
        })?;
        assert!(
            total.failed.is_empty(),
            "{}: failed {:?}",
            self.name,
            total.failed
        );
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
    let mut a = Client::new(&identity, "a")?;
    let mut b = Client::new(&identity, "b")?;
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
    assert_eq!(a.tree_text(run), b.tree_text(run));
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
    a.sync()?;
    assert_eq!(a.tree_text(run), b.tree_text(run));
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
