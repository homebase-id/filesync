//! The answers to this device's own writes against a pull that recorded a newer state of the
//! same file between the write landing and its answer's commit (another folder's engine, or an
//! announcement, pulls the account's feed on its own connection meanwhile).

use std::fs;
use std::sync::{Arc, Mutex};

use hbsync_engine::Drive;
use hbsync_remote::fake::FakeRemote;
use hbsync_remote::{
    Answers, ChangesPage, Deletes, ForGood, Holds, NodeMeta, RefsPage, Remote, RemoteHeader,
    Result as RemoteResult, Upload,
};
use hbsync_state::StateDb;
use hbsync_types::{Doomed, NodeId, RemoteFileId, Target, Written};

mod common;

use common::{Client, header_named, server};

type R = Result<(), Box<dyn std::error::Error>>;

/// What runs once, right after a write of the kind armed landed, with its answer.
type Then = Box<dyn FnOnce(&Written) + Send>;

/// A remote that, once armed, runs a step right after the next update (or removal) lands and
/// before its answer goes back: another writer acts, and the account pulls the feed.
struct Meanwhile {
    inner: FakeRemote,
    on_update: Mutex<Option<Then>>,
    on_removal: Mutex<Option<Then>>,
}

impl Meanwhile {
    fn after(slot: &Mutex<Option<Then>>, w: &Written) {
        let then = slot.lock().ok().and_then(|mut s| s.take());
        if let Some(then) = then {
            then(w);
        }
    }
}

impl Remote for Meanwhile {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<ChangesPage> {
        self.inner.changes(cursor, limit)
    }
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<RefsPage> {
        self.inner.change_refs(cursor, limit)
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }
    fn header(&self, file_id: RemoteFileId) -> RemoteResult<RemoteHeader> {
        self.inner.header(file_id)
    }
    fn holding(
        &self,
        folders: &[NodeId],
        holds: Holds<'_>,
    ) -> RemoteResult<std::collections::HashSet<NodeId>> {
        self.inner.holding(folders, holds)
    }
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> RemoteResult<Written> {
        self.inner.create(node, payload)
    }
    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<Written> {
        let w = self.inner.update(target, meta, payload)?;
        Self::after(&self.on_update, &w);
        Ok(w)
    }
    fn delete(&self, files: &[Doomed]) -> RemoteResult<Deletes> {
        self.inner.delete(files)
    }
    fn delete_for_good(&self, files: &[Doomed]) -> RemoteResult<ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(&self, files: &[Target], removed: bool) -> RemoteResult<Answers<Written>> {
        let answers = self.inner.set_removed(files, removed)?;
        if removed && let Some(Ok(w)) = answers.answers.first() {
            Self::after(&self.on_removal, w);
        }
        Ok(answers)
    }
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn std::io::Write,
    ) -> RemoteResult<u64> {
        self.inner.read(file_id, range, sink)
    }
}

/// `a` restarted on a [`Meanwhile`] over its own remote, and its drive (the step pulls through
/// it).
fn meanwhile(a: &mut Client) -> Result<(Arc<Meanwhile>, Arc<Drive>), String> {
    meanwhile_with(a, |_| {})
}

/// [`meanwhile`], its engine's config set up by `tweak` too.
fn meanwhile_with(
    a: &mut Client,
    tweak: impl FnOnce(&mut hbsync_engine::EngineConfig),
) -> Result<(Arc<Meanwhile>, Arc<Drive>), String> {
    let remote = Arc::new(Meanwhile {
        inner: a.remote.clone(),
        on_update: Mutex::new(None),
        on_removal: Mutex::new(None),
    });
    a.restart_with(Box::new(Arc::clone(&remote)), tweak)
        .map_err(|e| e.to_string())?;
    Ok((remote, Arc::clone(a.engine.drive())))
}

/// Where the account's picture has `node`: (in the mirror, in Recently deleted).
fn picture(a: &mut Client, node: NodeId) -> Result<(bool, bool), Box<dyn std::error::Error>> {
    Ok(a.engine.db().read(|s| {
        Ok((
            s.get_mirror(node)?.is_some(),
            s.removed_by_node(node)?.is_some(),
        ))
    })?)
}

/// A client with "keep.txt" and "f.txt" synced, another computer on its drive, and f.txt's node.
fn two_files() -> Result<(Client, FakeRemote, NodeId), Box<dyn std::error::Error>> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    a.write("keep.txt", "k")?;
    a.write("f.txt", "one")?;
    a.sync()?;
    let node = header_named(&server, "f.txt").node().ok_or("removed")?;
    Ok((a, FakeRemote::new(server), node))
}

/// `a` on a [`Meanwhile`]: once its next update (or with `removal`, its next removal) landed,
/// the other computer `b` moves the file to Recently deleted (or with `removal`, restores it),
/// and the account pulls that before the answer is recorded.
fn racing(a: &mut Client, b: FakeRemote, removal: bool) -> R {
    let (remote, drive) = meanwhile(a)?;
    let slot = match removal {
        true => &remote.on_removal,
        false => &remote.on_update,
    };
    *slot.lock().map_err(|_| "poisoned")? = Some(Box::new(move |w: &Written| {
        b.set_removed(std::slice::from_ref(&w.target), !removal)
            .expect("b's write");
        drive.pull(None).expect("the pull");
    }));
    Ok(())
}

/// A rename lands; another computer moves the file to Recently deleted; the account pulls that
/// removal; then the rename's answer is recorded. The drive has the file in Recently deleted, as
/// the newer state: this device's picture must too, and not also as live.
#[test]
fn a_renames_answer_keeps_a_newer_removal() -> R {
    let (mut a, b, node) = two_files()?;
    racing(&mut a, b, false)?;
    fs::rename(a.root.join("f.txt"), a.root.join("g.txt"))?;
    a.sync()?;
    let (live, removed) = picture(&mut a, node)?;
    assert!(removed, "the newer removal stays");
    assert!(!(live && removed), "live and in Recently deleted at once");
    // The removal reaches this device as any other device's does.
    a.sync()?;
    assert!(!a.root.join("g.txt").exists(), "{:?}", a.tree_text(""));
    Ok(())
}

/// A removal lands; another computer restores the file; the account pulls that restore; then
/// the removal's answer is recorded. The drive has the file live, as the newer state: this
/// device's picture must too, and the file comes back here.
#[test]
fn a_removals_answer_keeps_a_newer_restore() -> R {
    let (mut a, b, node) = two_files()?;
    racing(&mut a, b, true)?;
    fs::remove_file(a.root.join("f.txt"))?;
    a.sync()?;
    let (live, removed) = picture(&mut a, node)?;
    assert!(
        live && !removed,
        "the newer restore stays: live {live}, removed {removed}"
    );
    a.sync()?;
    assert_eq!(a.read_text("f.txt")?, "one", "{:?}", a.tree_text(""));
    Ok(())
}

/// An edit's upload lands; another computer moves the file to Recently deleted; the account
/// pulls that removal; then the upload's answer is recorded. The newer removal stays.
#[test]
fn an_uploads_answer_keeps_a_newer_removal() -> R {
    let (mut a, b, node) = two_files()?;
    racing(&mut a, b, false)?;
    a.write("f.txt", "two")?;
    a.sync()?;
    let (live, removed) = picture(&mut a, node)?;
    assert!(
        removed && !live,
        "the newer removal stays: live {live}, removed {removed}"
    );
    a.sync()?;
    assert!(!a.root.join("f.txt").exists(), "{:?}", a.tree_text(""));
    Ok(())
}

/// The answer's check and write and the pull's recording take turns: a pull asked for while an
/// answer's transaction is open (between its check and its commit) waits for that commit, and
/// records the newer state over the answer then. Writes to a state DB take turns at its
/// process's gate ([`StateDb::write`]), the account's connection the pulls record through and
/// the folder's alike, and in SQLite's write lock across processes.
#[test]
fn a_pull_waits_for_an_answers_commit() -> R {
    use hbsync_state::testkit::{Commit, Hook, on_commit};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::Duration;

    let (mut a, b, node) = two_files()?;

    // Armed once the rename landed and another computer removed the file: at the next commit
    // (the answer's), a pull starts while its transaction is open.
    type Shared<T> = Arc<Mutex<Option<T>>>;
    let armed = Arc::new(AtomicBool::new(false));
    let drive: Shared<Arc<Drive>> = Arc::default();
    let pulling: Shared<JoinHandle<Result<(), String>>> = Arc::default();
    // (the pull still waiting once the answer's transaction was open a while, what the
    // answer's commit left in the mirror: its name)
    let seen: Shared<(bool, Option<String>)> = Arc::default();
    let reader: Shared<StateDb> = Arc::default();
    let hook = {
        let (armed, drive, pulling, seen, reader) = (
            Arc::clone(&armed),
            Arc::clone(&drive),
            Arc::clone(&pulling),
            Arc::clone(&seen),
            Arc::clone(&reader),
        );
        Hook(Arc::new(move |at| match at {
            Commit::Before if armed.swap(false, Ordering::SeqCst) => {
                let drive = drive.lock().expect("drive").clone().expect("the drive");
                let pull = std::thread::spawn(move || {
                    drive.pull(None).map(drop).map_err(|e| e.to_string())
                });
                std::thread::sleep(Duration::from_millis(300));
                *seen.lock().expect("seen") = Some((!pull.is_finished(), None));
                *pulling.lock().expect("pulling") = Some(pull);
            }
            Commit::After => {
                let mut seen = seen.lock().expect("seen");
                if let Some((waited, name @ None)) = seen.as_mut()
                    && let Some(db) = reader.lock().expect("reader").as_mut()
                {
                    let row = db.read(|s| s.get_mirror(node)).expect("a read");
                    *name = Some(row.map(|r| r.name).unwrap_or_default());
                    // Still waiting: this commit holds the gate until it returns.
                    *waited &= !pulling
                        .lock()
                        .expect("pulling")
                        .as_ref()
                        .is_some_and(JoinHandle::is_finished);
                }
            }
            _ => {}
        }))
    };
    on_commit(&a.db_path(), Some(hook));
    // The pull lands while the next round plans: its debug check of the narrow view against
    // the whole one reads twice, and would see it in between.
    let (remote, d) = meanwhile_with(&mut a, |cfg| cfg.check_narrow = false)?;
    *drive.lock().map_err(|_| "poisoned")? = Some(d);
    *reader.lock().map_err(|_| "poisoned")? = Some(StateDb::open_account(&a.db_path())?);
    let arm = Arc::clone(&armed);
    *remote.on_update.lock().map_err(|_| "poisoned")? = Some(Box::new(move |w: &Written| {
        b.set_removed(std::slice::from_ref(&w.target), true)
            .expect("b removes it");
        arm.store(true, Ordering::SeqCst);
    }));
    fs::rename(a.root.join("f.txt"), a.root.join("g.txt"))?;
    let synced = a.sync();
    on_commit(&a.db_path(), None);
    synced?;
    let pull = pulling.lock().map_err(|_| "poisoned")?.take();
    pull.ok_or("no pull started")?
        .join()
        .map_err(|_| "the pull panicked")??;
    let seen = seen.lock().map_err(|_| "poisoned")?.clone();
    assert_eq!(
        seen,
        Some((true, Some("g.txt".to_owned()))),
        "the pull waited for the answer's commit, which recorded the rename"
    );
    let (live, removed) = picture(&mut a, node)?;
    assert!(removed && !live, "the pull's newer removal over the answer");
    // The rename settled against its answer, which the pull's removal came after: the next
    // round deleted it here already, or the base waits at the name renamed.
    let base = a.engine.db().read(|s| s.get_base(node))?;
    match base {
        None => assert!(!a.root.join("g.txt").exists(), "{:?}", a.tree_text("")),
        Some(base) => assert_eq!(base.state.name, "g.txt"),
    }
    Ok(())
}
