//! Two folders of one account on one computer: the main folder and a role folder (Documents),
//! each with its engine, sharing the account's state DB and drive (sync semantics §12).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hbsync_engine::root::{AccountReader, SyncRoot};
use hbsync_engine::{Drive, Engine, EngineConfig, SyncReport};
use hbsync_fileio::HashAlgorithm;
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_remote::{NodeMeta, RemoteBody};
use hbsync_state::StateDb;
use hbsync_types::{NodeId, Role};

type R = Result<(), Box<dyn std::error::Error>>;

/// A computer syncing the main folder and Documents of one account.
struct Computer {
    _dir: tempfile::TempDir,
    main_root: PathBuf,
    docs_root: PathBuf,
    main: Engine,
    docs: Engine,
}

fn config(root: &Path) -> EngineConfig {
    let mut cfg = EngineConfig::new(root, "pc");
    cfg.exec.retry_pause = std::time::Duration::from_millis(1);
    cfg
}

impl Computer {
    fn new(server: &Arc<Mutex<FakeServer>>) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let base = dir.path().canonicalize()?;
        let (main_root, docs_root) = (base.join("Homebase"), base.join("Documents"));
        for root in [&main_root, &docs_root] {
            std::fs::create_dir_all(root.join(hbsync_local::STATE_DIR))?;
        }
        let db = base.join("state.db");
        let mut main_db = StateDb::open(&db)?;
        let docs = main_db
            .write(|s| s.add_folder(Role::Documents.node(), &docs_root.to_string_lossy(), "m"))?;
        let drive = Drive::new(
            StateDb::open_account(&db)?,
            Arc::new(FakeRemote::new(server.clone())),
        );
        let main = Engine::new(main_db, drive.clone(), config(&main_root));
        let docs = Engine::new(StateDb::open_folder(&db, docs)?, drive, config(&docs_root));
        Ok(Computer {
            _dir: dir,
            main_root,
            docs_root,
            main,
            docs,
        })
    }

    fn sync(&mut self) -> R {
        sync(&mut self.main)?;
        sync(&mut self.docs)?;
        sync(&mut self.main)?;
        Ok(())
    }
}

/// Scan, then sync until idle; nothing may fail.
fn sync(engine: &mut Engine) -> Result<SyncReport, Box<dyn std::error::Error>> {
    let mut total = SyncReport::default();
    for _ in 0..6 {
        engine.scan_local()?;
        let pass = engine.sync_once("2026-10-14")?;
        let idle = pass.ops_done == 0;
        total.absorb(pass);
        if idle {
            break;
        }
    }
    assert!(total.failed.is_empty(), "failed: {:?}", total.failed);
    Ok(total)
}

/// The files under `root` (`.hbsync` left out), sorted.
fn files(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path
                .file_name()
                .is_some_and(|n| n == hbsync_local::STATE_DIR)
            {
                continue;
            }
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(rel) = path.strip_prefix(root) {
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    out.sort();
    out
}

/// Where a live node named `name` is on the server: its parent.
fn parent_on_server(server: &Arc<Mutex<FakeServer>>, name: &str) -> Option<NodeId> {
    let srv = server.lock().ok()?;
    srv.live().find_map(|h| match &h.body {
        RemoteBody::Live { content, .. } if content.name == name => Some(h.parent),
        _ => None,
    })
}

fn server_with_documents() -> Result<Arc<Mutex<FakeServer>>, Box<dyn std::error::Error>> {
    let server = FakeServer::new(ServerConfig::default());
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let docs = NodeMeta {
            node: Role::Documents.node(),
            ..hbsync_remote::contract::folder("Documents", NodeId::ROOT)
        };
        srv.create(&docs, None)?;
        srv.seed_file(
            Role::Documents.node(),
            "cv.txt",
            b"cv",
            HashAlgorithm::Blake3,
        )?;
        srv.seed_file(NodeId::ROOT, "notes.txt", b"n", HashAlgorithm::Blake3)?;
    }
    Ok(server)
}

#[test]
fn each_folder_syncs_its_part_of_the_drive() -> R {
    let server = server_with_documents()?;
    let mut pc = Computer::new(&server)?;
    pc.sync()?;
    assert_eq!(
        files(&pc.main_root),
        ["notes.txt"],
        "no Documents in the main folder"
    );
    assert_eq!(files(&pc.docs_root), ["cv.txt"]);

    // New files go to their folder's part of the drive.
    std::fs::write(pc.docs_root.join("letter.txt"), "l")?;
    std::fs::create_dir(pc.main_root.join("Projects"))?;
    std::fs::write(pc.main_root.join("Projects/plan.txt"), "p")?;
    pc.sync()?;
    assert_eq!(
        parent_on_server(&server, "letter.txt"),
        Some(Role::Documents.node())
    );
    assert_ne!(
        parent_on_server(&server, "plan.txt"),
        Some(Role::Documents.node())
    );
    assert_eq!(files(&pc.main_root), ["Projects/plan.txt", "notes.txt"]);
    assert_eq!(files(&pc.docs_root), ["cv.txt", "letter.txt"]);
    Ok(())
}

/// A file moved on the drive from Documents to the main folder's part leaves Documents here and
/// comes to the main folder, with nothing lost.
#[test]
fn a_move_between_the_folders_parts_follows() -> R {
    let server = server_with_documents()?;
    let mut pc = Computer::new(&server)?;
    pc.sync()?;
    {
        let mut srv = server.lock().map_err(|_| "poisoned")?;
        let cv = srv
            .live()
            .find(
                |h| matches!(&h.body, RemoteBody::Live { content, .. } if content.name == "cv.txt"),
            )
            .cloned()
            .ok_or("no cv.txt")?;
        if let RemoteBody::Live { node, content, .. } = cv.body {
            let meta = NodeMeta {
                node,
                parent: NodeId::ROOT,
                kind: cv.kind,
                content,
            };
            srv.update(cv.file_id, cv.version, &meta, None)?;
        }
    }
    pc.sync()?;
    assert_eq!(files(&pc.main_root), ["cv.txt", "notes.txt"]);
    assert_eq!(files(&pc.docs_root), Vec::<String>::new());
    assert_eq!(std::fs::read_to_string(pc.main_root.join("cv.txt"))?, "cv");
    Ok(())
}

/// The two engines syncing at the same time on two threads: no busy DB, no change lost.
#[test]
fn two_engines_sync_at_once() -> R {
    let server = server_with_documents()?;
    let Computer {
        _dir,
        main_root,
        docs_root,
        mut main,
        mut docs,
    } = Computer::new(&server)?;
    for i in 0..20 {
        std::fs::write(main_root.join(format!("m{i}.txt")), "m")?;
        std::fs::write(docs_root.join(format!("d{i}.txt")), "d")?;
    }
    std::thread::scope(|scope| -> R {
        let a = scope.spawn(|| sync(&mut main).map(drop).map_err(|e| e.to_string()));
        let b = scope.spawn(|| sync(&mut docs).map(drop).map_err(|e| e.to_string()));
        a.join().map_err(|_| "panicked")??;
        b.join().map_err(|_| "panicked")??;
        Ok(())
    })?;
    sync(&mut main)?;
    sync(&mut docs)?;
    for i in 0..20 {
        assert_eq!(
            parent_on_server(&server, &format!("m{i}.txt")),
            Some(NodeId::ROOT)
        );
        assert_eq!(
            parent_on_server(&server, &format!("d{i}.txt")),
            Some(Role::Documents.node())
        );
    }
    assert_eq!(files(&main_root).len(), 21);
    assert_eq!(files(&docs_root).len(), 21);
    Ok(())
}

/// The user moves a file from the main folder to Documents on this computer: a delete there and
/// a new file here, nothing lost (the old node waits in Recently deleted).
#[test]
fn a_file_moved_across_the_folders_here_is_kept() -> R {
    let server = server_with_documents()?;
    let mut pc = Computer::new(&server)?;
    pc.sync()?;
    std::fs::rename(
        pc.main_root.join("notes.txt"),
        pc.docs_root.join("notes.txt"),
    )?;
    pc.sync()?;
    assert_eq!(files(&pc.main_root), Vec::<String>::new());
    assert_eq!(files(&pc.docs_root), ["cv.txt", "notes.txt"]);
    assert_eq!(
        parent_on_server(&server, "notes.txt"),
        Some(Role::Documents.node())
    );
    let deleted = AccountReader::open(&pc.main_root.with_file_name("state.db"), "t.example")?
        .ok_or("no state DB")?
        .recently_deleted(10)?;
    assert_eq!(
        deleted.iter().map(|d| d.path.as_str()).collect::<Vec<_>>(),
        ["notes.txt"]
    );
    Ok(())
}

/// A computer syncing only Documents, as set up on its account's DB, and its observer.
struct DocsOnly {
    _dir: tempfile::TempDir,
    root: PathBuf,
    engine: Engine,
    observer: hbsync_local::LocalObserver,
}

impl DocsOnly {
    fn new(
        server: &Arc<Mutex<FakeServer>>,
        files: &[(&str, &str)],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?.join("Documents");
        std::fs::create_dir_all(&root)?;
        for (name, text) in files {
            std::fs::write(root.join(name), text)?;
        }
        let db = root.with_file_name("state.db");
        let sync_root = hbsync_engine::root::SyncRoot::create_for(
            &db,
            "t.example",
            &root,
            Role::Documents.node(),
        )?;
        let drive = Drive::open(&db, "t.example", Arc::new(FakeRemote::new(server.clone())))?;
        let engine = Engine::open(&sync_root, drive, config(&root))?;
        let observer = hbsync_local::LocalObserver::start(&root, engine.scan_options().clone());
        Ok(DocsOnly {
            _dir: dir,
            root,
            engine,
            observer,
        })
    }

    /// Starts as the daemon does, then syncs until idle: what was synced.
    fn start(&mut self) -> Result<Vec<hbsync_types::activity::Synced>, Box<dyn std::error::Error>> {
        let first = self.engine.start_up(&mut self.observer, "2026-10-14")?;
        assert!(first.sync.failed.is_empty(), "{:?}", first.sync.failed);
        let mut synced = first.sync.synced;
        synced.extend(sync(&mut self.engine)?.synced);
        Ok(synced)
    }
}

/// The first computer adding Documents makes it on the drive; a second one whose Documents
/// holds mostly the same files joins it: those are adopted, nothing moves; a file that differs
/// keeps both versions; a new one goes up (plan E1).
#[test]
fn computers_join_documents_merging_what_they_hold() -> R {
    let server = FakeServer::new(ServerConfig::default());
    let mut a = DocsOnly::new(
        &server,
        &[("cv.txt", "cv"), ("notes.txt", "n"), ("letter.txt", "a's")],
    )?;
    a.start()?;
    assert_eq!(
        parent_on_server(&server, "cv.txt"),
        Some(Role::Documents.node()),
        "the role folder was made, its files in it"
    );

    let mut b = DocsOnly::new(
        &server,
        &[
            ("cv.txt", "cv"),
            ("notes.txt", "n"),
            ("letter.txt", "b's"),
            ("new.txt", "b"),
        ],
    )?;
    let synced = b.start()?;
    let moved: Vec<&str> = synced.iter().map(|s| s.path.as_str()).collect();
    assert!(
        !moved.iter().any(|p| *p == "cv.txt" || *p == "notes.txt"),
        "the same files are adopted, not transferred: {moved:?}"
    );
    assert!(moved.contains(&"new.txt"), "{moved:?}");
    a.start()?;
    let (on_a, on_b) = (files(&a.root), files(&b.root));
    assert_eq!(on_a, on_b);
    assert_eq!(on_a.len(), 5, "both letters kept: {on_a:?}");
    assert!(
        on_a.iter()
            .any(|f| f.starts_with("letter") && f.contains("conflict"))
    );
    Ok(())
}

/// Documents deleted on the drive (in Recently deleted) comes back, with what was in it, when a
/// computer adds it again (plan E1).
#[test]
fn documents_deleted_on_the_drive_comes_back_when_added_again() -> R {
    let server = server_with_documents()?;
    remove_documents(&server)?;
    let mut pc = DocsOnly::new(&server, &[])?;
    pc.start()?;
    assert_eq!(files(&pc.root), ["cv.txt"], "what was in it came back");
    assert_eq!(
        parent_on_server(&server, "cv.txt"),
        Some(Role::Documents.node())
    );
    Ok(())
}

/// Moves Documents (the folder itself) to Recently deleted on the server.
fn remove_documents(server: &Arc<Mutex<FakeServer>>) -> R {
    let mut srv = server.lock().map_err(|_| "poisoned")?;
    let docs = srv
        .live()
        .find(
            |h| matches!(&h.body, RemoteBody::Live { node, .. } if *node == Role::Documents.node()),
        )
        .cloned()
        .ok_or("no Documents")?;
    if let RemoteBody::Live { node, content, .. } = docs.body {
        let meta = hbsync_remote::contract::removed(&NodeMeta {
            node,
            parent: docs.parent,
            kind: docs.kind,
            content,
        });
        srv.update(docs.file_id, docs.version, &meta, None)?;
    }
    Ok(())
}

/// Documents deleted on the drive while a computer syncs it comes back at that computer's next
/// sync, nothing trashed here, nothing uploaded into Recently deleted (§12).
#[test]
fn documents_deleted_on_the_drive_while_synced_comes_back() -> R {
    let server = FakeServer::new(ServerConfig::default());
    let mut pc = DocsOnly::new(&server, &[("cv.txt", "cv")])?;
    pc.start()?;
    remove_documents(&server)?;
    std::fs::write(pc.root.join("new.txt"), "n")?;
    pc.start()?;
    assert_eq!(files(&pc.root), ["cv.txt", "new.txt"]);
    assert_eq!(
        parent_on_server(&server, "cv.txt"),
        Some(Role::Documents.node())
    );
    assert_eq!(
        parent_on_server(&server, "new.txt"),
        Some(Role::Documents.node())
    );
    let srv = server.lock().map_err(|_| "poisoned")?;
    assert!(
        srv.live().any(
            |h| matches!(&h.body, RemoteBody::Live { node, .. } if *node == Role::Documents.node())
        ),
        "Documents is live again"
    );
    Ok(())
}

/// A folder's engine keeps its state in its drive's DB: one on another DB is refused.
#[test]
fn an_engine_on_another_drives_db_is_refused() -> R {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("Homebase");
    std::fs::create_dir_all(&root)?;
    let db = dir.path().join("a.db");
    let sync_root = SyncRoot::create(&db, "t.example", &root)?;
    let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
    let other = Drive::open(&dir.path().join("b.db"), "t.example", remote.clone())?;
    assert!(Engine::open(&sync_root, other, config(&root)).is_err());
    let ours = Drive::open(&db, "t.example", remote)?;
    assert!(Engine::open(&sync_root, ours, config(&root)).is_ok());
    Ok(())
}
