//! Two folders of one account on one computer: the main folder and a role folder (Documents),
//! each with its engine, sharing the account's state DB and drive (sync semantics §12).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hbsync_engine::scope::Scope;
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
        let mut docs_cfg = config(&docs_root);
        docs_cfg.scope = Arc::new(Scope::below(Role::Documents.node()));
        let docs = Engine::new(StateDb::open_folder(&db, docs)?, drive, docs_cfg);
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
    let deleted = pc.main.drive().recently_deleted(10)?;
    assert_eq!(
        deleted.iter().map(|d| d.path.as_str()).collect::<Vec<_>>(),
        ["notes.txt"]
    );
    Ok(())
}
