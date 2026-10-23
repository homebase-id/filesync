use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

use hbsync_engine::transfer::{
    Download, Job, JobError, JobResult, SendHash, TransferConfig, run_jobs,
};
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_remote::{
    Answers, ChangesPage, Deletes, ErrorClass, ForGood, Holds, NodeMeta, Planned, RefsPage, Remote,
    RemoteHeader, Result as RemoteResult, Upload,
};
use hbsync_types::schema::NodeContent;
use hbsync_types::{Doomed, NodeId, NodeKind, RemoteFileId, Target, Written};

type R = Result<(), Box<dyn std::error::Error>>;

fn upload_job(dir: &std::path::Path, name: &str, size: usize) -> std::io::Result<Job> {
    let data = vec![b'x'; size];
    let path = dir.join(name);
    fs::write(&path, &data)?;
    let seen = hbsync_platform::stat(&path)?.fingerprint();
    Ok(Job::Upload {
        meta: NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file(name, 0, size as u64, false),
            removed: false,
        },
        target: None,
        path,
        hash: SendHash::Known(hash_bytes(&data, HashAlgorithm::Blake3)),
        size: size as u64,
        seen,
    })
}

#[test]
fn transfers_run_in_parallel() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    remote.faults().latency = Duration::from_millis(100);
    let jobs = (0..8)
        .map(|i| upload_job(dir.path(), &format!("f{i}"), 10))
        .collect::<Result<Vec<_>, _>>()?;
    let start = Instant::now();
    let mut ok = 0;
    run_jobs(
        &remote,
        TransferConfig {
            workers: 4,
            ..TransferConfig::default()
        },
        jobs,
        |_, r| {
            if matches!(r, JobResult::Written(_)) {
                ok += 1;
            }
        },
    );
    assert_eq!(ok, 8);
    // Serially this takes 800 ms; four workers need about 200.
    assert!(
        start.elapsed() < Duration::from_millis(600),
        "{:?}",
        start.elapsed()
    );
    Ok(())
}

#[test]
fn only_a_few_large_files_at_once() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    remote.faults().latency = Duration::from_millis(100);
    // Two large files at a time: the ones over the limit wait; the small ones don't wait for them.
    let mut jobs = Vec::new();
    for i in 0..4 {
        jobs.push(upload_job(dir.path(), &format!("large{i}"), 1000)?);
    }
    for i in 0..8 {
        jobs.push(upload_job(dir.path(), &format!("small{i}"), 10)?);
    }
    let start = Instant::now();
    let mut done = Vec::new();
    run_jobs(
        &remote,
        TransferConfig {
            workers: 10,
            large_at_once: 2,
            large_from: 1000,
            ..TransferConfig::default()
        },
        jobs,
        |job, r| {
            assert!(matches!(r, JobResult::Written(_)));
            done.push((job.size(), start.elapsed()));
        },
    );
    assert_eq!(done.len(), 12);
    // Two large files a round: two rounds for four. All ten workers at once would take one.
    let last_large = done
        .iter()
        .filter(|(size, _)| *size == 1000)
        .map(|d| d.1)
        .max();
    assert!(last_large >= Some(Duration::from_millis(200)), "{done:?}");
    // The small files ran beside the first two large ones, not after them.
    let last_small = done
        .iter()
        .filter(|(size, _)| *size == 10)
        .map(|d| d.1)
        .max();
    assert!(last_small < last_large, "{done:?}");
    Ok(())
}

#[test]
fn smallest_first() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let jobs = vec![
        upload_job(dir.path(), "large", 3000)?,
        upload_job(dir.path(), "small", 10)?,
        upload_job(dir.path(), "medium", 500)?,
    ];
    let names: std::collections::HashMap<NodeId, String> = jobs
        .iter()
        .map(|j| match j {
            Job::Upload { meta, .. } => (meta.node, meta.content.name.clone()),
            _ => unreachable!(),
        })
        .collect();
    let mut order = Vec::new();
    run_jobs(
        &remote,
        TransferConfig {
            workers: 1,
            ..TransferConfig::default()
        },
        jobs,
        |job, _| order.push(names[&job.node()].clone()),
    );
    assert_eq!(order, ["small", "medium", "large"]);
    Ok(())
}

#[test]
fn interrupted_download_resumes_without_refetching() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let hash = hash_bytes(&data, HashAlgorithm::Blake3);
    let node = NodeId::new();
    let new = NodeMeta {
        node,
        parent: NodeId::ROOT,
        kind: NodeKind::File,
        content: NodeContent::file("big", 0, data.len() as u64, false),
        removed: false,
    };
    let written = remote.create(
        &new,
        Some(Upload {
            hash: Planned::Known(hash),
            size: data.len() as u64,
            reader: &mut std::io::Cursor::new(data.as_slice()),
            pace: None,
        }),
    )?;
    let job = Job::Download(Download {
        node,
        file_id: written.at().file_id,
        expected: hash,
        size: data.len() as u64,
        mtime_ms: Some(1_700_000_000_000),
        executable: None,
        tmp_dir: dir.path().join("tmp"),
        key: "big".into(),
        local_algorithm: HashAlgorithm::Blake3,
    });
    let cfg = TransferConfig {
        workers: 1,
        ..TransferConfig::default()
    };

    // Cut before the download's buffer is full: what came still reaches the partial file.
    remote.faults().cut_download_after = Some(30_000);
    let mut first = None;
    run_jobs(&remote, cfg, vec![job.clone()], |_, r| first = Some(r));
    assert!(
        matches!(first, Some(JobResult::Failed(ref e)) if e.class() == ErrorClass::Transient),
        "{first:?}"
    );

    let mut second = None;
    run_jobs(&remote, cfg, vec![job], |_, r| second = Some(r));
    let Some(JobResult::Downloaded { file, local_hash }) = second else {
        panic!("{second:?}")
    };
    assert_eq!(local_hash, hash);
    assert_eq!(fs::read(file.path())?, data);
    assert_eq!(
        remote.faults().served,
        data.len() as u64,
        "the second try only fetched the rest"
    );
    Ok(())
}

#[test]
fn bandwidth_cap_slows_transfers() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let jobs = vec![
        upload_job(dir.path(), "a", 150_000)?,
        upload_job(dir.path(), "b", 150_000)?,
    ];
    let start = Instant::now();
    run_jobs(
        &remote,
        TransferConfig {
            workers: 2,
            max_bytes_per_sec: Some(600_000),
            ..TransferConfig::default()
        },
        jobs,
        |_, _| {},
    );
    // 300 kB at 600 kB/s ≈ 0.5 s, whatever the number of workers.
    assert!(
        start.elapsed() >= Duration::from_millis(400),
        "{:?}",
        start.elapsed()
    );
    Ok(())
}

#[test]
fn results_arrive_while_other_transfers_still_run() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let jobs = vec![
        upload_job(dir.path(), "small", 10)?,
        // Throttled so it takes a while.
        upload_job(dir.path(), "large", 400_000)?,
    ];
    let start = Instant::now();
    let mut arrivals = Vec::new();
    run_jobs(
        &remote,
        TransferConfig {
            workers: 2,
            max_bytes_per_sec: Some(800_000),
            ..TransferConfig::default()
        },
        jobs,
        |job, _| {
            arrivals.push((job.size(), start.elapsed()));
        },
    );
    let (small, large) = (arrivals[0], arrivals[1]);
    assert_eq!((small.0, large.0), (10, 400_000));
    assert!(
        small.1 + Duration::from_millis(200) < large.1,
        "{arrivals:?}"
    );
    Ok(())
}

/// A remote that appends to `path` once it has read `after` bytes of an upload: the file
/// changes while it is sent.
struct ChangesWhileSent {
    inner: FakeRemote,
    path: std::path::PathBuf,
    after: usize,
}

impl Remote for ChangesWhileSent {
    fn changes(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<ChangesPage> {
        self.inner.changes(cursor, limit)
    }
    fn change_refs(&self, cursor: Option<&str>, limit: usize) -> RemoteResult<RefsPage> {
        self.inner.change_refs(cursor, limit)
    }
    fn header(&self, file_id: RemoteFileId) -> RemoteResult<RemoteHeader> {
        self.inner.header(file_id)
    }
    fn holding(&self, folders: &[NodeId], holds: Holds<'_>) -> RemoteResult<HashSet<NodeId>> {
        self.inner.holding(folders, holds)
    }
    fn create(&self, node: &NodeMeta, payload: Option<Upload<'_>>) -> RemoteResult<Written> {
        let Some(upload) = payload else {
            return self.inner.create(node, None);
        };
        let mut first = vec![0; self.after];
        upload.reader.read_exact(&mut first)?;
        fs::OpenOptions::new()
            .append(true)
            .open(&self.path)?
            .write_all(b"more")?;
        let mut rest = std::io::Cursor::new(first).chain(upload.reader);
        self.inner.create(
            node,
            Some(Upload {
                reader: &mut rest,
                ..upload
            }),
        )
    }
    fn update(
        &self,
        target: &Target,
        meta: &NodeMeta,
        payload: Option<Upload<'_>>,
    ) -> RemoteResult<Written> {
        self.inner.update(target, meta, payload)
    }
    fn delete(&self, files: &[Doomed]) -> RemoteResult<Deletes> {
        self.inner.delete(files)
    }
    fn delete_for_good(&self, files: &[Doomed]) -> RemoteResult<ForGood> {
        self.inner.delete_for_good(files)
    }
    fn set_removed(&self, files: &[Target], removed: bool) -> RemoteResult<Answers<Written>> {
        self.inner.set_removed(files, removed)
    }
    fn cursor_time(&self, cursor: &str) -> Option<i64> {
        self.inner.cursor_time(cursor)
    }
    fn read(
        &self,
        file_id: RemoteFileId,
        range: Option<std::ops::Range<u64>>,
        sink: &mut dyn Write,
    ) -> RemoteResult<u64> {
        self.inner.read(file_id, range, sink)
    }
}

/// An upload's file that changed since it was hashed (in any way its fingerprint shows) isn't
/// sent; one that changes while it is sent stops at the next look at it, not at its end. Both
/// are left for the scan the change brings (stale), not tried again at once.
#[test]
fn an_upload_whose_file_changed_stops() -> R {
    let dir = tempfile::tempdir()?;
    let server = FakeServer::new(ServerConfig::default());
    let remote = FakeRemote::new(server.clone());
    let job = upload_job(dir.path(), "before", 10)?;
    // The same size, another change time.
    std::thread::sleep(Duration::from_millis(20));
    hbsync_platform::ops::set_mtime(&dir.path().join("before"), 1_600_000_000_000_000_000)?;
    let mut results = Vec::new();
    run_jobs(&remote, TransferConfig::default(), vec![job], |_, r| {
        results.push(r)
    });
    let [JobResult::Failed(e)] = &results[..] else {
        return Err(format!("{results:?}").into());
    };
    assert!(matches!(e, JobError::Changed), "{e}");
    assert_eq!(e.class(), ErrorClass::Stale, "{e}");
    assert_eq!(remote.faults().uploads, 0, "nothing was sent");

    let size = 40 << 20;
    let job = upload_job(dir.path(), "during", size)?;
    let midway = ChangesWhileSent {
        inner: remote.clone(),
        path: dir.path().join("during"),
        after: 20 << 20,
    };
    let mut results = Vec::new();
    run_jobs(&midway, TransferConfig::default(), vec![job], |_, r| {
        results.push(r)
    });
    let [JobResult::Failed(e)] = &results[..] else {
        return Err(format!("{results:?}").into());
    };
    // Its own look, not the hash of what was sent.
    assert!(matches!(e, JobError::Changed), "{e}");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 0);
    Ok(())
}

/// An upload hashed on send (sync semantics §3): the answer gives the hash of the bytes sent.
/// One whose file changes while it is sent, too small for a look on the way, is caught by the
/// look after its last byte: stale, nothing stored.
#[test]
fn an_upload_hashed_on_send_is_looked_at_after_its_last_byte() -> R {
    let dir = tempfile::tempdir()?;
    let server = FakeServer::new(ServerConfig::default());
    let remote = FakeRemote::new(server.clone());
    let unread = |mut job: Job| {
        if let Job::Upload { hash, .. } = &mut job {
            *hash = SendHash::OnSend(HashAlgorithm::Blake3);
        }
        job
    };
    let mut results = Vec::new();
    let job = unread(upload_job(dir.path(), "sent", 10)?);
    run_jobs(&remote, TransferConfig::default(), vec![job], |_, r| {
        results.push(r)
    });
    let [JobResult::Written(w)] = &results[..] else {
        return Err(format!("{results:?}").into());
    };
    assert_eq!(
        w.content,
        Some(hash_bytes(&[b'x'; 10], HashAlgorithm::Blake3))
    );

    let job = unread(upload_job(dir.path(), "changed", 10)?);
    let midway = ChangesWhileSent {
        inner: remote.clone(),
        path: dir.path().join("changed"),
        after: 5,
    };
    let mut results = Vec::new();
    run_jobs(&midway, TransferConfig::default(), vec![job], |_, r| {
        results.push(r)
    });
    let [JobResult::Failed(e)] = &results[..] else {
        return Err(format!("{results:?}").into());
    };
    assert!(matches!(e, JobError::Changed), "{e}");
    assert_eq!(server.lock().map_err(|_| "poisoned")?.live().count(), 1);
    Ok(())
}
