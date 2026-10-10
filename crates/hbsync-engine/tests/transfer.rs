use std::fs;
use std::time::{Duration, Instant};

use hbsync_engine::transfer::{Job, JobResult, TransferConfig, run_jobs};
use hbsync_fileio::{HashAlgorithm, hash_bytes};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_remote::{NodeMeta, Remote, Upload};
use hbsync_types::schema::NodeContent;
use hbsync_types::{NodeId, NodeKind};

type R = Result<(), Box<dyn std::error::Error>>;

fn upload_job(dir: &std::path::Path, name: &str, size: usize) -> std::io::Result<Job> {
    let data = vec![b'x'; size];
    let path = dir.join(name);
    fs::write(&path, &data)?;
    Ok(Job::Upload {
        meta: NodeMeta {
            node: NodeId::new(),
            parent: NodeId::ROOT,
            kind: NodeKind::File,
            content: NodeContent::file(name, 0, size as u64, false),
        },
        target: None,
        path,
        hash: hash_bytes(&data, HashAlgorithm::Blake3),
        size: size as u64,
    })
}

#[test]
fn transfers_run_in_parallel() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    remote.faults().latency = Duration::from_millis(100);
    let jobs = (0..8)
        .map(|i| upload_job(dir.path(), &format!("f{i}"), 10).map(|j| (false, j)))
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
            if matches!(r, JobResult::Uploaded(_)) {
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
    // Urgent large files first: the ones over the limit wait, and small files go meanwhile.
    let mut jobs = Vec::new();
    for i in 0..4 {
        jobs.push((true, upload_job(dir.path(), &format!("large{i}"), 1000)?));
    }
    for i in 0..8 {
        jobs.push((false, upload_job(dir.path(), &format!("small{i}"), 10)?));
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
            assert!(matches!(r, JobResult::Uploaded(_)));
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
fn urgent_first_then_smallest_first() -> R {
    let dir = tempfile::tempdir()?;
    let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
    let jobs = vec![
        (false, upload_job(dir.path(), "large", 3000)?),
        (false, upload_job(dir.path(), "small", 10)?),
        (true, upload_job(dir.path(), "wanted", 5000)?),
        (false, upload_job(dir.path(), "medium", 500)?),
    ];
    let names: std::collections::HashMap<NodeId, String> = jobs
        .iter()
        .map(|(_, j)| match j {
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
    assert_eq!(order, ["wanted", "small", "medium", "large"]);
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
    };
    let written = remote.create(
        &new,
        Some(Upload {
            hash,
            size: data.len() as u64,
            reader: &mut std::io::Cursor::new(data.as_slice()),
            pace: None,
        }),
    )?;
    let job = Job::Download {
        node,
        file_id: written.file_id,
        expected: hash,
        size: data.len() as u64,
        mtime_ms: 1_700_000_000_000,
        executable: None,
        tmp_dir: dir.path().join("tmp"),
        key: "big".into(),
        local_algorithm: HashAlgorithm::Blake3,
    };
    let cfg = TransferConfig {
        workers: 1,
        ..TransferConfig::default()
    };

    remote.faults().cut_download_after = Some(30_000);
    let mut first = None;
    run_jobs(&remote, cfg, vec![(false, job.clone())], |_, r| {
        first = Some(r)
    });
    assert!(
        matches!(first, Some(JobResult::Failed(ref e)) if e.is_transient()),
        "{first:?}"
    );

    let mut second = None;
    run_jobs(&remote, cfg, vec![(false, job)], |_, r| second = Some(r));
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
        (false, upload_job(dir.path(), "a", 150_000)?),
        (false, upload_job(dir.path(), "b", 150_000)?),
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
        (false, upload_job(dir.path(), "small", 10)?),
        // Throttled so it takes a while.
        (false, upload_job(dir.path(), "large", 400_000)?),
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
