mod common;
use common::{Client, server};
use std::time::Instant;

#[test]
#[ignore]
fn bench_first_sync_on_disk() -> common::Res<()> {
    let server = server();
    let mut a = Client::new(&server, "a")?;
    for d in 0..20 {
        for f in 0..50 {
            a.write(&format!("d{d}/f{f}.txt"), format!("{d}/{f}"))?;
        }
    }
    let t = Instant::now();
    let ra = a.sync()?;
    let up = t.elapsed();
    let mut b = Client::new(&server, "b")?;
    let t = Instant::now();
    b.engine.scan_local()?;
    eprintln!("scan {:?}", t.elapsed());
    let t2 = Instant::now();
    let r1 = b.engine.sync_once("2026-10-08")?;
    eprintln!(
        "sync_once {:?} rounds {} ops {} adopted {}",
        t2.elapsed(),
        r1.rounds,
        r1.ops_done,
        r1.adopted
    );
    let rb = b.sync()?;
    let down = t.elapsed();
    eprintln!(
        "BENCH upload {} ops {:?}; download {} ops {:?}",
        ra.ops_done, up, rb.ops_done, down
    );
    Ok(())
}
