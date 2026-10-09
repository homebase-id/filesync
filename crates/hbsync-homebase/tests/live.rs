//! Against a live identity: `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync-homebase
//! --test live -- --ignored`. Needs the credentials `hbsync-register` saves for it. Each run
//! works in its own top-level folder and deletes it at the end.

use std::time::Duration;

use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::notify::{Event, Notifier};
use hbsync_homebase::{Client, Credentials};
use hbsync_remote::contract::{self, Bench};

type R = Result<(), Box<dyn std::error::Error>>;

fn live_remote() -> Result<HomebaseRemote, Box<dyn std::error::Error>> {
    let identity = std::env::var("HBSYNC_LIVE")
        .map_err(|_| "set HBSYNC_LIVE to an identity registered with hbsync-register")?;
    let path = Credentials::dev_path(&identity)?;
    let creds =
        Credentials::load(&path)?.ok_or_else(|| format!("no credentials at {}", path.display()))?;
    Ok(HomebaseRemote::connect(Client::new(&creds)?)?)
}

fn run_name(what: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    format!("hbsync-test {what} {now}")
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn the_contract_holds_on_a_live_drive() -> R {
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("contract"), Duration::from_millis(20))?;
    let failed = bench.run_all();
    let cleanup = bench.cleanup();
    assert!(failed.is_empty(), "{failed:#?}");
    cleanup?;
    let skipped = remote.take_skipped();
    assert!(skipped.is_empty(), "feed rows skipped: {skipped:#?}");
    Ok(())
}

#[test]
#[ignore = "needs a live identity (HBSYNC_LIVE)"]
fn a_change_on_the_drive_is_announced() -> R {
    let remote = live_remote()?;
    let bench = Bench::new(&remote, &run_name("notify"), Duration::ZERO)?;
    let (tx, rx) = std::sync::mpsc::channel();
    let notifier = Notifier::spawn(remote.client().clone(), remote.drive(), tx);
    let wait = |what: Event| -> Result<(), String> {
        loop {
            match rx.recv_timeout(Duration::from_secs(15)) {
                Ok(e) if e == what => return Ok(()),
                Ok(Event::Lost(why)) => return Err(why),
                Ok(_) => {}
                Err(e) => return Err(format!("waiting for {what:?}: {e}")),
            }
        }
    };
    let result = (|| -> R {
        wait(Event::Connected)?;
        let start = std::time::Instant::now();
        bench.create(&contract::folder("inside", bench.scratch()), None)?;
        wait(Event::Changed)?;
        let took = start.elapsed();
        assert!(took < Duration::from_secs(2), "announced after {took:?}");
        println!("announced {took:?} after the write returned");
        Ok(())
    })();
    drop(notifier);
    bench.cleanup()?;
    result
}
