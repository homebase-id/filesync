//! The account's feed of the drive's changes: [`Notifications`] turns the notification
//! socket's events into asks, which [`Puller`] takes from the drive on a thread of its own.

use std::sync::mpsc::{Receiver, RecvTimeoutError, SendError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use hbsync_engine::Drive;
use hbsync_homebase::notify::Event;
use hbsync_remote::{ChangeRef, ErrorClass};

use crate::protocol::Hints;
use crate::sync::lock;

/// While the notifications aren't connected (not yet, not any more, or never: a proxy, a
/// refusal), the drive is read this often; never while they are.
pub const UNANNOUNCED_READ: Duration = Duration::from_secs(300);

/// What the identity's notifications do to its drive: changes announced are taken once the
/// announcements have quieted (a burst of deletes reaches the mass-delete guard whole), the
/// feed read only for what the store doesn't know already ([`Drive::announced`]); connecting
/// (again) pulls at once, as changes made meanwhile weren't announced; while not connected,
/// the drive is read every [`UNANNOUNCED_READ`]. Nothing is pulled while paused. A pull wakes
/// the folders its changes concern.
#[derive(Debug)]
pub struct Notifications {
    puller: Puller,
    announced: Announced,
    /// The live changes announced since the last take, up to [`MAX_LIVE_REFS`], and whether
    /// the feed is to be read too (an announcement didn't say which file, or there were more).
    /// A hard delete isn't gathered here: the puller takes it within [`GATHER_WINDOW`].
    changes: Vec<ChangeRef>,
    unsaid: bool,
    /// Connected: the drive's mark of this connection ([`Drive::connection`]).
    connected: Option<u64>,
    /// Connected while paused: the read that follows connecting waits for the resume.
    connect_read: bool,
    /// While not connected: the next read of the drive.
    next_read: Instant,
}

/// Live changes kept while announcements gather (or the account is paused): more are read
/// from the feed instead, in one go.
const MAX_LIVE_REFS: usize = 2_000;

impl Notifications {
    /// Not connected yet: the first read of the drive comes [`UNANNOUNCED_READ`] after `now`
    /// (the folders' start-up has just read it).
    pub fn new(puller: Puller, now: Instant) -> Self {
        Notifications {
            puller,
            announced: Announced::default(),
            changes: Vec::new(),
            unsaid: false,
            connected: None,
            connect_read: false,
            next_read: now + UNANNOUNCED_READ,
        }
    }

    /// Takes `event` at `now`; the account's notification status when it changed.
    pub fn event(&mut self, event: Event, now: Instant, paused: bool) -> Option<Hints> {
        match event {
            Event::Connected => {
                // The credentials work: what waited for a login asks the drive again.
                self.puller.drive.allow();
                // Changes made while not connected weren't announced: the feed is read, now or
                // at the resume, and it brings what was gathered too. Only once that read
                // worked are the drive's changes taken as announced ([`Drive::current`]).
                self.connected = Some(self.puller.drive.connection());
                self.announced = Announced::default();
                self.changes.clear();
                self.unsaid = false;
                self.connect_read = true;
                if !paused {
                    self.read_connected();
                }
                Some(Hints::Connected)
            }
            Event::Changed(change) => {
                match change {
                    // In no feed: taken within the puller's window, also while paused (it
                    // writes only the account's DB; the folders sync it at the resume).
                    Some(change) if change.deleted => {
                        self.puller.send(vec![change], false, self.connected);
                    }
                    Some(change) => {
                        self.announced.note(now);
                        if !self.unsaid {
                            self.changes.push(change);
                            if self.changes.len() > MAX_LIVE_REFS {
                                self.read_instead();
                            }
                        }
                    }
                    None => {
                        self.announced.note(now);
                        self.read_instead();
                    }
                }
                None
            }
            Event::Lost(why) => {
                tracing::info!("notifications lost ({why}); reconnecting");
                self.disconnected(now);
                Some(Hints::Lost(why))
            }
            Event::Refused(why) => {
                tracing::warn!("notifications refused ({why}); asking again later");
                // The identity refused the credentials: the folders and the puller don't ask.
                self.puller.drive.deny(&why);
                self.disconnected(now);
                Some(Hints::Refused(why))
            }
        }
    }

    fn disconnected(&mut self, now: Instant) {
        // What was announced and not taken yet may be lost with it: behind.
        self.puller.drive.not_announcing();
        self.connect_read = false;
        if self.connected.take().is_some() {
            self.next_read = now + UNANNOUNCED_READ;
        }
    }

    /// The read that follows connecting.
    fn read_connected(&mut self) {
        self.connect_read = false;
        self.puller.send(Vec::new(), true, self.connected);
    }

    /// The live changes announced are read from the feed instead.
    fn read_instead(&mut self) {
        self.changes.clear();
        self.unsaid = true;
    }

    /// The account is ending: the puller takes what it gathered, and ends.
    pub fn finish(&self) {
        self.puller.finish();
    }

    /// Hands what was announced to the puller.
    fn take(&mut self) {
        let read = std::mem::take(&mut self.unsaid);
        self.puller
            .send(std::mem::take(&mut self.changes), read, self.connected);
    }

    /// Pulls what was announced once it has quieted, and the drive every [`UNANNOUNCED_READ`]
    /// while not connected; nothing while `paused` (a resume syncs every folder anyway).
    pub fn tick(&mut self, now: Instant, paused: bool) {
        if paused {
            return;
        }
        if self.connect_read {
            self.read_connected();
        }
        if self.announced.due(now) {
            self.take();
        }
        if self.reads_on_a_timer() && now >= self.next_read {
            self.next_read = now + UNANNOUNCED_READ;
            self.puller.ask();
        }
    }

    /// Whether the drive is read every [`UNANNOUNCED_READ`]: while not connected. (Refused, a
    /// read asks nothing: [`Drive::deny`].)
    fn reads_on_a_timer(&self) -> bool {
        self.connected.is_none()
    }

    /// When [`Notifications::tick`] has something to do (`paused`: never), `now` being now.
    pub fn deadline(&self, now: Instant, paused: bool) -> Option<Instant> {
        if paused {
            return None;
        }
        let unannounced = self.reads_on_a_timer().then_some(self.next_read);
        [
            self.connect_read.then_some(now),
            self.announced.deadline(),
            unannounced,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// What waits to be taken: the changes kept, and whether the feed is to be read too.
    #[cfg(test)]
    fn waiting(&self) -> (&[ChangeRef], bool) {
        (&self.changes, self.unsaid)
    }
}

/// A sync announced by the drive waits until its changes have stopped coming this long...
const REMOTE_QUIET: Duration = Duration::from_millis(1000);
/// ...but no longer than this after the first.
const REMOTE_MAX_WAIT: Duration = Duration::from_secs(10);

/// Changes the drive announced: pulled once they stop coming (a burst of deletes reaches the
/// mass-delete guard whole), or [`REMOTE_MAX_WAIT`] after the first.
#[derive(Debug, Default)]
struct Announced(Option<(Instant, Instant)>);

impl Announced {
    /// The drive announced a change.
    fn note(&mut self, now: Instant) {
        let first = self.0.map_or(now, |(first, _)| first);
        self.0 = Some((first, now + REMOTE_QUIET));
    }

    /// When announced changes are due to be pulled ([`Announced::due`]); `None`: none were.
    fn deadline(&self) -> Option<Instant> {
        self.0
            .map(|(first, quiet)| quiet.min(first + REMOTE_MAX_WAIT))
    }

    /// Whether announced changes are due to be pulled now: they are taken then.
    fn due(&mut self, now: Instant) -> bool {
        let due = self.deadline().is_some_and(|at| now >= at);
        if due {
            self.0 = None;
        }
        due
    }
}

/// Pulls the drive's changes on a thread of its own, so whoever asks (the account's loop)
/// never waits for the network or the DB: asks while a pull runs are answered by one more.
/// Changes announced are taken first, and the feed read only for what they leave unknown. A
/// pull wakes the folders its changes concern; when it fails every folder is woken, and their
/// syncs pull (and back off) themselves. A hard delete announced that couldn't be taken is
/// kept and tried again: it is in no feed. A thread that ended is started again.
#[derive(Debug, Clone)]
pub struct Puller {
    asks: Arc<Mutex<Sender<Asked>>>,
    /// Its thread, joined when the account ends ([`Puller::finish`]).
    thread: Arc<Mutex<Option<JoinHandle<()>>>>,
    drive: Arc<Drive>,
    /// Rounds done (taken and read), for tests.
    rounds: crate::TestCount,
}

/// What the puller is asked: changes announced, whether to read the feed (asked to, or for
/// changes announced without saying which: an event without a file, or too many), and the
/// notifications' connection it was asked at, if connected: a read that works for it means
/// the drive's changes are announced ([`Drive::announcing_from`]).
type Asked = (Vec<ChangeRef>, bool, Option<u64>);

/// Hard deletes not taken (taking them failed) are tried again after this, then twice as
/// long each time up to [`HELD_RETRY_MAX`].
const HELD_RETRY: Duration = Duration::from_secs(1);
const HELD_RETRY_MAX: Duration = Duration::from_secs(300);
/// Hard deletes kept while taking them fails, one per file; more are left stale (written to
/// next, a file deleted for good answers not found).
const MAX_HELD: usize = 10_000;
/// A round that takes announced changes gathers what is announced for this long after its first
/// ask, or until it holds this many changes: one write for a burst (a folder of thousands of
/// files deleted is a notification per file), not one each. A change waits this long at most; a
/// plain read of the feed doesn't wait.
const GATHER_WINDOW: Duration = Duration::from_secs(1);
const GATHER_MAX: usize = 500;
/// What a round of the puller did, for the next: hard deletes not taken yet (taking them
/// failed; they are in no feed), and whether the feed was read.
#[derive(Debug, Default, PartialEq)]
struct Round {
    held: Vec<ChangeRef>,
    read: bool,
}

impl Puller {
    pub fn spawn(drive: Arc<Drive>) -> Puller {
        let rounds = crate::TestCount::default();
        let (asks, thread) = match start(&drive, &rounds) {
            Ok((asks, thread)) => (asks, Some(thread)),
            Err(e) => {
                tracing::error!("can't start reading the drive on its own ({e}): trying again");
                // A sender whose thread is gone: the first ask starts one.
                (channel().0, None)
            }
        };
        Puller {
            asks: Arc::new(Mutex::new(asks)),
            thread: Arc::new(Mutex::new(thread)),
            drive,
            rounds,
        }
    }

    /// Ends the puller's thread once it has taken what it gathered: the account is ending. A
    /// hard delete announced is in no feed; only a crash loses one gathered (a second's worth).
    pub fn finish(&self) {
        *lock(&self.asks) = channel().0;
        if let Some(thread) = lock(&self.thread).take() {
            let _ = thread.join();
        }
    }

    /// Reads the feed.
    pub fn ask(&self) {
        self.send(Vec::new(), true, None);
    }

    /// Takes announced `changes`, reading the feed for what they leave unknown or when one
    /// announcement didn't say which file (`unsaid`).
    pub fn take(&self, changes: Vec<ChangeRef>, unsaid: bool) {
        self.send(changes, unsaid, None);
    }

    /// Rounds the puller's thread has done.
    #[cfg(feature = "testkit")]
    pub fn rounds(&self) -> u64 {
        self.rounds.get()
    }

    /// Ends the puller's thread, as a panic in it would (tests: it is started again).
    #[cfg(test)]
    fn end_thread(&self) {
        *lock(&self.asks) = channel().0;
    }

    fn send(&self, changes: Vec<ChangeRef>, read: bool, connection: Option<u64>) {
        let mut asks = lock(&self.asks);
        let Err(SendError(ask)) = asks.send((changes, read, connection)) else {
            return;
        };
        // Never on the asker's thread (the account's loop): a thread of its own again.
        match start(&self.drive, &self.rounds) {
            Ok((again, thread)) => {
                tracing::warn!("reading the drive had stopped: started again");
                let _ = again.send(ask);
                *asks = again;
                *lock(&self.thread) = Some(thread);
            }
            Err(e) => {
                tracing::error!("can't start reading the drive ({e}): the folders will");
                self.drive.wake_all();
            }
        }
    }
}

/// Starts the puller's thread: it takes what is announced and reads the feed, one round at a
/// time, until every sender is gone (what it gathered taken first).
fn start(
    drive: &Arc<Drive>,
    rounds: &crate::TestCount,
) -> std::io::Result<(Sender<Asked>, JoinHandle<()>)> {
    let (asks, rx) = channel::<Asked>();
    let (mine, done) = (Arc::clone(drive), rounds.clone());
    let thread = std::thread::Builder::new()
        .name("hbsync-pull".into())
        .spawn(move || {
            // Hard deletes not taken.
            let (mut held, mut held_wait) = (Vec::<ChangeRef>::new(), HELD_RETRY);
            loop {
                let first = match held.is_empty() {
                    true => rx.recv().ok(),
                    false => match rx.recv_timeout(held_wait) {
                        Ok(ask) => Some(ask),
                        Err(RecvTimeoutError::Timeout) => Some((Vec::new(), false, None)),
                        Err(RecvTimeoutError::Disconnected) => None,
                    },
                };
                let Some(first) = first else { break };
                let (changes, read, connection) = merge(
                    std::mem::take(&mut held),
                    gathered(first, &rx, GATHER_WINDOW),
                );
                let r = round(
                    changes,
                    read,
                    |c| mine.announced(c).map_err(|e| e.to_string()),
                    || {
                        // Refused: the drive isn't asked (nor the folders woken to ask it)
                        // until the user acts or the notifications connect.
                        if mine.is_denied() {
                            return Ok(false);
                        }
                        match mine.pull(None) {
                            Ok(_) => Ok(true),
                            Err(e) if e.class() == ErrorClass::Denied => Ok(false),
                            Err(e) => Err(e.to_string()),
                        }
                    },
                    || mine.wake_all(),
                );
                if r.read
                    && let Some(connection) = connection
                {
                    mine.announcing_from(connection);
                }
                held_wait = match r.held.is_empty() {
                    true => HELD_RETRY,
                    false => (held_wait * 2).min(HELD_RETRY_MAX),
                };
                held = r.held;
                done.add();
            }
        })?;
    Ok((asks, thread))
}

/// The asks of one round: `first`, and when it takes changes, what comes within `window` of
/// it, until they hold [`GATHER_MAX`] changes (the rest wait for the next round); a plain read
/// takes only what is waiting already.
fn gathered(first: Asked, rx: &Receiver<Asked>, window: Duration) -> Vec<Asked> {
    if first.0.is_empty() {
        return std::iter::once(first).chain(rx.try_iter()).collect();
    }
    let until = Instant::now() + window;
    let mut count = first.0.len();
    let mut asks = vec![first];
    while count < GATHER_MAX {
        let Some(left) = until.checked_duration_since(Instant::now()) else {
            break;
        };
        match rx.recv_timeout(left) {
            Ok(ask) => {
                count += ask.0.len();
                asks.push(ask);
            }
            Err(_) => break,
        }
    }
    asks
}

/// The asks of one round, after what the last left (`carried`): all their changes, whether
/// the feed is to be read, and the latest connection asked at.
fn merge(carried: Vec<ChangeRef>, asks: impl IntoIterator<Item = Asked>) -> Asked {
    asks.into_iter().fold(
        (carried, false, None),
        |(mut changes, read, at), (more, also, connection)| {
            changes.extend(more);
            (changes, read || also, connection.or(at))
        },
    )
}

/// The hard deletes of `changes`, one per file, at most [`MAX_HELD`].
fn keep(changes: Vec<ChangeRef>) -> Vec<ChangeRef> {
    let mut seen = std::collections::HashSet::new();
    let mut held: Vec<ChangeRef> = changes
        .into_iter()
        .filter(|c| c.deleted && seen.insert(c.file_id))
        .collect();
    if held.len() > MAX_HELD {
        tracing::warn!(
            "{} hard deletes can't be taken: those files stay until written to",
            held.len() - MAX_HELD
        );
        held.truncate(MAX_HELD);
    }
    held
}

/// One round of the puller: takes `changes` (`announced`), the hard deletes first and on
/// their own (a failure of the rest can't hold them back), reads the feed when asked or when
/// they leave something unknown (`read`), and wakes every folder when the read fails. Says
/// which hard deletes are still to take (taking failed, and they are in no feed). A read
/// brings every announced live change: the server announces a write once it is committed and
/// its cached answers cleared, and a row it holds back a moment comes with more rows to
/// follow, which the read reads on (`homebase-api-notes.md`, the change feed).
fn round(
    changes: Vec<ChangeRef>,
    mut pull: bool,
    mut announced: impl FnMut(&[ChangeRef]) -> Result<bool, String>,
    read: impl FnOnce() -> Result<bool, String>,
    wake_all: impl FnOnce(),
) -> Round {
    let mut round = Round::default();
    let (hard, live): (Vec<ChangeRef>, Vec<ChangeRef>) =
        changes.into_iter().partition(|c| c.deleted);
    for (changes, hard) in [(&hard, true), (&live, false)] {
        if changes.is_empty() {
            continue;
        }
        match announced(changes) {
            Ok(all) => pull |= !all,
            Err(e) => {
                tracing::info!("taking announced changes failed ({e}); reading them");
                if hard {
                    round.held = keep(changes.clone());
                }
                pull = true;
            }
        }
    }
    if !pull {
        return round;
    }
    match read() {
        Ok(read) => round.read = read,
        Err(e) => {
            tracing::info!("pulling the drive's changes failed ({e}); the folders' syncs will");
            wake_all();
        }
    }
    round
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Announcements are pulled once they stop for a second, or ten seconds after the first.
    #[test]
    fn announcements_wait_until_they_stop() {
        let t0 = Instant::now();
        let mut a = Announced::default();
        assert!(!a.due(t0), "nothing announced");
        assert_eq!(a.deadline(), None);
        a.note(t0);
        assert_eq!(a.deadline(), Some(t0 + ms(1000)));
        assert!(!a.due(t0 + ms(500)));
        a.note(t0 + ms(800));
        assert!(!a.due(t0 + ms(1500)), "still coming");
        assert!(a.due(t0 + ms(1800)), "quiet for a second");
        assert!(!a.due(t0 + ms(1900)), "taken");
        // A steady stream: pulled after ten seconds all the same.
        let t1 = t0 + Duration::from_secs(100);
        for i in 0..20 {
            a.note(t1 + ms(i * 500));
        }
        assert_eq!(
            a.deadline(),
            Some(t1 + ms(10_000)),
            "no later than ten seconds"
        );
        assert!(!a.due(t1 + ms(9_999)));
        assert!(a.due(t1 + ms(10_000)));
    }

    /// A ref of a file nobody has: live, or (`deleted`) deleted for good.
    fn a_ref(deleted: bool) -> ChangeRef {
        use hbsync_types::{RemoteFileId, VersionTag};
        let live = ChangeRef {
            file_id: RemoteFileId(uuid::Uuid::new_v4()),
            version: VersionTag(uuid::Uuid::new_v4()),
            deleted: false,
            parent: None,
            removed_at: None,
            changed_at: Some(1),
        };
        if deleted { live.tombstone() } else { live }
    }

    /// Notifications on a drive of their own (a file on it, pulled into the mirror); no
    /// announcement comes but the test's.
    /// The notifications, their puller, the drive, and the file on it.
    type Set = (Notifications, Puller, Arc<Drive>, ChangeRef);

    fn notifications(home: &Path) -> Result<Set, Box<dyn std::error::Error>> {
        use hbsync_remote::{NodeMeta, Remote};
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&home.join("state.db"), "a.example", remote.clone())?;
        let made = remote.create(
            &NodeMeta {
                node: hbsync_types::NodeId::new(),
                parent: hbsync_types::NodeId::ROOT,
                kind: hbsync_types::NodeKind::Folder,
                content: hbsync_types::schema::NodeContent::folder("Docs"),
                removed: false,
            },
            None,
        )?;
        drive.pull(None)?;
        let on_drive = ChangeRef::of(&remote.header(made.at.file_id)?);
        let puller = Puller::spawn(Arc::clone(&drive));
        let n = Notifications::new(puller.clone(), Instant::now());
        Ok((n, puller, drive, on_drive))
    }

    fn wait_for(what: &str, done: impl Fn() -> bool) -> Result<(), Box<dyn std::error::Error>> {
        let until = Instant::now() + Duration::from_secs(10);
        while !done() {
            if Instant::now() > until {
                return Err(format!("timed out waiting until {what}").into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    /// A hard delete announced is taken within the puller's window, also while paused (it writes
    /// only the account's DB): never held where a pause could lose it.
    #[test]
    fn a_hard_delete_is_taken_even_paused() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (mut n, puller, _drive, on_drive) = notifications(home.path())?;
        n.event(
            Event::Changed(Some(on_drive.tombstone())),
            Instant::now(),
            true,
        );
        wait_for("the puller took it", || puller.rounds() == 1)?;
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        assert!(db.read(|s| s.mirror_by_file(on_drive.file_id))?.is_none());
        assert!(n.waiting().0.is_empty(), "nothing gathered");
        Ok(())
    }

    /// A hard delete gathered when the account ends is taken before its puller ends: only a
    /// crash loses one.
    #[test]
    fn a_hard_delete_gathered_is_taken_when_the_account_ends()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (mut n, puller, _drive, on_drive) = notifications(home.path())?;
        n.event(
            Event::Changed(Some(on_drive.tombstone())),
            Instant::now(),
            false,
        );
        let began = Instant::now();
        n.finish();
        assert!(began.elapsed() < GATHER_WINDOW * 3);
        assert_eq!(puller.rounds(), 1, "taken before the end");
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        assert!(db.read(|s| s.mirror_by_file(on_drive.file_id))?.is_none());
        Ok(())
    }

    /// More live changes than are kept are read from the feed instead.
    #[test]
    fn too_many_live_changes_are_read_instead() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (mut n, ..) = notifications(home.path())?;
        let now = Instant::now();
        for _ in 0..MAX_LIVE_REFS {
            n.event(Event::Changed(Some(a_ref(false))), now, true);
        }
        assert_eq!(n.waiting().0.len(), MAX_LIVE_REFS);
        n.event(Event::Changed(Some(a_ref(false))), now, true);
        assert_eq!(n.waiting(), (&[][..], true), "read from the feed instead");
        n.event(Event::Changed(Some(a_ref(false))), now, true);
        assert!(
            n.waiting().0.is_empty(),
            "nothing more is kept: the feed is read"
        );
        Ok(())
    }

    /// A puller whose thread ended (a panic) starts one again: what is asked is never taken on
    /// the asker's thread (the account's loop).
    #[test]
    fn a_puller_whose_thread_ended_starts_one_again() -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (_, puller, _drive, on_drive) = notifications(home.path())?;
        puller.end_thread();
        puller.take(vec![on_drive.tombstone()], false);
        wait_for("a new thread took it", || puller.rounds() == 1)?;
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        assert!(db.read(|s| s.mirror_by_file(on_drive.file_id))?.is_none());
        Ok(())
    }

    /// The hard deletes kept while taking them fails: one per file, and only so many.
    #[test]
    fn hard_deletes_kept_are_one_per_file_and_bounded() {
        let gone = a_ref(true);
        let kept = keep(vec![gone, a_ref(false), gone, gone]);
        assert_eq!(kept, vec![gone]);
        let many: Vec<ChangeRef> = (0..MAX_HELD + 5).map(|_| a_ref(true)).collect();
        assert_eq!(keep(many).len(), MAX_HELD);
    }

    /// A drive and the fake remote it reads.
    fn fake_drive(
        home: &Path,
    ) -> Result<(Arc<Drive>, Arc<FakeRemote>), Box<dyn std::error::Error>> {
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig::default())));
        let drive = Drive::open(&home.join("state.db"), "a.example", remote.clone())?;
        Ok((drive, remote))
    }

    /// The drive's changes count as announced only once the read after connecting worked: not
    /// while it runs, nor after it failed; a lost connection leaves the account behind.
    #[test]
    fn announced_only_once_the_read_after_connecting_worked()
    -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let (drive, remote) = fake_drive(home.path())?;
        let puller = Puller::spawn(Arc::clone(&drive));
        let mut n = Notifications::new(puller.clone(), Instant::now());
        remote.faults().latency = Duration::from_millis(300);
        n.event(Event::Connected, Instant::now(), false);
        assert!(!drive.current(), "not before the read");
        wait_for("the read after connecting", || drive.current())?;
        n.event(Event::Lost("gone".into()), Instant::now(), false);
        assert!(!drive.current(), "lost: behind");
        {
            let mut faults = remote.faults();
            faults.latency = Duration::ZERO;
            faults.denied = true;
        }
        let rounds = puller.rounds();
        n.event(Event::Connected, Instant::now(), false);
        wait_for("the read after connecting", || puller.rounds() > rounds)?;
        assert!(!drive.current(), "its read failed");
        Ok(())
    }

    /// An announced change whose row the server holds back (written in its current
    /// millisecond) is in the mirror after one round: the page says more rows follow, and the
    /// read goes on (odin-core #1906). Nothing is read for again.
    #[test]
    fn an_announced_change_held_back_is_read_in_one_round() -> Result<(), Box<dyn std::error::Error>>
    {
        use hbsync_remote::Remote;
        let home = tempfile::tempdir()?;
        let remote = Arc::new(FakeRemote::new(FakeServer::new(ServerConfig {
            auto_tick: false,
            ..ServerConfig::default()
        })));
        let drive = Drive::open(&home.path().join("state.db"), "a.example", remote.clone())?;
        let puller = Puller::spawn(Arc::clone(&drive));
        let events = remote.subscribe(true);
        let other = FakeRemote::new(remote.server_handle());
        let made = other.create(
            &hbsync_remote::contract::folder("From b", hbsync_types::NodeId::ROOT),
            None,
        )?;
        let change = events.recv_timeout(Duration::from_secs(5))?;
        puller.take(vec![change], false);
        wait_for("a round", || puller.rounds() > 0)?;
        let mut db = hbsync_state::StateDb::open_account(&home.path().join("state.db"))?;
        assert!(
            db.read(|s| s.mirror_by_file(made.at.file_id))?.is_some(),
            "in the mirror"
        );
        Ok(())
    }

    /// Changes announced without saying which are read from the feed.
    #[test]
    fn an_unsaid_change_is_read() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_remote::Remote;
        let home = tempfile::tempdir()?;
        let (drive, remote) = fake_drive(home.path())?;
        let mut n = Notifications::new(Puller::spawn(Arc::clone(&drive)), Instant::now());
        n.event(Event::Connected, Instant::now(), false);
        wait_for("the read after connecting", || drive.current())?;
        let other = FakeRemote::new(remote.server_handle());
        let made = other.create(
            &hbsync_remote::contract::folder("From b", hbsync_types::NodeId::ROOT),
            None,
        )?;
        n.event(Event::Changed(None), Instant::now(), false);
        n.tick(Instant::now() + Duration::from_secs(5), false);
        let db = Mutex::new(hbsync_state::StateDb::open_account(
            &home.path().join("state.db"),
        )?);
        wait_for("read", || {
            lock(&db)
                .read(|s| s.mirror_by_file(made.at.file_id))
                .is_ok_and(|m| m.is_some())
        })?;
        Ok(())
    }

    /// A burst of announced changes (a notification each) is taken in rounds of
    /// [`GATHER_MAX`], not one write each; a lone one waits the window at most; a plain read
    /// doesn't wait.
    #[test]
    fn a_burst_is_taken_in_few_rounds() {
        let window = Duration::from_millis(20);
        let (tx, rx) = channel::<Asked>();
        for _ in 0..2000 {
            let _ = tx.send((vec![a_ref(false)], false, None));
        }
        let mut sizes = Vec::new();
        while let Ok(first) = rx.try_recv() {
            let (changes, read, _) = merge(Vec::new(), gathered(first, &rx, window));
            assert!(!read);
            sizes.push(changes.len());
        }
        assert_eq!(sizes, [GATHER_MAX; 4]);

        let _ = tx.send((vec![a_ref(true)], false, None));
        let began = Instant::now();
        let asks = rx.recv().map(|first| gathered(first, &rx, window));
        assert_eq!(asks.map(|a| a.len()), Ok(1));
        let took = began.elapsed();
        assert!(took >= window && took < window * 50, "{took:?}");

        let began = Instant::now();
        let asks = gathered((Vec::new(), true, None), &rx, Duration::from_secs(60));
        assert_eq!(asks.len(), 1);
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "a plain read goes at once"
        );
    }

    /// The hard deletes of a round are taken first, on their own: taking the live changes
    /// failing doesn't hold them back (they are read from the feed instead).
    #[test]
    fn hard_deletes_are_taken_before_live_changes() {
        let (gone, live) = (a_ref(true), a_ref(false));
        let calls = std::cell::RefCell::new(Vec::new());
        let reads = std::cell::Cell::new(0);
        let left = round(
            vec![live, gone],
            false,
            |c| {
                calls.borrow_mut().push(c.to_vec());
                match c.iter().all(|c| c.deleted) {
                    true => Ok(true),
                    false => Err("timed out".into()),
                }
            },
            || {
                reads.set(reads.get() + 1);
                Ok(true)
            },
            || {},
        );
        assert_eq!(*calls.borrow(), vec![vec![gone], vec![live]]);
        assert!(left.held.is_empty(), "the hard delete was taken");
        assert_eq!(reads.get(), 1, "the live change is read instead");
    }

    /// A read the identity refused wakes no folder (each would be refused alike) and reads as
    /// not done.
    #[test]
    fn a_refused_read_wakes_no_folder() {
        let woken = std::cell::Cell::new(false);
        let left = round(
            Vec::new(),
            true,
            |_| Ok(true),
            || Ok(false),
            || woken.set(true),
        );
        assert!(!woken.get());
        assert!(!left.read);
    }

    /// A take and a plain ask in one round: the changes are taken and the feed is read, once.
    #[test]
    fn a_take_merges_with_a_pull() {
        let (gone, live) = (a_ref(true), a_ref(false));
        let held = vec![gone];
        let asks = [(vec![live], false, Some(1)), (Vec::new(), true, None)];
        let (changes, read, connection) = merge(held, asks);
        assert_eq!(changes, vec![gone, live]);
        assert!(read);
        assert_eq!(connection, Some(1), "asked while connected");
        assert!(!merge(Vec::new(), [(vec![live], false, None)]).1);
    }

    /// Taking announced changes that fails keeps the hard deletes for the next round (they
    /// are in no feed) and reads the feed meanwhile; once taken they are gone.
    #[test]
    fn a_hard_delete_survives_a_failed_take() {
        let (gone, live) = (a_ref(true), a_ref(false));
        let reads = std::cell::Cell::new(0);
        let read = || {
            reads.set(reads.get() + 1);
            Ok(true)
        };
        let left = round(
            vec![gone, live],
            false,
            |_| Err("the DB is busy".into()),
            read,
            || {},
        );
        assert_eq!(left.held, vec![gone]);
        assert_eq!(reads.get(), 1, "the live change is read instead");
        let taken = std::cell::RefCell::new(Vec::new());
        let left = round(
            merge(left.held, []).0,
            false,
            |c| {
                taken.borrow_mut().extend_from_slice(c);
                Ok(true)
            },
            read,
            || {},
        );
        assert_eq!(left, Round::default());
        assert_eq!(*taken.borrow(), vec![gone], "taken the next round");
        assert_eq!(reads.get(), 1, "known: nothing to read");
    }
}
