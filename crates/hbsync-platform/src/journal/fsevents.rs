//! macOS FSEvents history replay.
//!
//! Position format: `fsevents:<volume store UUID>:<event id>`. Catch-up opens a stream on the
//! sync root with `sinceWhen = event id`, collects the folder-level events until FSEvents sends
//! `HistoryDone`, and closes it. Folder-level (not per-file) events are what the rescan needs.
//!
//! The position is unusable when the volume's event store UUID changed (store wiped, volume
//! reformatted). Dropped events, "must scan subdirs", wrapped ids or a changed root ask for a
//! full rescan.

use std::ffi::{CStr, c_char, c_void};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use super::{ChangeJournal, JournalDelta, JournalError, Touched};

type CFRef = *const c_void;
type FSEventStreamRef = *mut c_void;

#[repr(C)]
struct FSEventStreamContext {
    version: isize,
    info: *mut c_void,
    retain: Option<extern "C" fn(*const c_void) -> *const c_void>,
    release: Option<extern "C" fn(*const c_void)>,
    copy_description: Option<extern "C" fn(*const c_void) -> CFRef>,
}

#[repr(C)]
struct CFArrayCallBacks {
    _opaque: [u8; 0],
}

type Callback = extern "C" fn(
    stream: *const c_void,
    info: *mut c_void,
    num_events: usize,
    paths: *mut c_void,
    flags: *const u32,
    ids: *const u64,
);

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    static kCFTypeArrayCallBacks: CFArrayCallBacks;
    fn CFStringCreateWithBytes(
        alloc: CFRef,
        bytes: *const u8,
        len: isize,
        encoding: u32,
        external: u8,
    ) -> CFRef;
    fn CFArrayCreate(
        alloc: CFRef,
        values: *const CFRef,
        count: isize,
        callbacks: *const CFArrayCallBacks,
    ) -> CFRef;
    fn CFRelease(cf: CFRef);
    fn CFUUIDCreateString(alloc: CFRef, uuid: CFRef) -> CFRef;
    fn CFStringGetCString(s: CFRef, buf: *mut c_char, size: isize, encoding: u32) -> u8;
}

#[link(name = "CoreServices", kind = "framework")]
unsafe extern "C" {
    fn FSEventStreamCreate(
        alloc: CFRef,
        callback: Callback,
        context: *const FSEventStreamContext,
        paths: CFRef,
        since_when: u64,
        latency: f64,
        flags: u32,
    ) -> FSEventStreamRef;
    fn FSEventStreamSetDispatchQueue(stream: FSEventStreamRef, queue: *mut c_void);
    fn FSEventStreamStart(stream: FSEventStreamRef) -> u8;
    fn FSEventStreamStop(stream: FSEventStreamRef);
    fn FSEventStreamInvalidate(stream: FSEventStreamRef);
    fn FSEventStreamRelease(stream: FSEventStreamRef);
    fn FSEventsGetCurrentEventId() -> u64;
    fn FSEventsCopyUUIDForDevice(dev: libc::dev_t) -> CFRef;
}

unsafe extern "C" {
    fn dispatch_queue_create(label: *const c_char, attr: *const c_void) -> *mut c_void;
    fn dispatch_release(object: *mut c_void);
}

const UTF8: u32 = 0x0800_0100;
const CREATE_NO_DEFER: u32 = 0x02;
const CREATE_WATCH_ROOT: u32 = 0x04;
const MUST_SCAN_SUBDIRS: u32 = 0x01;
const USER_DROPPED: u32 = 0x02;
const KERNEL_DROPPED: u32 = 0x04;
const IDS_WRAPPED: u32 = 0x08;
const HISTORY_DONE: u32 = 0x10;
const ROOT_CHANGED: u32 = 0x20;
const LOST_DETAIL: u32 =
    MUST_SCAN_SUBDIRS | USER_DROPPED | KERNEL_DROPPED | IDS_WRAPPED | ROOT_CHANGED;

/// How long a history replay may take before the position is treated as unusable.
const REPLAY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct FsEventsJournal {
    root: PathBuf,
    dev: libc::dev_t,
}

impl FsEventsJournal {
    pub(crate) fn open(root: &Path) -> io::Result<Self> {
        let root = root.canonicalize()?;
        let dev = crate::stat(&root)?.file_id.volume as libc::dev_t;
        let journal = FsEventsJournal { root, dev };
        journal
            .store_uuid()
            .ok_or_else(|| io::Error::other("volume has no FSEvents store"))?;
        Ok(journal)
    }

    fn store_uuid(&self) -> Option<String> {
        // SAFETY: CF objects created here are released here; the C string buffer is sized.
        unsafe {
            let uuid = FSEventsCopyUUIDForDevice(self.dev);
            if uuid.is_null() {
                return None;
            }
            let s = CFUUIDCreateString(std::ptr::null(), uuid);
            CFRelease(uuid);
            if s.is_null() {
                return None;
            }
            let mut buf = [0 as c_char; 64];
            let ok = CFStringGetCString(s, buf.as_mut_ptr(), buf.len() as isize, UTF8);
            CFRelease(s);
            (ok != 0).then(|| CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned())
        }
    }

    fn position(&self, uuid: &str, event_id: u64) -> String {
        format!("fsevents:{uuid}:{event_id}")
    }
}

impl ChangeJournal for FsEventsJournal {
    fn now(&mut self) -> Result<String, JournalError> {
        let uuid = self
            .store_uuid()
            .ok_or_else(|| JournalError::Unusable("no FSEvents store".into()))?;
        // SAFETY: no arguments.
        Ok(self.position(&uuid, unsafe { FSEventsGetCurrentEventId() }))
    }

    fn since(&mut self, cursor: &str) -> Result<JournalDelta, JournalError> {
        let bad = || JournalError::Unusable(format!("not an FSEvents position: {cursor:?}"));
        let rest = cursor.strip_prefix("fsevents:").ok_or_else(bad)?;
        let (uuid, id) = rest.rsplit_once(':').ok_or_else(bad)?;
        let since: u64 = id.parse().map_err(|_| bad())?;
        let current_uuid = self.store_uuid();
        if current_uuid.as_deref() != Some(uuid) {
            return Err(JournalError::Unusable("FSEvents store changed".into()));
        }
        let collected = replay(&self.root, since)?;
        // Advance only past events actually received: an event fseventsd hasn't delivered yet
        // is replayed next time instead of being skipped.
        let next = collected.max_id.max(since);
        Ok(JournalDelta {
            touched: collected.dirs.into_iter().map(Touched::Dir).collect(),
            rescan_all: collected.lost_detail,
            cursor: self.position(uuid, next),
        })
    }
}

#[derive(Debug, Default)]
struct Collected {
    dirs: Vec<PathBuf>,
    lost_detail: bool,
    done: bool,
    max_id: u64,
}

struct Shared {
    state: Mutex<Collected>,
    cv: Condvar,
}

extern "C" fn on_events(
    _stream: *const c_void,
    info: *mut c_void,
    num_events: usize,
    paths: *mut c_void,
    flags: *const u32,
    ids: *const u64,
) {
    // SAFETY: `info` is the `Shared` kept alive by `replay` until the stream is released;
    // without kFSEventStreamCreateFlagUseCFTypes, `paths` is `char**` with `num_events` items.
    let shared = unsafe { &*(info as *const Shared) };
    let paths = paths as *const *const c_char;
    let Ok(mut state) = shared.state.lock() else {
        return;
    };
    for i in 0..num_events {
        let (flag, id, path) =
            unsafe { (*flags.add(i), *ids.add(i), CStr::from_ptr(*paths.add(i))) };
        if flag & HISTORY_DONE != 0 {
            state.done = true;
            continue;
        }
        state.max_id = state.max_id.max(id);
        if flag & LOST_DETAIL != 0 {
            state.lost_detail = true;
        }
        use std::os::unix::ffi::OsStrExt;
        state
            .dirs
            .push(PathBuf::from(std::ffi::OsStr::from_bytes(path.to_bytes())));
    }
    if state.done {
        shared.cv.notify_all();
    }
}

fn replay(root: &Path, since: u64) -> Result<Collected, JournalError> {
    use std::os::unix::ffi::OsStrExt;
    let shared = Arc::new(Shared {
        state: Mutex::new(Collected::default()),
        cv: Condvar::new(),
    });
    let root_bytes = root.as_os_str().as_bytes();
    // SAFETY: every CF/dispatch object created here is released before returning; the stream is
    // stopped and invalidated before `shared` can be dropped.
    unsafe {
        let path = CFStringCreateWithBytes(
            std::ptr::null(),
            root_bytes.as_ptr(),
            root_bytes.len() as isize,
            UTF8,
            0,
        );
        let paths = CFArrayCreate(std::ptr::null(), &path, 1, &raw const kCFTypeArrayCallBacks);
        CFRelease(path);
        let context = FSEventStreamContext {
            version: 0,
            info: Arc::as_ptr(&shared) as *mut c_void,
            retain: None,
            release: None,
            copy_description: None,
        };
        let stream = FSEventStreamCreate(
            std::ptr::null(),
            on_events,
            &context,
            paths,
            since,
            0.0,
            CREATE_NO_DEFER | CREATE_WATCH_ROOT,
        );
        CFRelease(paths);
        if stream.is_null() {
            return Err(JournalError::Unusable("FSEventStreamCreate failed".into()));
        }
        let queue = dispatch_queue_create(c"id.homebase.sync.fsevents".as_ptr(), std::ptr::null());
        FSEventStreamSetDispatchQueue(stream, queue);
        let started = FSEventStreamStart(stream) != 0;
        let result = if started {
            let guard = shared
                .state
                .lock()
                .map_err(|_| io::Error::other("poisoned"))?;
            let (mut guard, timeout) = shared
                .cv
                .wait_timeout_while(guard, REPLAY_TIMEOUT, |s| !s.done)
                .map_err(|_| io::Error::other("poisoned"))?;
            if timeout.timed_out() {
                Err(JournalError::Unusable(
                    "FSEvents history replay timed out".into(),
                ))
            } else {
                Ok(std::mem::take(&mut *guard))
            }
        } else {
            Err(JournalError::Unusable("FSEventStreamStart failed".into()))
        };
        if started {
            FSEventStreamStop(stream);
        }
        FSEventStreamInvalidate(stream);
        FSEventStreamRelease(stream);
        dispatch_release(queue);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn replay_names_the_changed_folder() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        fs::create_dir(root.join("sub"))?;
        let mut journal = FsEventsJournal::open(&root)?;
        let cursor = journal.now()?;
        fs::write(root.join("sub").join("new.txt"), b"x")?;

        // fseventsd records events with a small delay; give it a few tries.
        let sub = root.join("sub");
        for attempt in 0..20 {
            let delta = journal.since(&cursor)?;
            let hit = delta.touched.iter().any(|t| match t {
                Touched::Dir(p) => p.components().eq(sub.components()),
                Touched::DirId(_) => false,
            });
            if hit {
                assert!(!delta.rescan_all);
                return Ok(());
            }
            eprintln!("attempt {attempt}: {:?}", delta.touched);
            std::thread::sleep(Duration::from_millis(500));
        }
        Err("FSEvents never reported the changed folder".into())
    }

    #[test]
    fn foreign_uuid_is_unusable() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut journal = FsEventsJournal::open(dir.path())?;
        let err = journal
            .since("fsevents:00000000-0000-0000-0000-000000000000:1")
            .expect_err("uuid");
        assert!(matches!(err, JournalError::Unusable(_)));
        Ok(())
    }
}
