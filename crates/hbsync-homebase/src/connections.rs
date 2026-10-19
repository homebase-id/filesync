//! The connections of a [`Client`](crate::Client) and its clones: one ureq agent, made when a
//! call needs it and let go once no call has run for a while ([`REUSED_WITHIN`]).
//!
//! ureq keeps a connection that comes back to its pool, with its buffers and TLS state (about
//! 0.4 MB), until the next call, however long that takes; and it reuses none idle longer than
//! its `max_idle_age`, the same while. After a burst of transfers, the transfer workers'
//! connections would stay open for nothing until the next sync. Letting the agent go once its
//! calls are over for that long closes only connections that would never be reused. The agents
//! share one connector, whose TLS setup (the roots, session tickets) outlives them: the next
//! burst resumes a session instead of a full handshake.
//!
//! A thread of its own lets the agent go. It waits without a timeout while there is no agent,
//! and while calls run (the last one to end wakes it): no wake-up while idle, nor during a long
//! transfer.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use ureq::unversioned::transport::{ConnectionDetails, Connector, Transport};

/// How long a connection back in the pool may be reused (ureq's `max_idle_age`): once no call
/// has run for this long, the agent goes, and its connections with it.
pub(crate) const REUSED_WITHIN: Duration = Duration::from_secs(15);

/// The agent of a client and its clones, let go when unused.
pub(crate) struct Connections {
    shared: Arc<Shared>,
}

/// Makes an agent that reuses no connection idle longer than the given while.
type Make = Box<dyn Fn(Duration) -> ureq::Agent + Send + Sync>;

struct Shared {
    state: Mutex<State>,
    /// Rung for the janitor: an agent was made, the last call ended, or the connections went.
    ring: Condvar,
    make: Make,
    reused_within: Duration,
}

struct State {
    agent: Option<ureq::Agent>,
    /// Calls on their way ([`Call`]).
    calls: usize,
    /// When the last call ended.
    last: Instant,
    /// The janitor waits for the calls to end: the last one rings.
    awaiting_calls: bool,
    /// A janitor thread runs.
    watched: bool,
    dropped: bool,
}

impl Connections {
    /// Connections whose agents `make` makes, each let go `reused_within` after the last call.
    pub(crate) fn new(
        reused_within: Duration,
        make: impl Fn(Duration) -> ureq::Agent + Send + Sync + 'static,
    ) -> Connections {
        Connections {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    agent: None,
                    calls: 0,
                    last: Instant::now(),
                    awaiting_calls: false,
                    watched: false,
                    dropped: false,
                }),
                ring: Condvar::new(),
                make: Box::new(make),
                reused_within,
            }),
        }
    }

    /// The agent for one call, made if there is none. It stays while the [`Call`] lives (for a
    /// body read later, as long as the body).
    pub(crate) fn call(&self) -> Call {
        let shared = &self.shared;
        let mut state = shared.lock();
        let agent = match &state.agent {
            Some(agent) => agent.clone(),
            None => {
                let agent = (shared.make)(shared.reused_within);
                shared.ring.notify_all();
                state.agent.insert(agent).clone()
            }
        };
        state.calls += 1;
        if !state.watched {
            let watched = Arc::clone(shared);
            let spawned = std::thread::Builder::new()
                .name("hbsync-conns".into())
                .spawn(move || janitor(&watched));
            // Tried again at the next call; meanwhile the agent stays, as ureq has it.
            match spawned {
                Ok(_) => state.watched = true,
                Err(e) => tracing::warn!("nothing lets idle connections go: {e}"),
            }
        }
        Call {
            agent,
            shared: Arc::clone(shared),
        }
    }

    /// Whether an agent is kept (tests).
    #[cfg(test)]
    pub(crate) fn kept(&self) -> bool {
        self.shared.lock().agent.is_some()
    }
}

impl Drop for Connections {
    fn drop(&mut self) {
        self.shared.lock().dropped = true;
        self.shared.ring.notify_all();
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Lets the agent go once no call has run for `reused_within`; ends when the connections go.
fn janitor(shared: &Shared) {
    let mut state = shared.lock();
    loop {
        if state.dropped {
            break;
        }
        let idle = state.last.elapsed();
        let wait = match (&state.agent, state.calls) {
            (None, _) => None,
            (Some(_), 0) if idle >= shared.reused_within => {
                let agent = state.agent.take();
                // Closed outside the lock: a TLS close may write to the socket.
                drop(state);
                drop(agent);
                state = shared.lock();
                continue;
            }
            (Some(_), 0) => Some(shared.reused_within - idle),
            (Some(_), _) => {
                state.awaiting_calls = true;
                None
            }
        };
        state = match wait {
            Some(wait) => {
                let waited = shared.ring.wait_timeout(state, wait);
                waited.unwrap_or_else(|e| e.into_inner()).0
            }
            None => shared.ring.wait(state).unwrap_or_else(|e| e.into_inner()),
        };
    }
    // The connections went with the last client: what is left goes with this.
    let agent = state.agent.take();
    drop(state);
    drop(agent);
}

/// One call on its way: the agent to make it with, which stays (and the connection it uses)
/// while this lives. Its end counts as the agent's last use.
pub(crate) struct Call {
    agent: ureq::Agent,
    shared: Arc<Shared>,
}

impl std::ops::Deref for Call {
    type Target = ureq::Agent;

    fn deref(&self) -> &ureq::Agent {
        &self.agent
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.calls -= 1;
        state.last = Instant::now();
        if state.calls == 0 && std::mem::take(&mut state.awaiting_calls) {
            self.shared.ring.notify_all();
        }
    }
}

/// A connector shared by the agents of one [`Connections`]: what it keeps (rustls's config, with
/// its session tickets) outlives each agent.
#[derive(Debug)]
pub(crate) struct SharedConnector<C>(pub(crate) Arc<C>);

impl<In: Transport, C: Connector<In>> Connector<In> for SharedConnector<C> {
    type Out = C::Out;

    fn connect(
        &self,
        details: &ConnectionDetails,
        chained: Option<In>,
    ) -> Result<Option<C::Out>, ureq::Error> {
        self.0.connect(details, chained)
    }
}
