//! The QUIC driver: one thread running a mio event loop over quinn-proto
//! endpoints and connections, controlled through a command channel and
//! reporting through an event channel.
//!
//! Commands are queued without waking the driver; [`DriverHandle::flush`]
//! wakes it at most once per call, so an owner that flushes once per frame
//! costs one wake per frame however many commands it sent.
//!
//! Drivers can form a [`DriverGroup`]. A server endpoint then keeps
//! receiving on its own driver, but hands each connection it accepts to the
//! least busy driver in the group, so one server's connections run on every
//! core.
//!
//! All quinn-proto and quinn-udp calls stay inside this module.

mod config;
mod core;
mod datagram;
mod io;
mod protocol;

#[cfg(test)]
mod sim;
#[cfg(test)]
mod tests;

use std::{
    io::ErrorKind,
    net::SocketAddr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use mio::{Events, Poll, Token, Waker};

use crate::{common::runtime::ThreadStartHook, config::ConfigError};

pub(crate) use self::io::UdpSocket;
pub(crate) use self::{
    config::{EndpointOptions, EndpointSetup},
    protocol::{
        Command, ConnId, Dir, EndpointId, Event, IdAllocator, RecvBudget, SendBudget,
        StreamId, StreamInfo,
    },
};
use self::{
    core::Core,
    io::{MioIo, Socket},
};

const WAKER: Token = Token(usize::MAX);

/// Settings for [`Driver::spawn`].
#[derive(Default)]
pub(crate) struct DriverOptions {
    pub thread_name: Option<String>,
    pub on_thread_start: Option<ThreadStartHook>,
    /// Shares endpoint and connection IDs with other drivers, so they're
    /// unique across all of them. Each driver has its own by default.
    pub ids: Option<Arc<IdAllocator>>,
    /// The group to join, and this driver's number in it. Drivers in a group
    /// must share `ids`. Each driver is in a group of its own by default.
    pub group: Option<(Arc<DriverGroup>, usize)>,
}

/// Drivers that may hand connections to each other.
///
/// Create it with the number of drivers, spawn each with its number, then
/// call [`Self::start`] with their handles. Until then, every driver keeps its
/// own connections.
pub(crate) struct DriverGroup {
    handles: OnceLock<Vec<DriverHandle>>,
    /// Each driver's connections, counting ones handed to it that it hasn't
    /// taken yet.
    loads: Box<[AtomicUsize]>,
}

impl DriverGroup {
    pub fn new(drivers: usize) -> Self {
        Self {
            handles: OnceLock::new(),
            loads: (0..drivers.max(1)).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    /// Hands the group its drivers, in order.
    pub fn start(&self, handles: Vec<DriverHandle>) {
        assert_eq!(handles.len(), self.loads.len(), "one handle per driver");
        let _ = self.handles.set(handles);
    }

    /// The number of drivers, or 1 if the group hasn't started.
    fn len(&self) -> usize {
        self.handles.get().map_or(1, Vec::len)
    }

    fn handle(&self, driver: usize) -> Option<&DriverHandle> {
        self.handles.get()?.get(driver)
    }

    fn load(&self, driver: usize) -> usize {
        self.loads.get(driver).map_or(0, |l| l.load(Ordering::Relaxed))
    }

    fn add_load(&self, driver: usize, delta: isize) {
        if let Some(load) = self.loads.get(driver) {
            if delta >= 0 {
                load.fetch_add(delta.unsigned_abs(), Ordering::Relaxed);
            } else {
                load.fetch_sub(delta.unsigned_abs(), Ordering::Relaxed);
            }
        }
    }
}

/// Counters for one driver, readable from any thread.
#[derive(Debug, Default)]
pub(crate) struct DriverMetrics {
    /// Times [`DriverHandle::flush`] woke the driver.
    pub wakes: AtomicU64,
    /// Times another driver in the group woke it.
    pub peer_wakes: AtomicU64,
    /// Nanoseconds spent handling work, not counting time waiting for it.
    pub busy_nanos: AtomicU64,
}

/// A running driver thread. Dropping it stops the thread without closing
/// connections gracefully.
pub(crate) struct Driver {
    handle: DriverHandle,
    thread: Option<JoinHandle<()>>,
}

impl Driver {
    /// Starts a driver thread. Returns it with the receiver for its events.
    pub fn spawn(options: DriverOptions) -> std::io::Result<(Self, Receiver<Event>)> {
        let poll = Poll::new()?;
        let waker = Waker::new(poll.registry(), WAKER)?;
        let registry = poll.registry().try_clone()?;
        let (group, index) = options
            .group
            .unwrap_or_else(|| (Arc::new(DriverGroup::new(1)), 0));
        let io = MioIo {
            registry: poll.registry().try_clone()?,
            group: group.clone(),
            index,
        };

        let (commands, command_rx) = mpsc::channel();
        let (event_tx, events) = mpsc::channel();
        let ids = options.ids.unwrap_or_default();
        let core = Core::new(io, command_rx, event_tx, ids.clone());
        let metrics = Arc::new(DriverMetrics::default());
        let thread_metrics = metrics.clone();

        let mut builder = thread::Builder::new();
        if let Some(name) = options.thread_name {
            builder = builder.name(name);
        }
        let on_thread_start = options.on_thread_start;
        let thread = builder.spawn(move || {
            if let Some(hook) = on_thread_start {
                hook();
            }
            run(poll, core, &thread_metrics);
        })?;

        let handle = DriverHandle {
            shared: Arc::new(Shared {
                commands,
                waker,
                registry,
                pending: AtomicBool::new(false),
                ids,
                metrics,
            }),
        };
        if group.loads.len() == 1 {
            group.start(vec![handle.clone()]);
        }

        Ok((
            Self {
                handle,
                thread: Some(thread),
            },
            events,
        ))
    }

    pub fn handle(&self) -> &DriverHandle {
        &self.handle
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.handle.send(Command::Shutdown);
        self.handle.flush();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Sends commands to a [`Driver`]. Cheap to clone.
#[derive(Clone)]
pub(crate) struct DriverHandle {
    shared: Arc<Shared>,
}

struct Shared {
    commands: Sender<Command<UdpSocket>>,
    waker: Waker,
    registry: mio::Registry,
    /// Commands were sent since the last wake.
    pending: AtomicBool,
    ids: Arc<IdAllocator>,
    metrics: Arc<DriverMetrics>,
}

impl DriverHandle {
    pub fn ids(&self) -> &IdAllocator {
        &self.shared.ids
    }

    pub fn metrics(&self) -> &DriverMetrics {
        &self.shared.metrics
    }

    /// [`Self::open_endpoint`] with a new ID.
    #[cfg(test)]
    pub fn open_endpoint_new(
        &self,
        bind: SocketAddr,
        setup: EndpointSetup,
    ) -> Result<(EndpointId, SocketAddr), ConfigError> {
        let endpoint = self.ids().endpoint();
        let local_addr = self.open_endpoint(endpoint, bind, setup)?;
        Ok((endpoint, local_addr))
    }

    /// Binds a UDP socket on this thread, so errors and the bound address
    /// are known right away, then hands it to the driver as `endpoint`, an
    /// ID from [`Self::ids`]. Returns the bound address.
    pub fn open_endpoint(
        &self,
        endpoint: EndpointId,
        bind: SocketAddr,
        setup: EndpointSetup,
    ) -> Result<SocketAddr, ConfigError> {
        let error = |e: std::io::Error| ConfigError::Endpoint {
            addr: bind,
            reason: e.to_string(),
        };

        let mut socket = UdpSocket::bind(bind).map_err(error)?;
        let local_addr = socket.local_addr().map_err(error)?;
        socket
            .register(&self.shared.registry, endpoint)
            .map_err(error)?;

        self.send(Command::OpenEndpoint {
            endpoint,
            socket,
            setup: Box::new(setup),
        });
        Ok(local_addr)
    }

    /// Queues a command without waking the driver. Call [`Self::flush`] to
    /// make sure it's handled promptly.
    pub fn send(&self, command: Command<UdpSocket>) {
        // A stopped driver has nothing left to do with the command.
        if self.shared.commands.send(command).is_ok() {
            self.shared.pending.store(true, Ordering::Release);
        }
    }

    /// Wakes the driver if commands were sent since the last flush.
    pub fn flush(&self) {
        if !self.shared.pending.swap(false, Ordering::AcqRel) {
            return;
        }

        self.shared.metrics.wakes.fetch_add(1, Ordering::Relaxed);
        self.wake();
    }

    /// [`Self::flush`] for another driver in the group, counted separately.
    fn wake_from_peer(&self) {
        if !self.shared.pending.swap(false, Ordering::AcqRel) {
            return;
        }

        self.shared.metrics.peer_wakes.fetch_add(1, Ordering::Relaxed);
        self.wake();
    }

    fn wake(&self) {
        if let Err(e) = self.shared.waker.wake() {
            tracing::warn!("can't wake the QUIC driver: {e}");
        }
    }
}

fn run(mut poll: Poll, mut core: Core<MioIo>, metrics: &DriverMetrics) {
    let mut events = Events::with_capacity(1024);

    loop {
        let timeout = if core.has_pending_work() {
            Some(Duration::ZERO)
        } else {
            core.next_timeout()
                .map(|at| at.saturating_duration_since(Instant::now()))
        };

        if let Err(e) = poll.poll(&mut events, timeout) {
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
            tracing::error!("the QUIC driver stopped: {e}");
            return;
        }

        for event in &events {
            if event.token() == WAKER {
                continue;
            }
            core.set_ready(
                EndpointId(event.token().0 as u64),
                event.is_readable() || event.is_error(),
                event.is_writable(),
            );
        }

        let start = Instant::now();
        let running = core.turn();
        metrics
            .busy_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if !running {
            return;
        }
    }
}
