//! The QUIC driver: one thread running a mio event loop over quinn-proto
//! endpoints and connections, controlled through a command channel and
//! reporting through an event channel.
//!
//! Commands are queued without waking the driver; [`DriverHandle::flush`]
//! wakes it at most once per call, so an owner that flushes once per frame
//! costs one wake per frame however many commands it sent.
//!
//! All quinn-proto and quinn-udp calls stay inside this module.

mod config;
mod core;
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
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use mio::{Events, Poll, Token, Waker};

use crate::{common::runtime::ThreadStartHook, config::ConfigError};

pub(crate) use self::{
    config::{EndpointOptions, EndpointSetup},
    protocol::{
        Command, ConnId, Dir, EndpointId, Event, IdAllocator, RecvBudget, SendBudget, StreamId,
        StreamInfo,
    },
};
use self::{
    core::Core,
    io::{MioIo, Socket, UdpSocket},
};

const WAKER: Token = Token(usize::MAX);

/// Settings for [`Driver::spawn`].
#[derive(Default)]
pub(crate) struct DriverOptions {
    pub thread_name: Option<String>,
    pub on_thread_start: Option<ThreadStartHook>,
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
        let io = MioIo {
            registry: poll.registry().try_clone()?,
        };

        let (commands, command_rx) = mpsc::channel();
        let (event_tx, events) = mpsc::channel();
        let ids = Arc::new(IdAllocator::default());
        let core = Core::new(io, command_rx, event_tx, ids.clone());

        let mut builder = thread::Builder::new();
        if let Some(name) = options.thread_name {
            builder = builder.name(name);
        }
        let on_thread_start = options.on_thread_start;
        let thread = builder.spawn(move || {
            if let Some(hook) = on_thread_start {
                hook();
            }
            run(poll, core);
        })?;

        let handle = DriverHandle {
            shared: Arc::new(Shared {
                commands,
                waker,
                registry,
                pending: AtomicBool::new(false),
                ids,
            }),
        };

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
}

impl DriverHandle {
    pub fn ids(&self) -> &IdAllocator {
        &self.shared.ids
    }

    /// Binds a UDP socket on this thread, so errors and the bound address
    /// are known right away, then hands it to the driver.
    pub fn open_endpoint(
        &self,
        bind: SocketAddr,
        setup: EndpointSetup,
    ) -> Result<(EndpointId, SocketAddr), ConfigError> {
        let error = |e: std::io::Error| ConfigError::Endpoint {
            addr: bind,
            reason: e.to_string(),
        };

        let mut socket = UdpSocket::bind(bind).map_err(error)?;
        let local_addr = socket.local_addr().map_err(error)?;
        let endpoint = self.ids().endpoint();
        socket
            .register(&self.shared.registry, endpoint)
            .map_err(error)?;

        self.send(Command::OpenEndpoint {
            endpoint,
            socket,
            setup: Box::new(setup),
        });
        Ok((endpoint, local_addr))
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
        if self.shared.pending.swap(false, Ordering::AcqRel)
            && let Err(e) = self.shared.waker.wake()
        {
            tracing::warn!("can't wake the QUIC driver: {e}");
        }
    }
}

fn run(mut poll: Poll, mut core: Core<MioIo>) {
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

        if !core.turn() {
            return;
        }
    }
}
