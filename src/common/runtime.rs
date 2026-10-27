use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    time::Duration,
};

use bevy::{ecs::resource::Resource, tasks::available_parallelism};

use crate::driver::{
    Command, Driver, DriverHandle, DriverOptions, EndpointOptions, Event, IdAllocator,
    UdpSocket,
};

use super::router::Notice;

/// Machines with at least this many logical processors get a second network
/// thread by default.
const TWO_THREAD_MIN_CPUS: usize = 8;

/// Name given to the network threads unless one is configured.
const DEFAULT_THREAD_NAME: &str = "bevy-s2n-quic";

pub(crate) type ThreadStartHook = Arc<dyn Fn() + Send + Sync>;

/// Settings for building a [QuicRuntime].
#[derive(Default)]
pub(crate) struct RuntimeOptions {
    pub threads: Option<usize>,
    pub max_connections: Option<usize>,
    pub thread_name: Option<String>,
    pub on_thread_start: Option<ThreadStartHook>,
}

/// The background threads that run all QUIC network IO.
///
/// Each thread runs one event loop. Every server and client is assigned to
/// one thread, and its connections run on that thread too.
///
/// [QuicDriverPlugin][crate::driver_plugin::QuicDriverPlugin] inserts this
/// resource. Servers and clients are created with it.
#[derive(Resource)]
pub struct QuicRuntime {
    drivers: Vec<DriverSlot>,
    next_driver: AtomicUsize,
    endpoint_options: EndpointOptions,
    notices: Sender<Notice>,
    notice_rx: Mutex<Receiver<Notice>>,
}

struct DriverSlot {
    driver: Driver,
    events: Mutex<Receiver<Event>>,
}

impl Default for QuicRuntime {
    fn default() -> Self {
        Self::with_options(RuntimeOptions::default())
    }
}

impl QuicRuntime {
    /// Creates a runtime with `threads` network threads.
    pub fn new(threads: usize) -> Self {
        Self::with_options(RuntimeOptions {
            threads: Some(threads),
            ..Default::default()
        })
    }

    /// The number of network threads used when none is configured: 2 on
    /// machines with at least 8 logical processors, otherwise 1.
    ///
    /// Useful for sizing other thread pools (such as Bevy's) around the runtime.
    pub fn default_thread_count() -> usize {
        if available_parallelism() >= TWO_THREAD_MIN_CPUS {
            2
        } else {
            1
        }
    }

    /// The number of network threads.
    pub fn thread_count(&self) -> usize {
        self.drivers.len()
    }

    pub(crate) fn with_options(options: RuntimeOptions) -> Self {
        let threads = options
            .threads
            .unwrap_or_else(Self::default_thread_count)
            .max(1);
        let thread_name = options
            .thread_name
            .unwrap_or_else(|| DEFAULT_THREAD_NAME.to_owned());
        let ids = Arc::new(IdAllocator::default());

        let drivers = (0..threads)
            .map(|_| {
                let (driver, events) = Driver::spawn(DriverOptions {
                    thread_name: Some(thread_name.clone()),
                    on_thread_start: options.on_thread_start.clone(),
                    ids: Some(ids.clone()),
                })
                .expect("Unable to start the QUIC network thread.");

                DriverSlot {
                    driver,
                    events: Mutex::new(events),
                }
            })
            .collect();

        let endpoint_options = EndpointOptions {
            max_connections: options.max_connections.unwrap_or(usize::MAX),
            ..Default::default()
        };
        let (notices, notice_rx) = mpsc::channel();

        Self {
            drivers,
            next_driver: AtomicUsize::new(0),
            endpoint_options,
            notices,
            notice_rx: Mutex::new(notice_rx),
        }
    }

    /// Picks the driver for a new endpoint, round robin.
    pub(crate) fn pick_driver(&self) -> usize {
        self.next_driver.fetch_add(1, Ordering::Relaxed) % self.drivers.len()
    }

    pub(crate) fn link(&self, driver: usize) -> Link {
        Link {
            driver: self.drivers[driver].driver.handle().clone(),
            notices: self.notices.clone(),
        }
    }

    pub(crate) fn endpoint_options(&self) -> &EndpointOptions {
        &self.endpoint_options
    }

    pub(crate) fn notice_receiver(&self) -> &Mutex<Receiver<Notice>> {
        &self.notice_rx
    }

    /// Each driver's event receiver, by driver index.
    pub(crate) fn event_receivers(
        &self,
    ) -> impl Iterator<Item = &Mutex<Receiver<Event>>> {
        self.drivers.iter().map(|slot| &slot.events)
    }

    /// Wakes every driver that has commands waiting.
    pub(crate) fn flush(&self) {
        for slot in &self.drivers {
            slot.driver.handle().flush();
        }
    }

    pub(crate) fn metrics(&self) -> impl Iterator<Item = (u64, Duration)> + '_ {
        self.drivers.iter().map(|slot| {
            let metrics = slot.driver.handle().metrics();
            (
                metrics.wakes.load(Ordering::Relaxed),
                Duration::from_nanos(metrics.busy_nanos.load(Ordering::Relaxed)),
            )
        })
    }
}

/// How a connection or stream object reaches its driver, and tells the
/// router when it's dropped.
#[derive(Clone)]
pub(crate) struct Link {
    pub driver: DriverHandle,
    notices: Sender<Notice>,
}

impl Link {
    /// Queues a command for the driver. It's sent when the frame ends.
    pub fn send(&self, command: Command<UdpSocket>) {
        self.driver.send(command);
    }

    pub fn notify(&self, notice: Notice) {
        // The router is gone only when the app is shutting down.
        let _ = self.notices.send(notice);
    }
}
