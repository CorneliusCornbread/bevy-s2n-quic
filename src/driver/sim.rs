//! A simulated network and fake clock for deterministic driver tests.
//!
//! Every driver core runs on the test thread. [`Sim::run_until`] turns each
//! core, then jumps the clock to the next timer or packet arrival, so tests
//! covering seconds of protocol time finish instantly.

use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    io::{self, IoSliceMut},
    net::SocketAddr,
    rc::Rc,
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
    time::{Duration, Instant},
};

use quinn_udp::{RecvMeta, Transmit};

use super::{
    EndpointSetup,
    core::Core,
    io::{Io, Socket},
    protocol::{Command, EndpointId, Event, IdAllocator},
};

/// How packets travel between simulated sockets.
#[derive(Debug, Clone, Copy)]
pub(super) struct LinkConfig {
    /// One-way delay for every packet.
    pub latency: Duration,
    /// Extra random delay in `0..jitter`, which reorders packets.
    pub jitter: Duration,
    /// Fraction of packets dropped, `0.0..=1.0`.
    pub loss: f64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            latency: Duration::from_millis(10),
            jitter: Duration::ZERO,
            loss: 0.0,
        }
    }
}

#[derive(Clone)]
pub(super) struct SimNet(Rc<RefCell<NetState>>);

struct NetState {
    now: Instant,
    link: LinkConfig,
    rng: u64,
    seq: u64,
    inboxes: HashMap<SocketAddr, BinaryHeap<Reverse<Packet>>>,
    sent: u64,
    dropped: u64,
}

struct Packet {
    at: Instant,
    seq: u64,
    from: SocketAddr,
    data: Vec<u8>,
}

impl PartialEq for Packet {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for Packet {}

impl PartialOrd for Packet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Packet {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

impl NetState {
    /// xorshift64*: deterministic and good enough for loss and jitter.
    fn random(&mut self) -> f64 {
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        let bits = self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11;
        bits as f64 / (1u64 << 53) as f64
    }
}

impl SimNet {
    fn new(seed: u64) -> Self {
        Self(Rc::new(RefCell::new(NetState {
            now: Instant::now(),
            link: LinkConfig::default(),
            rng: seed.max(1),
            seq: 0,
            inboxes: HashMap::new(),
            sent: 0,
            dropped: 0,
        })))
    }

    fn socket(&self, addr: SocketAddr) -> SimSocket {
        self.0.borrow_mut().inboxes.entry(addr).or_default();
        SimSocket {
            addr,
            net: self.clone(),
        }
    }

    fn now(&self) -> Instant {
        self.0.borrow().now
    }

    fn next_arrival(&self) -> Option<Instant> {
        let state = self.0.borrow();
        state
            .inboxes
            .values()
            .filter_map(|inbox| inbox.peek().map(|Reverse(packet)| packet.at))
            .min()
    }
}

pub(super) struct SimSocket {
    addr: SocketAddr,
    net: SimNet,
}

impl Socket for SimSocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }

    fn max_transmit_segments(&self) -> usize {
        // Exercises the GSO path: transmits carry several datagrams.
        8
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        false
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        let mut state = self.net.0.borrow_mut();
        let segment = transmit.segment_size.unwrap_or(transmit.contents.len());

        for datagram in transmit.contents.chunks(segment.max(1)) {
            state.sent += 1;
            if state.random() < state.link.loss {
                state.dropped += 1;
                continue;
            }

            let jitter = state.link.jitter.mul_f64(state.random());
            let at = state.now + state.link.latency + jitter;
            state.seq += 1;
            let packet = Packet {
                at,
                seq: state.seq,
                from: self.addr,
                data: datagram.to_vec(),
            };
            // Packets to an address nobody listens on vanish.
            if let Some(inbox) = state.inboxes.get_mut(&transmit.destination) {
                inbox.push(Reverse(packet));
            }
        }

        Ok(())
    }

    fn recv(&self, bufs: &mut [IoSliceMut<'_>], meta: &mut [RecvMeta]) -> io::Result<usize> {
        let mut state = self.net.0.borrow_mut();
        let now = state.now;
        let inbox = state.inboxes.get_mut(&self.addr).expect("socket is bound");

        let mut count = 0;
        while count < bufs.len() {
            match inbox.peek() {
                Some(Reverse(packet)) if packet.at <= now => {}
                _ => break,
            }
            let Reverse(packet) = inbox.pop().unwrap();

            let len = packet.data.len().min(bufs[count].len());
            bufs[count][..len].copy_from_slice(&packet.data[..len]);
            let m = &mut meta[count];
            *m = RecvMeta::default();
            m.addr = packet.from;
            m.len = len;
            m.stride = len;
            m.dst_ip = Some(self.addr.ip());
            count += 1;
        }

        if count == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(count)
    }
}

pub(super) struct SimIo {
    net: SimNet,
}

impl Io for SimIo {
    type Socket = SimSocket;

    fn now(&self) -> Instant {
        self.net.now()
    }

    fn set_writable_interest(&self, _: &mut SimSocket, _: EndpointId, _: bool) {}
}

pub(super) struct SimDriver {
    pub core: Core<SimIo>,
    commands: Sender<Command<SimSocket>>,
    events: Receiver<Event>,
    pub ids: Arc<IdAllocator>,
    /// Every event the driver has emitted, oldest first.
    pub log: Vec<Event>,
}

pub(super) struct Sim {
    net: SimNet,
    start: Instant,
    pub drivers: Vec<SimDriver>,
}

impl Sim {
    pub fn new(seed: u64) -> Self {
        let net = SimNet::new(seed);
        let start = net.now();
        Self {
            net,
            start,
            drivers: Vec::new(),
        }
    }

    pub fn set_link(&mut self, link: LinkConfig) {
        self.net.0.borrow_mut().link = link;
    }

    /// Datagrams sent and dropped so far.
    pub fn packet_counts(&self) -> (u64, u64) {
        let state = self.net.0.borrow();
        (state.sent, state.dropped)
    }

    /// Simulated time since the start.
    pub fn elapsed(&self) -> Duration {
        self.net.now() - self.start
    }

    pub fn add_driver(&mut self) -> usize {
        let (commands, command_rx) = mpsc::channel();
        let (event_tx, events) = mpsc::channel();
        let ids = Arc::new(IdAllocator::default());
        let io = SimIo {
            net: self.net.clone(),
        };

        self.drivers.push(SimDriver {
            core: Core::new(io, command_rx, event_tx, ids.clone()),
            commands,
            events,
            ids,
            log: Vec::new(),
        });
        self.drivers.len() - 1
    }

    pub fn open_endpoint(
        &mut self,
        driver: usize,
        addr: SocketAddr,
        setup: EndpointSetup,
    ) -> EndpointId {
        let socket = self.net.socket(addr);
        let endpoint = self.drivers[driver].ids.endpoint();
        self.command(
            driver,
            Command::OpenEndpoint {
                endpoint,
                socket,
                setup: Box::new(setup),
            },
        );
        endpoint
    }

    pub fn command(&mut self, driver: usize, command: Command<SimSocket>) {
        self.drivers[driver].commands.send(command).unwrap();
    }

    /// Turns every driver once, collecting their events.
    pub fn step(&mut self) {
        for driver in &mut self.drivers {
            for endpoint in driver.core.endpoint_ids() {
                driver.core.set_ready(endpoint, true, false);
            }
            driver.core.turn();
            driver.log.extend(driver.events.try_iter());
        }
    }

    /// Steps until `done` returns `true`, jumping the clock forward whenever
    /// every driver is idle. Returns `false` if `limit` of simulated time
    /// passes first, or nothing is left to happen.
    pub fn run_until(&mut self, limit: Duration, mut done: impl FnMut(&mut Sim) -> bool) -> bool {
        let deadline = self.net.now() + limit;

        loop {
            self.step();
            if done(self) {
                return true;
            }

            let now = self.net.now();
            let busy = self.drivers.iter().any(|d| d.core.has_pending_work())
                || self.net.next_arrival().is_some_and(|at| at <= now);
            if busy {
                continue;
            }

            let next = self
                .drivers
                .iter()
                .filter_map(|d| d.core.next_timeout())
                .chain(self.net.next_arrival())
                .min();
            match next {
                Some(at) if at <= deadline => {
                    let mut state = self.net.0.borrow_mut();
                    state.now = state.now.max(at);
                }
                _ => {
                    self.net.0.borrow_mut().now = deadline;
                    self.step();
                    return done(self);
                }
            }
        }
    }

    /// Runs for `duration` of simulated time.
    pub fn run_for(&mut self, duration: Duration) {
        self.run_until(duration, |_| false);
    }
}
