//! A simulated network and fake clock for deterministic driver tests.
//!
//! Every driver core runs on the test thread. [`Sim::run_until`] turns each
//! core, then jumps the clock to the next timer or packet arrival, so tests
//! covering seconds of protocol time finish instantly.

use std::{
    cell::{Cell, RefCell},
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
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
    io::{Io, Socket, SocketSender},
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
    /// Sockets whose sends fail with `WouldBlock`.
    blocked: HashSet<SocketAddr>,
    would_block: u64,
    /// The address peers see for a socket, if it isn't its own, as if it
    /// were behind a NAT.
    nat: HashMap<SocketAddr, SocketAddr>,
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
            blocked: HashSet::new(),
            would_block: 0,
            nat: HashMap::new(),
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

#[derive(Clone)]
pub(super) struct SimSocket {
    addr: SocketAddr,
    net: SimNet,
}

impl SocketSender for SimSocket {
    fn max_transmit_segments(&self) -> usize {
        // What quinn-udp reports on Linux. Exercises the GSO path:
        // transmits carry several datagrams.
        64
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        // Linux fails larger GSO sends with EMSGSIZE, losing every datagram
        // in them.
        assert!(
            transmit.contents.len() <= 65_507,
            "a {}-byte send is larger than a UDP datagram",
            transmit.contents.len()
        );
        let mut state = self.net.0.borrow_mut();
        if state.blocked.contains(&self.addr) {
            state.would_block += 1;
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let from = state.nat.get(&self.addr).copied().unwrap_or(self.addr);
        let dest = transmit.destination;
        let to = match state.nat.iter().find(|(_, external)| **external == dest) {
            Some((internal, _)) => Some(*internal),
            // A rebound socket no longer gets packets sent to its own address.
            None if state.nat.contains_key(&dest) => None,
            None => Some(dest),
        };
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
                from,
                data: datagram.to_vec(),
            };
            // Packets to an address nobody listens on vanish.
            if let Some(inbox) = to.and_then(|to| state.inboxes.get_mut(&to)) {
                inbox.push(Reverse(packet));
            }
        }

        Ok(())
    }
}

impl Socket for SimSocket {
    type Sender = SimSocket;

    fn sender(&self) -> &SimSocket {
        self
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        false
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

/// Drivers that may hand connections to each other, all on the test thread.
struct SimGroup {
    commands: Vec<Sender<Command<SimSocket>>>,
    loads: RefCell<Vec<usize>>,
    /// Set whenever a driver sends another a command, so [`Sim::step`] turns
    /// them all again.
    sent: Rc<Cell<bool>>,
}

pub(super) struct SimIo {
    net: SimNet,
    group: Rc<SimGroup>,
    index: usize,
}

impl Io for SimIo {
    type Socket = SimSocket;

    fn now(&self) -> Instant {
        self.net.now()
    }

    // `Sim::step` reports every socket that isn't blocked as writable on
    // every step.
    fn set_writable_interest(&self, _: &mut SimSocket, _: EndpointId, _: bool) {}

    fn driver_index(&self) -> usize {
        self.index
    }

    fn driver_count(&self) -> usize {
        self.group.commands.len()
    }

    fn send_to(&self, driver: usize, command: Command<SimSocket>) {
        self.group.commands[driver].send(command).unwrap();
        self.group.sent.set(true);
    }

    fn wake(&self, _: usize) {}

    fn load(&self, driver: usize) -> usize {
        self.group.loads.borrow()[driver]
    }

    fn add_load(&self, driver: usize, delta: isize) {
        let mut loads = self.group.loads.borrow_mut();
        loads[driver] = loads[driver].checked_add_signed(delta).unwrap();
    }
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
    peer_sent: Rc<Cell<bool>>,
}

impl Sim {
    pub fn new(seed: u64) -> Self {
        let net = SimNet::new(seed);
        let start = net.now();
        Self {
            net,
            start,
            drivers: Vec::new(),
            peer_sent: Rc::default(),
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

    /// Makes sends on the socket at `addr` fail with `WouldBlock`, or stops
    /// them failing.
    pub fn set_blocked(&mut self, addr: SocketAddr, blocked: bool) {
        let mut state = self.net.0.borrow_mut();
        if blocked {
            state.blocked.insert(addr);
        } else {
            state.blocked.remove(&addr);
        }
    }

    /// Sends that failed with `WouldBlock` so far.
    pub fn would_block_count(&self) -> u64 {
        self.net.0.borrow().would_block
    }

    /// Makes peers see the socket at `addr` as `external`, as a NAT that
    /// rebinds it would. Packets to its previous address are lost.
    pub fn rebind(&mut self, addr: SocketAddr, external: SocketAddr) {
        self.net.0.borrow_mut().nat.insert(addr, external);
    }

    /// Simulated time since the start.
    pub fn elapsed(&self) -> Duration {
        self.net.now() - self.start
    }

    /// Adds a driver in a group of its own.
    pub fn add_driver(&mut self) -> usize {
        self.add_group(1)[0]
    }

    /// Adds `count` drivers that share IDs and may hand connections to each
    /// other. Returns their indices in [`Self::drivers`].
    pub fn add_group(&mut self, count: usize) -> Vec<usize> {
        let ids = Arc::new(IdAllocator::default());
        let (commands, receivers): (Vec<_>, Vec<_>) =
            (0..count).map(|_| mpsc::channel()).unzip();
        let group = Rc::new(SimGroup {
            commands: commands.clone(),
            loads: RefCell::new(vec![0; count]),
            sent: self.peer_sent.clone(),
        });

        let first = self.drivers.len();
        for (index, (commands, command_rx)) in commands.into_iter().zip(receivers).enumerate() {
            let (event_tx, events) = mpsc::channel();
            let io = SimIo {
                net: self.net.clone(),
                group: group.clone(),
                index,
            };
            self.drivers.push(SimDriver {
                core: Core::new(io, command_rx, event_tx, ids.clone()),
                commands,
                events,
                ids: ids.clone(),
                log: Vec::new(),
            });
        }
        (first..self.drivers.len()).collect()
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

    /// Turns every driver once, collecting their events, then again while
    /// drivers have sent each other commands.
    pub fn step(&mut self) {
        loop {
            self.peer_sent.set(false);
            for driver in &mut self.drivers {
                for (endpoint, addr) in driver.core.local_endpoints() {
                    let writable = !self.net.0.borrow().blocked.contains(&addr);
                    driver.core.set_ready(endpoint, true, writable);
                }
                driver.core.turn();
                driver.log.extend(driver.events.try_iter());
            }
            if !self.peer_sent.get() {
                return;
            }
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
