use std::{
    net::SocketAddr,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
        mpsc::Receiver,
    },
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;

use super::{
    Command, ConnId, Dir, Driver, DriverHandle, DriverOptions, EndpointId, EndpointOptions,
    EndpointSetup, Event, RecvBudget, SendBudget, StreamId,
    sim::{LinkConfig, Sim},
};
use crate::{
    client::Connect,
    common::error::{AppErrorCode, ConnectionError},
    config::{QuicClientConfig, QuicServerConfig},
};

const SERVER: usize = 0;
const CLIENT: usize = 1;

fn server_addr() -> SocketAddr {
    "10.0.0.1:4433".parse().unwrap()
}

fn client_addr(n: u16) -> SocketAddr {
    SocketAddr::new("10.0.0.2".parse().unwrap(), 5000 + n)
}

static SERVER_CONFIG: LazyLock<QuicServerConfig> = LazyLock::new(|| {
    let certs = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/certs");
    QuicServerConfig::from_pem_files(format!("{certs}/cert.pem"), format!("{certs}/key.pem"))
        .unwrap()
});

static CLIENT_CONFIG: LazyLock<QuicClientConfig> = LazyLock::new(|| {
    let cert = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/certs/cert.pem");
    QuicClientConfig::new().with_root_pem_file(cert).unwrap()
});

fn seeded(seed: u8) -> EndpointOptions {
    EndpointOptions {
        rng_seed: Some([seed; 32]),
        ..EndpointOptions::default()
    }
}

fn server_setup(options: &EndpointOptions) -> EndpointSetup {
    EndpointSetup::server(&SERVER_CONFIG, options).unwrap()
}

fn client_setup(options: &EndpointOptions) -> EndpointSetup {
    EndpointSetup::client(&CLIENT_CONFIG, options).unwrap()
}

/// A simulated server driver and client driver, each with one endpoint.
struct Pair {
    sim: Sim,
    server: EndpointId,
    client: EndpointId,
}

impl Pair {
    fn new(server: EndpointOptions, client: EndpointOptions) -> Self {
        let mut sim = Sim::new(7);
        assert_eq!(sim.add_driver(), SERVER);
        assert_eq!(sim.add_driver(), CLIENT);
        let server = sim.open_endpoint(SERVER, server_addr(), server_setup(&server));
        let client = sim.open_endpoint(CLIENT, client_addr(0), client_setup(&client));
        Self {
            sim,
            server,
            client,
        }
    }

    fn connect(&mut self) -> ConnId {
        connect(&mut self.sim, self.client)
    }

    /// Connects and waits for both sides. Returns `(client conn, server conn)`.
    fn connected(&mut self) -> (ConnId, ConnId) {
        let client = self.connect();
        let done = self.sim.run_until(Duration::from_secs(5), |sim| {
            has(&sim.drivers[CLIENT].log, |e| {
                matches!(e, Event::Connected { conn, .. } if *conn == client)
            }) && accepted(&sim.drivers[SERVER].log)
                .is_some_and(|server| has(&sim.drivers[SERVER].log, |e| {
                    matches!(e, Event::Connected { conn, .. } if *conn == server)
                }))
        });
        assert!(done, "handshake didn't complete");
        (client, accepted(&self.sim.drivers[SERVER].log).unwrap())
    }
}

fn connect(sim: &mut Sim, endpoint: EndpointId) -> ConnId {
    let conn = sim.drivers[CLIENT].ids.conn();
    sim.command(
        CLIENT,
        Command::Connect {
            endpoint,
            conn,
            connect: Connect::new(server_addr()).with_server_name("localhost"),
        },
    );
    conn
}

fn has(log: &[Event], f: impl Fn(&Event) -> bool) -> bool {
    log.iter().any(f)
}

fn accepted(log: &[Event]) -> Option<ConnId> {
    log.iter().find_map(|e| match e {
        Event::ConnectionAccepted { conn, .. } => Some(*conn),
        _ => None,
    })
}

fn lost_reason(log: &[Event], conn: ConnId) -> Option<ConnectionError> {
    log.iter().find_map(|e| match e {
        Event::ConnectionLost { conn: c, reason } if *c == conn => Some(reason.clone()),
        _ => None,
    })
}

fn open_stream(sim: &mut Sim, driver: usize, conn: ConnId, dir: Dir) -> super::StreamInfo {
    static NEXT_REQUEST: AtomicU64 = AtomicU64::new(0);
    let request = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
    sim.command(driver, Command::OpenStream { conn, dir, request });
    let mut info = None;
    sim.run_until(Duration::from_secs(1), |sim| {
        info = sim.drivers[driver].log.iter().find_map(|e| match e {
            Event::StreamOpened {
                conn: c,
                request: r,
                stream,
            } if *c == conn && *r == request => Some(stream.clone()),
            _ => None,
        });
        info.is_some()
    });
    info.expect("stream didn't open")
}

/// The byte at `offset` of a test stream. Not periodic in any chunk size used.
fn pattern(offset: usize, len: usize) -> Bytes {
    (offset..offset + len)
        .map(|i| (i.wrapping_mul(31) ^ (i >> 9)) as u8)
        .collect()
}

/// Writes `total` bytes of [`pattern`] as fast as the send budget allows,
/// then finishes the stream.
struct Writer {
    driver: usize,
    conn: ConnId,
    stream: StreamId,
    budget: Arc<SendBudget>,
    total: usize,
    chunk: usize,
    offset: usize,
    finished: bool,
}

impl Writer {
    fn pump(&mut self, sim: &mut Sim) {
        while self.offset < self.total {
            let len = self.chunk.min(self.total - self.offset);
            if !self.budget.try_reserve(len) {
                return;
            }
            sim.command(
                self.driver,
                Command::Write {
                    conn: self.conn,
                    stream: self.stream,
                    data: pattern(self.offset, len),
                },
            );
            self.offset += len;
        }

        if !self.finished {
            self.finished = true;
            sim.command(
                self.driver,
                Command::Finish {
                    conn: self.conn,
                    stream: self.stream,
                },
            );
        }
    }
}

/// Collects the data of the first stream the peer opens.
#[derive(Default)]
struct Reader {
    cursor: usize,
    stream: Option<(ConnId, StreamId, Arc<RecvBudget>)>,
    data: Vec<u8>,
    /// Delivered bytes not yet handed back to the budget.
    held: usize,
    finished: bool,
    reset: Option<AppErrorCode>,
}

impl Reader {
    /// Handles new events. Consumes what arrived if `consume` is set.
    fn pump(&mut self, sim: &mut Sim, driver: usize, consume: bool) {
        let events = sim.drivers[driver].log[self.cursor..].to_vec();
        self.cursor += events.len();

        for event in events {
            match event {
                Event::StreamAccepted { conn, stream } if self.stream.is_none() => {
                    self.stream = Some((conn, stream.id, stream.recv.unwrap()));
                }
                Event::StreamData { chunks, .. } => {
                    for chunk in chunks {
                        self.held += chunk.len();
                        self.data.extend_from_slice(&chunk);
                    }
                }
                Event::StreamFinished { .. } => self.finished = true,
                Event::StreamReset { code, .. } => self.reset = Some(code),
                _ => {}
            }
        }

        if consume {
            self.consume(sim, driver);
        }
    }

    fn consume(&mut self, sim: &mut Sim, driver: usize) {
        let Some((conn, stream, budget)) = &self.stream else {
            return;
        };
        if self.held > 0 && budget.consume(std::mem::take(&mut self.held)) {
            sim.command(
                driver,
                Command::ResumeRead {
                    conn: *conn,
                    stream: *stream,
                },
            );
        }
    }
}

#[test]
fn handshake_completes() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, server) = pair.connected();

    let log = &pair.sim.drivers[SERVER].log;
    let remote = log.iter().find_map(|e| match e {
        Event::ConnectionAccepted {
            endpoint,
            conn,
            remote,
        } if *conn == server => Some((*endpoint, *remote)),
        _ => None,
    });
    assert_eq!(remote, Some((pair.server, client_addr(0))));
    assert!(lost_reason(&pair.sim.drivers[CLIENT].log, client).is_none());
}

#[test]
fn connecting_without_a_server_times_out() {
    let options = EndpointOptions {
        idle_timeout: Some(Duration::from_secs(3)),
        ..seeded(2)
    };
    let mut sim = Sim::new(1);
    sim.add_driver();
    sim.add_driver();
    let client = sim.open_endpoint(CLIENT, client_addr(0), client_setup(&options));
    let conn = connect(&mut sim, client);

    assert!(sim.run_until(Duration::from_secs(10), |sim| {
        lost_reason(&sim.drivers[CLIENT].log, conn).is_some()
    }));
    assert_eq!(
        lost_reason(&sim.drivers[CLIENT].log, conn),
        Some(ConnectionError::TimedOut)
    );
}

#[test]
fn stream_delivers_in_order_under_loss_and_reordering() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, _) = pair.connected();
    pair.sim.set_link(LinkConfig {
        latency: Duration::from_millis(20),
        jitter: Duration::from_millis(15),
        loss: 0.1,
    });

    const TOTAL: usize = 2 << 20;
    let stream = open_stream(&mut pair.sim, CLIENT, client, Dir::Uni);
    assert!(stream.recv.is_none());
    let mut writer = Writer {
        driver: CLIENT,
        conn: client,
        stream: stream.id,
        budget: stream.send.unwrap(),
        total: TOTAL,
        chunk: 10_000,
        offset: 0,
        finished: false,
    };
    let mut reader = Reader::default();

    let done = pair.sim.run_until(Duration::from_secs(120), |sim| {
        writer.pump(sim);
        reader.pump(sim, SERVER, true);
        reader.finished
            && has(&sim.drivers[CLIENT].log, |e| {
                matches!(e, Event::SendFinished { stream, .. } if *stream == writer.stream)
            })
    });

    assert!(done, "received {} of {TOTAL} bytes", reader.data.len());
    assert!(reader.data == pattern(0, TOTAL), "stream data was corrupted");
    let (sent, dropped) = pair.sim.packet_counts();
    assert!(dropped > sent / 20, "only {dropped} of {sent} packets were lost");
    assert_eq!(writer.budget.queued(), 0);
}

#[test]
fn receive_budget_applies_backpressure() {
    const BUDGET: usize = 64 << 10;
    const TOTAL: usize = 1 << 20;

    let server = EndpointOptions {
        recv_budget: BUDGET,
        ..seeded(1)
    };
    let mut pair = Pair::new(server, seeded(2));
    let (client, _) = pair.connected();

    let stream = open_stream(&mut pair.sim, CLIENT, client, Dir::Uni);
    let mut writer = Writer {
        driver: CLIENT,
        conn: client,
        stream: stream.id,
        budget: stream.send.unwrap(),
        total: TOTAL,
        chunk: 16 << 10,
        offset: 0,
        finished: false,
    };
    let mut reader = Reader::default();

    // Nothing is consumed, so delivery stops at the budget.
    pair.sim.run_until(Duration::from_secs(5), |sim| {
        writer.pump(sim);
        reader.pump(sim, SERVER, false);
        false
    });
    assert_eq!(reader.data.len(), BUDGET);
    assert!(!reader.finished);
    let (_, _, budget) = reader.stream.as_ref().unwrap();
    assert_eq!(budget.unread(), BUDGET);

    // Consuming resumes delivery.
    let done = pair.sim.run_until(Duration::from_secs(30), |sim| {
        writer.pump(sim);
        reader.pump(sim, SERVER, true);
        reader.finished
    });
    assert!(done, "received {} of {TOTAL} bytes", reader.data.len());
    assert!(reader.data == pattern(0, TOTAL));
}

#[test]
fn send_budget_is_released_as_quinn_accepts_data() {
    let client = EndpointOptions {
        send_budget: 4096,
        ..seeded(2)
    };
    let mut pair = Pair::new(seeded(1), client);
    let (client, _) = pair.connected();

    let stream = open_stream(&mut pair.sim, CLIENT, client, Dir::Bi);
    let budget = stream.send.unwrap();
    assert!(budget.try_reserve(4096));
    assert!(!budget.try_reserve(1));

    pair.sim.command(
        CLIENT,
        Command::Write {
            conn: client,
            stream: stream.id,
            data: pattern(0, 4096),
        },
    );
    pair.sim.step();
    assert_eq!(budget.queued(), 0);
    assert!(budget.try_reserve(4096));
}

fn received_datagrams(log: &[Event], conn: ConnId) -> Vec<Bytes> {
    log.iter()
        .filter_map(|e| match e {
            Event::Datagrams { conn: c, data } if *c == conn => Some(data.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// The datagram sizes from the connection's `Connected` and latest `Stats`
/// events, and its latest drop count.
fn datagram_state(log: &[Event], conn: ConnId) -> (Option<usize>, Option<usize>, u64) {
    let min = log.iter().find_map(|e| match e {
        Event::Connected {
            conn: c,
            min_datagram_size,
        } if *c == conn => Some(*min_datagram_size),
        _ => None,
    });
    let (max, dropped) = log
        .iter()
        .rev()
        .find_map(|e| match e {
            Event::Stats {
                conn: c,
                stats,
                max_datagram_size,
            } if *c == conn => Some((*max_datagram_size, stats.datagrams_dropped)),
            _ => None,
        })
        .unwrap_or_default();
    (min.flatten(), max, dropped)
}

/// A datagram of `len` bytes that starts with `index`.
fn numbered(index: u16, len: usize) -> Bytes {
    let mut data = vec![index as u8; len];
    data[..2].copy_from_slice(&index.to_be_bytes());
    Bytes::from(data)
}

fn index_of(datagram: &Bytes) -> u16 {
    u16::from_be_bytes([datagram[0], datagram[1]])
}

#[test]
fn datagrams_are_delivered() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, server) = pair.connected();

    let (min, max, _) = datagram_state(&pair.sim.drivers[CLIENT].log, client);
    let (min, max) = (min.unwrap(), max.unwrap());
    assert!(min > 1000 && min <= max, "min {min}, max {max}");

    pair.sim.command(
        CLIENT,
        Command::SendDatagrams {
            conn: client,
            data: (0..50).map(|i| numbered(i, 100 + i as usize)).collect(),
        },
    );
    assert!(pair.sim.run_until(Duration::from_secs(2), |sim| {
        received_datagrams(&sim.drivers[SERVER].log, server).len() == 50
    }));
    // Long enough for stats to be reported.
    pair.sim.run_for(Duration::from_millis(200));

    let mut datagrams = received_datagrams(&pair.sim.drivers[SERVER].log, server);
    datagrams.sort_by_key(index_of);
    for (i, data) in datagrams.iter().enumerate() {
        assert_eq!(data, &numbered(i as u16, 100 + i));
    }
    assert_eq!(datagram_state(&pair.sim.drivers[CLIENT].log, client).2, 0);
}

#[test]
fn datagrams_past_the_send_buffer_drop_the_oldest() {
    const BUFFER: usize = 16 << 10;
    const LEN: usize = 1000;
    const BURST: u16 = 100;

    let options = EndpointOptions {
        datagram_send_buffer: BUFFER,
        ..seeded(2)
    };
    let mut pair = Pair::new(seeded(1), options);
    let (client, server) = pair.connected();

    // Queued in one turn, so none are sent before the buffer overflows.
    pair.sim.command(
        CLIENT,
        Command::SendDatagrams {
            conn: client,
            data: (0..BURST).map(|i| numbered(i, LEN)).collect(),
        },
    );
    pair.sim.run_for(Duration::from_secs(1));

    let mut received: Vec<u16> = received_datagrams(&pair.sim.drivers[SERVER].log, server)
        .iter()
        .map(index_of)
        .collect();
    received.sort();
    let (_, _, dropped) = datagram_state(&pair.sim.drivers[CLIENT].log, client);
    assert!(dropped > 0 && !received.is_empty());
    assert_eq!(received.len() as u64 + dropped, BURST as u64);
    // The newest are the ones kept.
    let first = BURST - received.len() as u16;
    assert_eq!(received, (first..BURST).collect::<Vec<_>>());

    // Sending faster than congestion control allows, while earlier
    // datagrams are still going out, keeps the count exact.
    const TOTAL: u16 = 5000;
    let burst_dropped = dropped;
    // 50 KB every millisecond.
    for start in (BURST..TOTAL).step_by(50) {
        let end = (start + 50).min(TOTAL);
        pair.sim.command(
            CLIENT,
            Command::SendDatagrams {
                conn: client,
                data: (start..end).map(|i| numbered(i, LEN)).collect(),
            },
        );
        pair.sim.run_for(Duration::from_millis(1));
    }
    pair.sim.run_for(Duration::from_secs(1));

    let received = received_datagrams(&pair.sim.drivers[SERVER].log, server).len();
    let (_, _, dropped) = datagram_state(&pair.sim.drivers[CLIENT].log, client);
    assert!(dropped > burst_dropped, "nothing more was dropped");
    assert!(received > 15 + 100, "only {received} arrived");
    assert_eq!(received as u64 + dropped, TOTAL as u64);
}

#[test]
fn oversized_datagrams_are_dropped_and_counted() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, server) = pair.connected();
    let (_, max, _) = datagram_state(&pair.sim.drivers[CLIENT].log, client);
    let max = max.unwrap();

    pair.sim.command(
        CLIENT,
        Command::SendDatagrams {
            conn: client,
            data: vec![numbered(0, max), numbered(1, max + 1), numbered(2, 10)],
        },
    );
    pair.sim.run_for(Duration::from_millis(500));

    let mut received: Vec<u16> = received_datagrams(&pair.sim.drivers[SERVER].log, server)
        .iter()
        .map(index_of)
        .collect();
    received.sort();
    assert_eq!(received, [0, 2]);
    assert_eq!(datagram_state(&pair.sim.drivers[CLIENT].log, client).2, 1);
}

#[test]
fn close_notifies_the_peer_and_drains() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, server) = pair.connected();

    pair.sim.command(
        CLIENT,
        Command::Close {
            conn: client,
            code: AppErrorCode::from(42u32),
            reason: Bytes::from_static(b"bye"),
        },
    );
    assert!(pair.sim.run_until(Duration::from_secs(1), |sim| {
        lost_reason(&sim.drivers[SERVER].log, server).is_some()
    }));

    assert_eq!(
        lost_reason(&pair.sim.drivers[CLIENT].log, client),
        Some(ConnectionError::LocallyClosed)
    );
    assert_eq!(
        lost_reason(&pair.sim.drivers[SERVER].log, server),
        Some(ConnectionError::ApplicationClosed {
            code: AppErrorCode::from(42u32),
            reason: Bytes::from_static(b"bye"),
        })
    );

    // Both sides keep the connection until it has drained (3 PTOs).
    assert!(pair.sim.run_until(Duration::from_secs(5), |sim| {
        sim.drivers.iter().all(|d| d.core.connection_count() == 0)
    }));

    // Only one ConnectionLost per side.
    for driver in &pair.sim.drivers {
        let lost = driver
            .log
            .iter()
            .filter(|e| matches!(e, Event::ConnectionLost { .. }))
            .count();
        assert_eq!(lost, 1);
    }
}

#[test]
fn closing_an_endpoint_closes_its_connections() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, server) = pair.connected();

    let endpoint = pair.server;
    pair.sim
        .command(SERVER, Command::CloseEndpoint { endpoint });
    assert!(pair.sim.run_until(Duration::from_secs(5), |sim| {
        has(&sim.drivers[SERVER].log, |e| {
            matches!(e, Event::EndpointClosed { endpoint: id } if *id == endpoint)
        })
    }));

    assert_eq!(
        lost_reason(&pair.sim.drivers[SERVER].log, server),
        Some(ConnectionError::LocallyClosed)
    );
    assert!(matches!(
        lost_reason(&pair.sim.drivers[CLIENT].log, client),
        Some(ConnectionError::ApplicationClosed { .. })
    ));
    assert!(pair.sim.drivers[SERVER].core.endpoint_ids().is_empty());
}

#[test]
fn idle_connections_time_out() {
    let options = |seed| EndpointOptions {
        idle_timeout: Some(Duration::from_secs(2)),
        keep_alive_interval: None,
        ..seeded(seed)
    };
    let mut pair = Pair::new(options(1), options(2));
    let (client, server) = pair.connected();
    let connected_at = pair.sim.elapsed();

    assert!(pair.sim.run_until(Duration::from_secs(10), |sim| {
        lost_reason(&sim.drivers[CLIENT].log, client).is_some()
            && lost_reason(&sim.drivers[SERVER].log, server).is_some()
    }));

    assert!(pair.sim.elapsed() - connected_at >= Duration::from_secs(2));
    assert_eq!(
        lost_reason(&pair.sim.drivers[CLIENT].log, client),
        Some(ConnectionError::TimedOut)
    );
    assert_eq!(
        lost_reason(&pair.sim.drivers[SERVER].log, server),
        Some(ConnectionError::TimedOut)
    );
    assert!(
        pair.sim
            .drivers
            .iter()
            .all(|d| d.core.connection_count() == 0)
    );
}

#[test]
fn keep_alive_prevents_idle_timeout() {
    let options = |seed| EndpointOptions {
        idle_timeout: Some(Duration::from_secs(2)),
        keep_alive_interval: Some(Duration::from_millis(500)),
        ..seeded(seed)
    };
    let mut pair = Pair::new(options(1), options(2));
    pair.connected();

    pair.sim.run_for(Duration::from_secs(20));
    for driver in &pair.sim.drivers {
        assert!(!has(&driver.log, |e| matches!(e, Event::ConnectionLost { .. })));
    }
}

#[test]
fn connections_past_the_limit_are_refused() {
    let server = EndpointOptions {
        max_connections: 1,
        ..seeded(1)
    };
    let mut pair = Pair::new(server, seeded(2));
    let (first, _) = pair.connected();

    let second_endpoint =
        pair.sim
            .open_endpoint(CLIENT, client_addr(1), client_setup(&seeded(3)));
    let second = connect(&mut pair.sim, second_endpoint);
    assert!(pair.sim.run_until(Duration::from_secs(5), |sim| {
        lost_reason(&sim.drivers[CLIENT].log, second).is_some()
    }));

    // CONNECTION_REFUSED is transport error 0x2.
    assert!(matches!(
        lost_reason(&pair.sim.drivers[CLIENT].log, second),
        Some(ConnectionError::ConnectionClosed { code: 2, .. })
    ));
    assert!(lost_reason(&pair.sim.drivers[CLIENT].log, first).is_none());
    let accepted = pair.sim.drivers[SERVER]
        .log
        .iter()
        .filter(|e| matches!(e, Event::ConnectionAccepted { .. }))
        .count();
    assert_eq!(accepted, 1);
}

#[test]
fn reset_and_stop_reach_the_peer() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, _) = pair.connected();

    // Reset: the client abandons a stream it wrote to.
    let stream = open_stream(&mut pair.sim, CLIENT, client, Dir::Uni);
    let budget = stream.send.unwrap();
    assert!(budget.try_reserve(1000));
    pair.sim.command(
        CLIENT,
        Command::Write {
            conn: client,
            stream: stream.id,
            data: pattern(0, 1000),
        },
    );
    pair.sim.command(
        CLIENT,
        Command::Reset {
            conn: client,
            stream: stream.id,
            code: AppErrorCode::from(9u32),
        },
    );
    let mut reader = Reader::default();
    assert!(pair.sim.run_until(Duration::from_secs(2), |sim| {
        reader.pump(sim, SERVER, true);
        reader.reset.is_some()
    }));
    assert_eq!(reader.reset, Some(AppErrorCode::from(9u32)));
    assert_eq!(budget.queued(), 0);

    // Stop: the server asks the client to stop sending on a second stream.
    let stream = open_stream(&mut pair.sim, CLIENT, client, Dir::Bi);
    let budget = stream.send.unwrap();
    assert!(budget.try_reserve(10));
    pair.sim.command(
        CLIENT,
        Command::Write {
            conn: client,
            stream: stream.id,
            data: pattern(0, 10),
        },
    );
    let mut server_stream = None;
    pair.sim.run_until(Duration::from_secs(2), |sim| {
        server_stream = sim.drivers[SERVER].log.iter().rev().find_map(|e| match e {
            Event::StreamAccepted { conn, stream: s } if s.id == stream.id => Some((*conn, s.id)),
            _ => None,
        });
        server_stream.is_some()
    });
    let (server_conn, id) = server_stream.expect("stream wasn't accepted");
    pair.sim.command(
        SERVER,
        Command::Stop {
            conn: server_conn,
            stream: id,
            code: AppErrorCode::from(11u32),
        },
    );
    assert!(pair.sim.run_until(Duration::from_secs(2), |sim| {
        has(&sim.drivers[CLIENT].log, |e| {
            matches!(e, Event::StreamStopped { stream, code, .. }
                if *stream == id && *code == AppErrorCode::from(11u32))
        })
    }));
}

#[test]
fn stream_opens_wait_for_the_peers_limit() {
    let mut pair = Pair::new(seeded(1), seeded(2));
    let (client, server) = pair.connected();

    // quinn's default limit is 100 concurrent uni streams.
    for request in 0..150 {
        pair.sim.command(
            CLIENT,
            Command::OpenStream {
                conn: client,
                dir: Dir::Uni,
                request,
            },
        );
    }
    pair.sim.run_for(Duration::from_millis(200));
    let opened = |log: &[Event]| -> Vec<(u64, super::StreamInfo)> {
        log.iter()
            .filter_map(|e| match e {
                Event::StreamOpened { request, stream, .. } => Some((*request, stream.clone())),
                _ => None,
            })
            .collect()
    };
    let first = opened(&pair.sim.drivers[CLIENT].log);
    assert_eq!(first.len(), 100);

    // Finishing streams lets the peer raise the limit once it has read them.
    for (_, stream) in &first {
        pair.sim.command(
            CLIENT,
            Command::Finish {
                conn: client,
                stream: stream.id,
            },
        );
    }
    assert!(pair.sim.run_until(Duration::from_secs(5), |sim| {
        opened(&sim.drivers[CLIENT].log).len() == 150
    }));

    let requests: Vec<u64> = opened(&pair.sim.drivers[CLIENT].log)
        .iter()
        .map(|(r, _)| *r)
        .collect();
    assert_eq!(requests, (0..150).collect::<Vec<_>>());
    assert!(lost_reason(&pair.sim.drivers[SERVER].log, server).is_none());
}

// ---- Real sockets ----

fn wait_for<T>(
    events: &Receiver<Event>,
    timeout: Duration,
    mut f: impl FnMut(Event) -> Option<T>,
) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let event = events.recv_timeout(left).expect("timed out waiting for an event");
        if let Some(value) = f(event) {
            return value;
        }
    }
}

struct Transfer {
    bytes: usize,
    elapsed: Duration,
}

/// Sends `total` bytes over one stream between two drivers on loopback,
/// checking every byte, and times it from the first write to the last byte.
fn loopback_transfer(total: usize, budget: usize) -> Transfer {
    const CHUNK: usize = 64 << 10;
    // Not a multiple of CHUNK, so chunks start at varying pattern offsets.
    const PERIOD: usize = 1_000_003;
    // Repeats every PERIOD bytes, so any slice of it starting at
    // `offset % PERIOD` holds the stream's bytes from `offset`.
    let period = pattern(0, PERIOD);
    let source: Bytes = period.iter().chain(&period[..CHUNK]).copied().collect();
    let expected = |source: &Bytes, offset: usize, len: usize| {
        source.slice(offset % PERIOD..offset % PERIOD + len)
    };

    let options = EndpointOptions {
        send_budget: budget,
        recv_budget: budget,
        ..EndpointOptions::default()
    };
    let spawn = |name: &str| {
        Driver::spawn(DriverOptions {
            thread_name: Some(name.to_owned()),
            ..DriverOptions::default()
        })
        .unwrap()
    };
    let (server, server_events) = spawn("quic-server");
    let (client, client_events) = spawn("quic-client");

    let (_, addr) = server
        .handle()
        .open_endpoint_new("127.0.0.1:0".parse().unwrap(), server_setup(&options))
        .unwrap();
    let (endpoint, _) = client
        .handle()
        .open_endpoint_new("127.0.0.1:0".parse().unwrap(), client_setup(&options))
        .unwrap();

    let handle = client.handle();
    let conn = handle.ids().conn();
    handle.send(Command::Connect {
        endpoint,
        conn,
        connect: Connect::new(addr).with_server_name("localhost"),
    });
    handle.send(Command::OpenStream {
        conn,
        dir: Dir::Uni,
        request: 1,
    });
    handle.flush();

    let stream = wait_for(&client_events, Duration::from_secs(5), |e| match e {
        Event::StreamOpened { stream, .. } => Some(stream),
        Event::ConnectionLost { reason, .. } => panic!("connection lost: {reason}"),
        _ => None,
    });

    let server_handle: DriverHandle = server.handle().clone();
    let receiver_source = source.clone();
    let receiver = thread::spawn(move || {
        let mut received = 0;
        let mut recv = None;
        loop {
            match server_events.recv_timeout(Duration::from_secs(30)).unwrap() {
                Event::StreamAccepted { conn, stream } => {
                    recv = Some((conn, stream.id, stream.recv.unwrap()));
                }
                Event::StreamData { chunks, .. } => {
                    let (conn, stream, budget) = recv.as_ref().unwrap();
                    let mut len = 0;
                    for chunk in chunks {
                        assert!(
                            chunk == expected(&receiver_source, received, chunk.len()),
                            "corrupt data"
                        );
                        received += chunk.len();
                        len += chunk.len();
                    }
                    if budget.consume(len) {
                        server_handle.send(Command::ResumeRead {
                            conn: *conn,
                            stream: *stream,
                        });
                        server_handle.flush();
                    }
                }
                Event::StreamFinished { .. } => return (received, Instant::now()),
                Event::ConnectionLost { reason, .. } => panic!("connection lost: {reason}"),
                _ => {}
            }
        }
    });

    let budget = stream.send.unwrap();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(60);
    let mut offset = 0;
    while offset < total {
        assert!(
            Instant::now() < deadline,
            "sent {offset} of {total} bytes before timing out"
        );
        let mut wrote = false;
        while offset < total {
            let len = CHUNK.min(total - offset);
            if !budget.try_reserve(len) {
                break;
            }
            handle.send(Command::Write {
                conn,
                stream: stream.id,
                data: expected(&source, offset, len),
            });
            offset += len;
            wrote = true;
        }
        handle.flush();
        if !wrote {
            thread::sleep(Duration::from_micros(100));
        }
    }
    handle.send(Command::Finish {
        conn,
        stream: stream.id,
    });
    handle.flush();

    let (bytes, end) = receiver.join().unwrap();
    Transfer {
        bytes,
        elapsed: end - start,
    }
}

#[test]
fn loopback_stream_transfer() {
    let total = 16 << 20;
    let transfer = loopback_transfer(total, 1 << 20);
    assert_eq!(transfer.bytes, total);
}

/// Bulk throughput on loopback, for comparison with quinn's `perf` tool:
///
/// ```sh
/// cargo test --release --lib driver::tests::bulk_throughput -- --ignored --nocapture
/// ```
#[test]
#[ignore = "benchmark"]
fn bulk_throughput() {
    let total = 2 << 30;
    let transfer = loopback_transfer(total, 16 << 20);
    assert_eq!(transfer.bytes, total);

    let secs = transfer.elapsed.as_secs_f64();
    println!(
        "{} MiB in {secs:.2} s: {:.1} MiB/s ({:.2} Gbit/s)",
        total >> 20,
        total as f64 / secs / (1 << 20) as f64,
        total as f64 * 8.0 / secs / 1e9,
    );
}
