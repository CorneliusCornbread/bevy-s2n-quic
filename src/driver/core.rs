//! The driver's event loop state: endpoints, connections and timers. One
//! [`Core::turn`] runs one loop iteration; the thread in `mod.rs` (or a test)
//! decides when to call it.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    io::{self, IoSliceMut},
    mem,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        mpsc::{Receiver, Sender, TryRecvError},
    },
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use quinn_proto::{
    self as proto, ConnectionHandle, DatagramEvent, Incoming, ReadError, StreamEvent, VarInt,
    WriteError,
};
use quinn_udp::{BATCH_SIZE, RecvMeta};
use slab::Slab;

use crate::{
    client::Connect,
    common::error::{AppErrorCode, ConnectionError},
};

use super::{
    config::{EndpointSetup, Role, connection_error, from_varint, to_varint},
    io::{Io, Socket},
    protocol::{
        Command, ConnId, Dir, EndpointId, Event, IdAllocator, RecvBudget, SendBudget, StreamId,
        StreamInfo,
    },
};

/// Datagrams one endpoint may receive per turn before connections get a
/// chance to send. Acknowledgements only go out when connections are driven,
/// so draining a busy socket completely would stall the peer.
const MAX_RECV_DATAGRAMS: usize = 256;

/// Datagrams one connection may send per turn, so a bulk sender can't starve
/// the others.
const MAX_TRANSMIT_DATAGRAMS: usize = 64;

/// Endpoint-generated responses (version negotiation, stateless resets,
/// refusals) kept while a socket is blocked. Past this they're dropped; the
/// peer retries.
const MAX_QUEUED_RESPONSES: usize = 1024;

pub(crate) struct Core<I: Io> {
    io: I,
    commands: Receiver<Command<I::Socket>>,
    events: Sender<Event>,
    ids: Arc<IdAllocator>,
    endpoints: Slab<Endpoint<I::Socket>>,
    endpoint_keys: HashMap<EndpointId, usize>,
    /// Endpoints whose sockets may have datagrams waiting.
    readable: Vec<usize>,
    conns: Slab<Conn>,
    conn_keys: HashMap<ConnId, usize>,
    /// `(deadline, conn key, conn generation)`. An entry is stale, and
    /// skipped, unless the connection still exists and its `timer` matches.
    timers: BinaryHeap<Reverse<(Instant, usize, u64)>>,
    next_generation: u64,
    /// Connections to drive this turn.
    dirty: Vec<usize>,
    /// `BATCH_SIZE` receive buffers of `recv_stride` bytes each.
    recv_buf: Vec<u8>,
    recv_stride: usize,
    send_buf: Vec<u8>,
    running: bool,
}

struct Endpoint<S> {
    id: EndpointId,
    socket: S,
    inner: proto::Endpoint,
    setup: EndpointSetup,
    conns: HashMap<ConnectionHandle, usize>,
    readable: bool,
    /// Transmits that hit `WouldBlock`, oldest first. While any are queued
    /// the socket waits for writability and no connection sends.
    blocked: VecDeque<OwnedTransmit>,
    /// Connections that wanted to send while the socket was blocked.
    waiting: Vec<usize>,
    closing: bool,
}

struct Conn {
    id: ConnId,
    endpoint: usize,
    handle: ConnectionHandle,
    inner: proto::Connection,
    generation: u64,
    /// The deadline currently in the timer heap.
    timer: Option<Instant>,
    dirty: bool,
    /// `ConnectionLost` has been sent; nothing else will be.
    lost: bool,
    streams: HashMap<proto::StreamId, StreamState>,
    /// `OpenStream` requests waiting for the peer's stream limit, by [`Dir`].
    pending_opens: [VecDeque<u64>; 2],
    /// Streams to try reading on the next drive.
    pending_reads: Vec<proto::StreamId>,
    send_budget: usize,
    recv_budget: usize,
}

#[derive(Default)]
struct StreamState {
    send: Option<SendHalf>,
    recv: Option<Arc<RecvBudget>>,
}

struct SendHalf {
    budget: Arc<SendBudget>,
    queue: VecDeque<Bytes>,
    /// quinn returned `Blocked`; wait for `Writable`.
    blocked: bool,
    /// Finish once `queue` is empty.
    finish: bool,
}

struct OwnedTransmit {
    destination: SocketAddr,
    ecn: Option<quinn_udp::EcnCodepoint>,
    segment_size: Option<usize>,
    src_ip: Option<IpAddr>,
    contents: Vec<u8>,
}

impl OwnedTransmit {
    fn as_udp(&self) -> quinn_udp::Transmit<'_> {
        quinn_udp::Transmit {
            destination: self.destination,
            ecn: self.ecn,
            contents: &self.contents,
            segment_size: self.segment_size,
            src_ip: self.src_ip,
        }
    }
}

impl<I: Io> Core<I> {
    pub fn new(
        io: I,
        commands: Receiver<Command<I::Socket>>,
        events: Sender<Event>,
        ids: Arc<IdAllocator>,
    ) -> Self {
        Self {
            io,
            commands,
            events,
            ids,
            endpoints: Slab::new(),
            endpoint_keys: HashMap::new(),
            readable: Vec::new(),
            conns: Slab::new(),
            conn_keys: HashMap::new(),
            timers: BinaryHeap::new(),
            next_generation: 0,
            dirty: Vec::new(),
            recv_buf: Vec::new(),
            recv_stride: 0,
            send_buf: Vec::new(),
            running: true,
        }
    }

    /// Runs one loop iteration. Returns `false` once the driver should stop.
    pub fn turn(&mut self) -> bool {
        self.drain_commands();
        if !self.running {
            return false;
        }

        self.receive();
        self.expire_timers();
        self.drive_dirty();
        self.reap_endpoints();
        true
    }

    /// Whether [`Self::turn`] has work to do right away, without waiting.
    pub fn has_pending_work(&self) -> bool {
        !self.dirty.is_empty() || !self.readable.is_empty()
    }

    /// When the earliest timer fires. May be earlier than needed; a turn
    /// then finds nothing to do.
    pub fn next_timeout(&self) -> Option<Instant> {
        self.timers.peek().map(|Reverse((at, _, _))| *at)
    }

    /// Records readiness reported for an endpoint's socket.
    pub fn set_ready(&mut self, endpoint: EndpointId, readable: bool, writable: bool) {
        let Some(&key) = self.endpoint_keys.get(&endpoint) else {
            return;
        };

        if readable {
            self.mark_readable(key);
        }
        if writable {
            self.flush_blocked(key);
        }
    }

    #[cfg(test)]
    pub fn endpoint_ids(&self) -> Vec<EndpointId> {
        self.endpoints.iter().map(|(_, ep)| ep.id).collect()
    }

    #[cfg(test)]
    pub fn connection_count(&self) -> usize {
        self.conns.len()
    }

    #[cfg(test)]
    pub fn connection_stats(&self, conn: ConnId) -> Option<proto::ConnectionStats> {
        let key = self.conn_keys.get(&conn)?;
        Some(self.conns[*key].inner.stats())
    }

    fn emit(&self, event: Event) {
        // The owner may have gone away; there's nobody left to tell.
        let _ = self.events.send(event);
    }

    fn mark_readable(&mut self, key: usize) {
        let ep = &mut self.endpoints[key];
        if !ep.readable {
            ep.readable = true;
            self.readable.push(key);
        }
    }

    fn mark_dirty(&mut self, key: usize) {
        mark_dirty(&mut self.dirty, &mut self.conns[key], key);
    }

    // ---- Commands ----

    fn drain_commands(&mut self) {
        loop {
            match self.commands.try_recv() {
                Ok(command) => self.handle_command(command),
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.running = false;
                    return;
                }
            }
        }
    }

    fn handle_command(&mut self, command: Command<I::Socket>) {
        let now = self.io.now();

        match command {
            Command::OpenEndpoint {
                endpoint,
                socket,
                setup,
            } => self.open_endpoint(endpoint, socket, *setup),
            Command::CloseEndpoint { endpoint } => self.close_endpoint(endpoint, now),
            Command::Connect {
                endpoint,
                conn,
                connect,
            } => self.connect(endpoint, conn, &connect, now),
            Command::Close { conn, code, reason } => {
                if let Some(key) = self.conn_keys.get(&conn).copied() {
                    self.close_conn(key, code, reason, now);
                }
            }
            Command::OpenStream { conn, dir, request } => {
                let Some(key) = self.conn_keys.get(&conn).copied() else {
                    return;
                };
                let c = &mut self.conns[key];
                if c.lost {
                    return;
                }
                c.pending_opens[dir_index(dir)].push_back(request);
                open_pending(&self.events, c, dir);
            }
            Command::Write { conn, stream, data } => {
                let Some(key) = self.conn_keys.get(&conn).copied() else {
                    return;
                };
                let c = &mut self.conns[key];
                let id = to_proto_stream(stream);
                let Some(send) = c.streams.get_mut(&id).and_then(|s| s.send.as_mut()) else {
                    return;
                };
                if data.is_empty() {
                    return;
                }

                send.queue.push_back(data);
                if !send.blocked {
                    write_queue(&mut c.inner, id, send);
                }
                self.mark_dirty(key);
            }
            Command::Finish { conn, stream } => {
                let Some(key) = self.conn_keys.get(&conn).copied() else {
                    return;
                };
                let c = &mut self.conns[key];
                let id = to_proto_stream(stream);
                let Some(send) = c.streams.get_mut(&id).and_then(|s| s.send.as_mut()) else {
                    return;
                };

                send.finish = true;
                if !send.blocked {
                    write_queue(&mut c.inner, id, send);
                }
                self.mark_dirty(key);
            }
            Command::Reset { conn, stream, code } => {
                let Some(key) = self.conn_keys.get(&conn).copied() else {
                    return;
                };
                let c = &mut self.conns[key];
                let id = to_proto_stream(stream);
                if let Some(mut send) = take_send(c, id) {
                    discard_queue(&mut send);
                    let _ = c.inner.send_stream(id).reset(to_varint(code));
                    self.mark_dirty(key);
                }
            }
            Command::Stop { conn, stream, code } => {
                let Some(key) = self.conn_keys.get(&conn).copied() else {
                    return;
                };
                let c = &mut self.conns[key];
                let id = to_proto_stream(stream);
                if take_recv(c, id).is_some() {
                    let _ = c.inner.recv_stream(id).stop(to_varint(code));
                    self.mark_dirty(key);
                }
            }
            Command::ResumeRead { conn, stream } => {
                let Some(key) = self.conn_keys.get(&conn).copied() else {
                    return;
                };
                self.conns[key].pending_reads.push(to_proto_stream(stream));
                self.mark_dirty(key);
            }
            Command::SendDatagram { conn, data } => {
                let Some(key) = self.conn_keys.get(&conn).copied() else {
                    return;
                };
                if let Err(e) = self.conns[key].inner.datagrams().send(data, true) {
                    tracing::debug!("can't send datagram on connection {}: {e}", conn.0);
                }
                self.mark_dirty(key);
            }
            Command::Shutdown => self.running = false,
        }
    }

    fn open_endpoint(&mut self, id: EndpointId, socket: I::Socket, setup: EndpointSetup) {
        let server_config = match &setup.role {
            Role::Server(config) => Some(config.clone()),
            Role::Client(_) => None,
        };
        let inner = proto::Endpoint::new(
            setup.endpoint_config.clone(),
            server_config,
            !socket.may_fragment(),
            None,
        );

        let payload = setup
            .endpoint_config
            .get_max_udp_payload_size()
            .min(u16::MAX.into()) as usize;
        let stride = payload * socket.max_receive_segments().max(1);
        if stride > self.recv_stride {
            self.recv_stride = stride;
            self.recv_buf = vec![0; stride * BATCH_SIZE];
        }

        let key = self.endpoints.insert(Endpoint {
            id,
            socket,
            inner,
            setup,
            conns: HashMap::new(),
            readable: false,
            blocked: VecDeque::new(),
            waiting: Vec::new(),
            closing: false,
        });
        self.endpoint_keys.insert(id, key);
        // Datagrams may have arrived before the socket was handed over, and
        // their readiness edge was missed.
        self.mark_readable(key);
    }

    fn close_endpoint(&mut self, id: EndpointId, now: Instant) {
        let Some(&key) = self.endpoint_keys.get(&id) else {
            return;
        };
        let ep = &mut self.endpoints[key];
        ep.closing = true;

        let conns: Vec<usize> = ep.conns.values().copied().collect();
        for conn in conns {
            self.close_conn(conn, AppErrorCode::UNKNOWN, Bytes::new(), now);
        }
    }

    fn connect(&mut self, endpoint: EndpointId, conn: ConnId, connect: &Connect, now: Instant) {
        let fail = |core: &Self, reason: &str| {
            core.emit(Event::ConnectionLost {
                conn,
                reason: ConnectionError::Transport(reason.to_owned()),
            });
        };

        let Some(&ep_key) = self.endpoint_keys.get(&endpoint) else {
            return fail(self, "the endpoint is closed");
        };
        let ep = &mut self.endpoints[ep_key];
        let Role::Client(config) = &ep.setup.role else {
            return fail(self, "the endpoint can't make connections");
        };
        if ep.closing {
            return fail(self, "the endpoint is closed");
        }

        let server_name = connect
            .server_name()
            .map(str::to_owned)
            .unwrap_or_else(|| connect.addr().ip().to_string());

        match ep
            .inner
            .connect(now, config.clone(), connect.addr(), &server_name)
        {
            Ok((handle, inner)) => {
                self.insert_conn(ep_key, handle, inner, conn);
            }
            Err(e) => fail(self, &e.to_string()),
        }
    }

    fn insert_conn(
        &mut self,
        ep_key: usize,
        handle: ConnectionHandle,
        inner: proto::Connection,
        id: ConnId,
    ) -> usize {
        let generation = self.next_generation;
        self.next_generation += 1;

        let ep = &mut self.endpoints[ep_key];
        let key = self.conns.insert(Conn {
            id,
            endpoint: ep_key,
            handle,
            inner,
            generation,
            timer: None,
            dirty: false,
            lost: false,
            streams: HashMap::new(),
            pending_opens: Default::default(),
            pending_reads: Vec::new(),
            send_budget: ep.setup.send_budget,
            recv_budget: ep.setup.recv_budget,
        });
        ep.conns.insert(handle, key);
        self.conn_keys.insert(id, key);
        self.mark_dirty(key);
        key
    }

    fn close_conn(&mut self, key: usize, code: AppErrorCode, reason: Bytes, now: Instant) {
        let c = &mut self.conns[key];
        c.inner.close(now, to_varint(code), reason);
        c.streams.clear();
        c.pending_opens = Default::default();

        if !c.lost {
            c.lost = true;
            let conn = c.id;
            self.emit(Event::ConnectionLost {
                conn,
                reason: ConnectionError::LocallyClosed,
            });
        }
        self.mark_dirty(key);
    }

    // ---- Receiving ----

    fn receive(&mut self) {
        let readable = mem::take(&mut self.readable);
        for &key in &readable {
            if self.endpoints.contains(key) {
                self.receive_endpoint(key);
            }
        }

        // Endpoints that hit the per-turn limit are still readable.
        self.readable = readable;
        self.readable
            .retain(|&key| self.endpoints.get(key).is_some_and(|ep| ep.readable));
    }

    fn receive_endpoint(&mut self, key: usize) {
        let mut meta = [RecvMeta::default(); BATCH_SIZE];
        let mut received = 0;

        while received < MAX_RECV_DATAGRAMS {
            let result = {
                let mut chunks = self.recv_buf.chunks_mut(self.recv_stride);
                let mut bufs: [IoSliceMut<'_>; BATCH_SIZE] =
                    std::array::from_fn(|_| IoSliceMut::new(chunks.next().unwrap()));
                self.endpoints[key].socket.recv(&mut bufs, &mut meta)
            };

            match result {
                Ok(count) => {
                    for (i, meta) in meta.iter().take(count).enumerate() {
                        let start = i * self.recv_stride;
                        let mut data = BytesMut::from(&self.recv_buf[start..start + meta.len]);
                        while !data.is_empty() {
                            let datagram = data.split_to(meta.stride.min(data.len()));
                            self.handle_datagram(key, meta, datagram);
                            received += 1;
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.endpoints[key].readable = false;
                    return;
                }
                // Windows reports ICMP port unreachable as a reset on the next
                // receive. It says nothing about the socket itself.
                Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {}
                Err(e) => {
                    tracing::warn!("receive failed on endpoint {}: {e}", self.endpoints[key].id.0);
                    self.endpoints[key].readable = false;
                    return;
                }
            }
        }
    }

    fn handle_datagram(&mut self, ep_key: usize, meta: &RecvMeta, data: BytesMut) {
        let now = self.io.now();
        let ecn = meta
            .ecn
            .and_then(|ecn| proto::EcnCodepoint::from_bits(ecn as u8));

        self.send_buf.clear();
        let ep = &mut self.endpoints[ep_key];
        match ep
            .inner
            .handle(now, meta.addr, meta.dst_ip, ecn, data, &mut self.send_buf)
        {
            Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                if let Some(&key) = ep.conns.get(&handle) {
                    self.conns[key].inner.handle_event(event);
                    self.mark_dirty(key);
                }
            }
            Some(DatagramEvent::NewConnection(incoming)) => self.accept(ep_key, incoming, now),
            Some(DatagramEvent::Response(transmit)) => {
                let buf = mem::take(&mut self.send_buf);
                self.send_response(ep_key, &transmit, &buf);
                self.send_buf = buf;
            }
            None => {}
        }
    }

    fn accept(&mut self, ep_key: usize, incoming: Incoming, now: Instant) {
        let mut buf = mem::take(&mut self.send_buf);
        buf.clear();

        let ep = &mut self.endpoints[ep_key];
        if ep.closing || ep.conns.len() >= ep.setup.max_connections {
            let transmit = ep.inner.refuse(incoming, &mut buf);
            self.send_response(ep_key, &transmit, &buf);
        } else {
            match ep.inner.accept(incoming, now, &mut buf, None) {
                Ok((handle, inner)) => {
                    let endpoint = ep.id;
                    let remote = inner.remote_address();
                    let conn = self.ids.conn();
                    self.insert_conn(ep_key, handle, inner, conn);
                    self.emit(Event::ConnectionAccepted {
                        endpoint,
                        conn,
                        remote,
                    });
                }
                Err(e) => {
                    tracing::debug!("can't accept a connection: {}", e.cause);
                    if let Some(transmit) = e.response {
                        self.send_response(ep_key, &transmit, &buf);
                    }
                }
            }
        }

        self.send_buf = buf;
    }

    // ---- Timers ----

    fn expire_timers(&mut self) {
        let now = self.io.now();

        while let Some(&Reverse((at, key, generation))) = self.timers.peek() {
            if at > now {
                break;
            }
            self.timers.pop();

            let Some(c) = self.conns.get_mut(key) else {
                continue;
            };
            if c.generation != generation || c.timer != Some(at) {
                continue;
            }

            c.timer = None;
            c.inner.handle_timeout(now);
            self.mark_dirty(key);
        }

        // Entries go stale whenever a deadline moves. Drop them before they
        // outnumber the live ones by too much.
        if self.timers.len() > 2 * self.conns.len() + 64 {
            let conns = &self.conns;
            self.timers.retain(|Reverse((at, key, generation))| {
                conns
                    .get(*key)
                    .is_some_and(|c| c.generation == *generation && c.timer == Some(*at))
            });
        }
    }

    // ---- Driving connections ----

    fn drive_dirty(&mut self) {
        let mut dirty = mem::take(&mut self.dirty);
        for key in dirty.drain(..) {
            match self.conns.get_mut(key) {
                Some(c) if c.dirty => c.dirty = false,
                _ => continue,
            }
            self.drive(key);
        }

        if self.dirty.is_empty() {
            // Keep the allocation.
            self.dirty = dirty;
        }
    }

    fn drive(&mut self, key: usize) {
        let now = self.io.now();

        self.poll_conn_events(key);
        if self.poll_endpoint_events(key) {
            return;
        }
        self.transmit(key, now);

        let c = &mut self.conns[key];
        let deadline = c.inner.poll_timeout();
        if deadline != c.timer {
            c.timer = deadline;
            if let Some(at) = deadline {
                self.timers.push(Reverse((at, key, c.generation)));
            }
        }
    }

    fn poll_conn_events(&mut self, key: usize) {
        let c = &mut self.conns[key];
        let conn = c.id;
        let events = &self.events;
        let emit = |event| {
            let _ = events.send(event);
        };

        while let Some(event) = c.inner.poll() {
            match event {
                proto::Event::HandshakeDataReady => {}
                proto::Event::Connected => emit(Event::Connected { conn }),
                proto::Event::ConnectionLost { reason } => {
                    c.streams.clear();
                    c.pending_opens = Default::default();
                    if !c.lost {
                        c.lost = true;
                        emit(Event::ConnectionLost {
                            conn,
                            reason: connection_error(reason),
                        });
                    }
                }
                proto::Event::Stream(StreamEvent::Opened { dir }) => {
                    while let Some(id) = c.inner.streams().accept(dir) {
                        let stream = add_stream(c, id, false);
                        c.pending_reads.push(id);
                        emit(Event::StreamAccepted { conn, stream });
                    }
                }
                proto::Event::Stream(StreamEvent::Readable { id }) => c.pending_reads.push(id),
                proto::Event::Stream(StreamEvent::Writable { id }) => {
                    if let Some(send) = c.streams.get_mut(&id).and_then(|s| s.send.as_mut()) {
                        send.blocked = false;
                        write_queue(&mut c.inner, id, send);
                    }
                }
                proto::Event::Stream(StreamEvent::Finished { id }) => {
                    take_send(c, id);
                    emit(Event::SendFinished {
                        conn,
                        stream: from_proto_stream(id),
                    });
                }
                proto::Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                    if let Some(mut send) = take_send(c, id) {
                        discard_queue(&mut send);
                    }
                    emit(Event::StreamStopped {
                        conn,
                        stream: from_proto_stream(id),
                        code: from_varint(error_code),
                    });
                }
                proto::Event::Stream(StreamEvent::Available { dir }) => {
                    open_pending(events, c, from_proto_dir(dir));
                }
                proto::Event::DatagramReceived => {
                    while let Some(data) = c.inner.datagrams().recv() {
                        emit(Event::Datagram { conn, data });
                    }
                }
                proto::Event::DatagramsUnblocked => {}
            }
        }

        // quinn reports `Available` when the peer raises its stream limit,
        // but not when the handshake sets the initial one.
        if !c.lost {
            open_pending(events, c, Dir::Bi);
            open_pending(events, c, Dir::Uni);
        }

        let mut reads = mem::take(&mut c.pending_reads);
        for id in reads.drain(..) {
            read_stream(events, c, id);
        }
        c.pending_reads = reads;
    }

    /// Hands the connection's events to its endpoint. Returns `true` if the
    /// connection drained and was removed.
    fn poll_endpoint_events(&mut self, key: usize) -> bool {
        let c = &mut self.conns[key];
        let ep = &mut self.endpoints[c.endpoint];

        while let Some(event) = c.inner.poll_endpoint_events() {
            let drained = event.is_drained();
            if let Some(reply) = ep.inner.handle_event(c.handle, event) {
                c.inner.handle_event(reply);
            }

            if drained {
                self.remove_conn(key);
                return true;
            }
        }

        false
    }

    fn remove_conn(&mut self, key: usize) {
        let c = self.conns.remove(key);
        self.conn_keys.remove(&c.id);
        self.endpoints[c.endpoint].conns.remove(&c.handle);

        if !c.lost {
            self.emit(Event::ConnectionLost {
                conn: c.id,
                reason: ConnectionError::LocallyClosed,
            });
        }
    }

    fn transmit(&mut self, key: usize, now: Instant) {
        let c = &mut self.conns[key];
        let ep = &mut self.endpoints[c.endpoint];
        if !ep.blocked.is_empty() {
            ep.waiting.push(key);
            return;
        }

        let max_segments = ep.socket.max_transmit_segments();
        let mut sent = 0;
        loop {
            if sent >= MAX_TRANSMIT_DATAGRAMS {
                // Let the other connections go first.
                mark_dirty(&mut self.dirty, c, key);
                return;
            }

            self.send_buf.clear();
            let Some(transmit) = c.inner.poll_transmit(now, max_segments, &mut self.send_buf)
            else {
                return;
            };
            sent += transmit
                .segment_size
                .map_or(1, |size| transmit.size.div_ceil(size));

            let udp = udp_transmit(&transmit, &self.send_buf[..transmit.size]);
            match ep.socket.try_send(&udp) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ep.blocked.push_back(own_transmit(&udp));
                    ep.waiting.push(key);
                    self.io.set_writable_interest(&mut ep.socket, ep.id, true);
                    return;
                }
                Err(e) => log_send_error(&e),
            }
        }
    }

    fn send_response(&mut self, ep_key: usize, transmit: &proto::Transmit, buf: &[u8]) {
        let ep = &mut self.endpoints[ep_key];
        let udp = udp_transmit(transmit, &buf[..transmit.size]);

        if !ep.blocked.is_empty() {
            if ep.blocked.len() < MAX_QUEUED_RESPONSES {
                ep.blocked.push_back(own_transmit(&udp));
            }
            return;
        }

        match ep.socket.try_send(&udp) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                ep.blocked.push_back(own_transmit(&udp));
                self.io.set_writable_interest(&mut ep.socket, ep.id, true);
            }
            Err(e) => log_send_error(&e),
        }
    }

    /// Sends the transmits queued while the socket was blocked, then lets the
    /// waiting connections send again.
    fn flush_blocked(&mut self, key: usize) {
        let ep = &mut self.endpoints[key];
        if ep.blocked.is_empty() {
            return;
        }

        while let Some(transmit) = ep.blocked.front() {
            match ep.socket.try_send(&transmit.as_udp()) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
                Err(e) => log_send_error(&e),
            }
            ep.blocked.pop_front();
        }

        self.io.set_writable_interest(&mut ep.socket, ep.id, false);
        let waiting = mem::take(&mut ep.waiting);
        for conn in waiting {
            if let Some(c) = self.conns.get_mut(conn) {
                mark_dirty(&mut self.dirty, c, conn);
            }
        }
    }

    fn reap_endpoints(&mut self) {
        let done: Vec<usize> = self
            .endpoints
            .iter()
            .filter(|(_, ep)| ep.closing && ep.conns.is_empty() && ep.blocked.is_empty())
            .map(|(key, _)| key)
            .collect();

        for key in done {
            let ep = self.endpoints.remove(key);
            self.endpoint_keys.remove(&ep.id);
            self.readable.retain(|&k| k != key);
            self.emit(Event::EndpointClosed { endpoint: ep.id });
        }
    }
}

fn mark_dirty(dirty: &mut Vec<usize>, c: &mut Conn, key: usize) {
    if !c.dirty {
        c.dirty = true;
        dirty.push(key);
    }
}

fn add_stream(c: &mut Conn, id: proto::StreamId, local: bool) -> StreamInfo {
    let dir = from_proto_dir(id.dir());
    let can_send = dir == Dir::Bi || local;
    let can_recv = dir == Dir::Bi || !local;

    let send = can_send.then(|| Arc::new(SendBudget::new(c.send_budget)));
    let recv = can_recv.then(|| Arc::new(RecvBudget::new(c.recv_budget)));

    c.streams.insert(
        id,
        StreamState {
            send: send.clone().map(|budget| SendHalf {
                budget,
                queue: VecDeque::new(),
                blocked: false,
                finish: false,
            }),
            recv: recv.clone(),
        },
    );

    StreamInfo {
        id: from_proto_stream(id),
        dir,
        send,
        recv,
    }
}

fn take_send(c: &mut Conn, id: proto::StreamId) -> Option<SendHalf> {
    let stream = c.streams.get_mut(&id)?;
    let send = stream.send.take();
    if stream.recv.is_none() {
        c.streams.remove(&id);
    }
    send
}

fn take_recv(c: &mut Conn, id: proto::StreamId) -> Option<Arc<RecvBudget>> {
    let stream = c.streams.get_mut(&id)?;
    let recv = stream.recv.take();
    if stream.send.is_none() {
        c.streams.remove(&id);
    }
    recv
}

fn open_pending(events: &Sender<Event>, c: &mut Conn, dir: Dir) {
    let conn = c.id;
    while let Some(&request) = c.pending_opens[dir_index(dir)].front() {
        let Some(id) = c.inner.streams().open(to_proto_dir(dir)) else {
            return;
        };
        c.pending_opens[dir_index(dir)].pop_front();

        let stream = add_stream(c, id, true);
        let _ = events.send(Event::StreamOpened {
            conn,
            request,
            stream,
        });
    }
}

/// Hands queued data to quinn until it's all accepted or quinn is blocked,
/// then finishes the stream if asked to. Bytes leave the send budget only
/// once quinn has accepted them.
fn write_queue(conn: &mut proto::Connection, id: proto::StreamId, send: &mut SendHalf) {
    let mut stream = conn.send_stream(id);

    while !send.queue.is_empty() {
        match stream.write_chunks(send.queue.make_contiguous()) {
            // A partial write leaves the rest of its chunk in place. The next
            // write then returns `Blocked`, which registers for `Writable`.
            Ok(written) => {
                send.budget.release(written.bytes);
                send.queue.drain(..written.chunks);
            }
            Err(WriteError::Blocked) => {
                send.blocked = true;
                return;
            }
            // The peer stopped the stream (also reported as an event), or it
            // was already finished or reset. Nothing queued can be sent.
            Err(WriteError::Stopped(_) | WriteError::ClosedStream) => {
                discard_queue(send);
                return;
            }
        }
    }

    if send.finish {
        // Fails only if the stream was stopped, which arrives as an event.
        let _ = stream.finish();
        send.finish = false;
    }
}

fn discard_queue(send: &mut SendHalf) {
    let queued: usize = send.queue.drain(..).map(|data| data.len()).sum();
    send.budget.release(queued);
}

/// Reads the stream until it's empty or its receive budget is full.
fn read_stream(events: &Sender<Event>, c: &mut Conn, id: proto::StreamId) {
    enum End {
        Finished,
        Reset(VarInt),
    }

    let Some(budget) = c.streams.get(&id).and_then(|s| s.recv.clone()) else {
        return;
    };
    let mut recv = c.inner.recv_stream(id);
    let Ok(mut chunks) = recv.read(true) else {
        return;
    };

    let mut data = Vec::new();
    let mut end = None;
    loop {
        let room = budget.room();
        if room == 0 {
            if budget.pause() {
                break;
            }
            continue;
        }

        match chunks.next(room) {
            Ok(Some(chunk)) => {
                budget.deliver(chunk.bytes.len());
                data.push(chunk.bytes);
            }
            Ok(None) => {
                end = Some(End::Finished);
                break;
            }
            Err(ReadError::Blocked) => break,
            Err(ReadError::Reset(code)) => {
                end = Some(End::Reset(code));
                break;
            }
        }
    }
    // Any flow control update this queues is sent when the connection
    // transmits, later in the same drive.
    let _ = chunks.finalize();

    let conn = c.id;
    let stream = from_proto_stream(id);
    if !data.is_empty() {
        let _ = events.send(Event::StreamData {
            conn,
            stream,
            chunks: data,
        });
    }

    let event = match end {
        None => return,
        Some(End::Finished) => Event::StreamFinished { conn, stream },
        Some(End::Reset(code)) => Event::StreamReset {
            conn,
            stream,
            code: from_varint(code),
        },
    };
    take_recv(c, id);
    let _ = events.send(event);
}

fn udp_transmit<'a>(transmit: &proto::Transmit, contents: &'a [u8]) -> quinn_udp::Transmit<'a> {
    quinn_udp::Transmit {
        destination: transmit.destination,
        ecn: transmit
            .ecn
            .and_then(|ecn| quinn_udp::EcnCodepoint::from_bits(ecn as u8)),
        contents,
        segment_size: transmit.segment_size,
        src_ip: transmit.src_ip,
    }
}

fn own_transmit(transmit: &quinn_udp::Transmit<'_>) -> OwnedTransmit {
    OwnedTransmit {
        destination: transmit.destination,
        ecn: transmit.ecn,
        segment_size: transmit.segment_size,
        src_ip: transmit.src_ip,
        contents: transmit.contents.to_vec(),
    }
}

fn log_send_error(e: &io::Error) {
    // UDP sends are best effort; QUIC retransmits whatever is lost. Path MTU
    // probes larger than the interface MTU fail with EMSGSIZE by design.
    if quinn_udp::is_msg_size_err(e) {
        tracing::trace!("send failed: {e}");
    } else {
        tracing::debug!("send failed: {e}");
    }
}

fn dir_index(dir: Dir) -> usize {
    match dir {
        Dir::Bi => 0,
        Dir::Uni => 1,
    }
}

fn to_proto_dir(dir: Dir) -> proto::Dir {
    match dir {
        Dir::Bi => proto::Dir::Bi,
        Dir::Uni => proto::Dir::Uni,
    }
}

fn from_proto_dir(dir: proto::Dir) -> Dir {
    match dir {
        proto::Dir::Bi => Dir::Bi,
        proto::Dir::Uni => Dir::Uni,
    }
}

fn to_proto_stream(id: StreamId) -> proto::StreamId {
    // Stream IDs only ever come from quinn, so they fit.
    proto::StreamId::from(VarInt::from_u64(id.0).unwrap_or(VarInt::MAX))
}

fn from_proto_stream(id: proto::StreamId) -> StreamId {
    StreamId(id.into())
}
