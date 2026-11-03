use aeronet_io::packet::RecvPacket;
use bevy::{
    ecs::{component::Component, lifecycle::HookContext, world::DeferredWorld},
    prelude::{Deref, DerefMut},
};
use bytes::Bytes;
use std::{
    collections::VecDeque,
    fmt,
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use thiserror::Error;

use crate::{
    common::{
        QuicParentId,
        attempt::{OpenRequest, QuicActionAttempt},
        connection::{
            disconnect::ConnectionDisconnectReason, id::ConnectionId,
            stats::ConnectionStats,
        },
        error::{AppErrorCode, ConnectionError, DatagramError},
        router::{self, ConnDelivery, Notice},
        runtime::Link,
        stream::{
            QuicBidirectionalStreamAttempt, QuicPeerStream, QuicPeerStreamAttempt,
            QuicReceiveStreamAttempt, QuicSendStreamAttempt,
        },
    },
    driver::{Command, ConnId, Dir},
};

pub mod disconnect;
pub mod id;
pub mod plugin;
pub mod stats;

/// This is a structure which represents an in progress connection.
/// The connection is closed if this is dropped before it connects.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a full [QuicConnection]
/// on the same entity.
///
/// In the event of a failure, with the `connection-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Deref, DerefMut, Component)]
#[component(storage = "SparseSet")]
pub struct QuicConnectionAttempt(QuicActionAttempt<QuicConnection, QuicParentId>);

impl QuicConnectionAttempt {
    pub(crate) fn new(connection: QuicConnection, parent_id: QuicParentId) -> Self {
        Self(QuicActionAttempt::new_connect(connection, parent_id))
    }
}

/// A QUIC connection to a peer, from which streams are opened and accepted,
/// and unreliable datagrams sent and received.
///
/// The connection's state, such as [Self::is_open] and [Self::stats], is
/// mirrored from the network thread once per frame, in
/// [IoSystems::Poll](https://docs.rs/aeronet_io/latest/aeronet_io/enum.IoSystems.html),
/// so it can be up to one frame out of date.
///
/// Dropping the component closes the connection.
#[derive(Component)]
#[component(on_insert, on_discard)]
pub struct QuicConnection {
    link: Link,
    conn: ConnId,
    connection_id: ConnectionId,
    remote_addr: SocketAddr,
    connected: bool,
    /// [Self::close] was called.
    closing: AtomicBool,
    lost: Option<ConnectionError>,
    accepted: VecDeque<QuicPeerStream>,
    stats: ConnectionStats,
    next_request: u64,
    /// Received datagrams that haven't been read, oldest first.
    datagrams: VecDeque<RecvPacket>,
    datagram_bytes: usize,
    /// The most bytes `datagrams` holds before the oldest are dropped.
    datagram_buffer: usize,
    min_datagram_size: Option<usize>,
    max_datagram_size: Option<usize>,
}

impl fmt::Debug for QuicConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicConnection")
            .field("id", &self.connection_id)
            .field("remote_addr", &self.remote_addr)
            .field("connected", &self.connected)
            .field("closing", &self.closing.load(Ordering::Relaxed))
            .field("lost", &self.lost)
            .field("accepted", &self.accepted.len())
            .field("unread_datagrams", &self.datagrams.len())
            .finish_non_exhaustive()
    }
}

impl QuicConnection {
    pub(crate) fn new(
        link: Link,
        conn: ConnId,
        connection_id: ConnectionId,
        remote_addr: SocketAddr,
        datagram_buffer: usize,
    ) -> Self {
        Self {
            link,
            conn,
            connection_id,
            remote_addr,
            connected: false,
            closing: AtomicBool::new(false),
            lost: None,
            accepted: VecDeque::new(),
            stats: ConnectionStats::default(),
            next_request: 0,
            datagrams: VecDeque::new(),
            datagram_bytes: 0,
            datagram_buffer,
            min_datagram_size: None,
            max_datagram_size: None,
        }
    }

    fn on_insert(world: DeferredWorld, context: HookContext) {
        router::bind::<Self>(world, context);
    }

    fn on_discard(world: DeferredWorld, context: HookContext) {
        router::unbind::<Self>(world, context);
    }

    pub(crate) fn conn(&self) -> ConnId {
        self.conn
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected
    }

    pub(crate) fn lost_reason(&self) -> Option<&ConnectionError> {
        self.lost.as_ref()
    }

    pub(crate) fn deliver(&mut self, delivery: ConnDelivery) {
        match delivery {
            ConnDelivery::Connected { min_datagram_size } => {
                self.connected = true;
                self.min_datagram_size = min_datagram_size;
            }
            ConnDelivery::Lost(reason) => {
                self.lost.get_or_insert(reason);
            }
            ConnDelivery::Accepted(stream) => self.accepted.push_back(*stream),
            ConnDelivery::Stats {
                stats,
                max_datagram_size,
            } => {
                self.stats = stats;
                self.max_datagram_size = max_datagram_size;
            }
            ConnDelivery::Datagrams(data, recv_at) => {
                if self.closing.load(Ordering::Relaxed) {
                    return;
                }
                for payload in data {
                    self.datagram_bytes += payload.len();
                    self.datagrams.push_back(RecvPacket { recv_at, payload });
                }
                while self.datagram_bytes > self.datagram_buffer
                    && let Some(old) = self.datagrams.pop_front()
                {
                    self.datagram_bytes -= old.payload.len();
                }
            }
        }
    }

    /// The largest datagram the connection can send whatever the path MTU,
    /// or `None` if the peer doesn't accept datagrams.
    pub(crate) fn min_datagram_size(&self) -> Option<usize> {
        self.min_datagram_size
    }

    pub(crate) fn has_datagrams(&self) -> bool {
        !self.datagrams.is_empty()
    }

    /// Takes the oldest stream the peer opened, if any.
    ///
    /// Returns an attempt that resolves to the stream, so it can be spawned
    /// and handled like other stream attempts.
    pub fn accept_stream(
        &mut self,
    ) -> Result<Option<QuicPeerStreamAttempt>, ConnectionCommandError> {
        if !self.is_open() {
            return Ok(None);
        }

        let attempt = self
            .accepted
            .pop_front()
            .map(|stream| QuicPeerStreamAttempt::new_ready(Some(stream), self.id()));
        Ok(attempt)
    }

    /// Takes the oldest receive stream the peer opened, if any. Bidirectional
    /// streams are left for [Self::accept_stream] and
    /// [Self::accept_bidirectional_stream].
    pub fn accept_receive_stream(
        &mut self,
    ) -> Result<Option<QuicReceiveStreamAttempt>, ConnectionCommandError> {
        if !self.is_open() {
            return Ok(None);
        }

        let index = self
            .accepted
            .iter()
            .position(|stream| matches!(stream, QuicPeerStream::Receive(_)));
        let attempt = match index.and_then(|i| self.accepted.remove(i)) {
            Some(QuicPeerStream::Receive(recv)) => {
                Some(QuicReceiveStreamAttempt::new_ready(Some(recv), self.id()))
            }
            _ => None,
        };
        Ok(attempt)
    }

    /// Takes the oldest bidirectional stream the peer opened, if any. Receive
    /// streams are left for [Self::accept_stream] and
    /// [Self::accept_receive_stream].
    pub fn accept_bidirectional_stream(
        &mut self,
    ) -> Result<Option<QuicBidirectionalStreamAttempt>, ConnectionCommandError> {
        if !self.is_open() {
            return Ok(None);
        }

        let index = self
            .accepted
            .iter()
            .position(|stream| matches!(stream, QuicPeerStream::Bidirectional(..)));
        let attempt = match index.and_then(|i| self.accepted.remove(i)) {
            Some(QuicPeerStream::Bidirectional(recv, send)) => Some(
                QuicBidirectionalStreamAttempt::new_ready(Some((recv, send)), self.id()),
            ),
            _ => None,
        };
        Ok(attempt)
    }

    /// Opens a new bidirectional stream. If the peer's stream limit is
    /// reached, the attempt waits until the peer allows more.
    ///
    /// Returns [ConnectionCommandError::Closed] if the connection is closed.
    pub fn open_bidrectional_stream(
        &mut self,
    ) -> Result<QuicBidirectionalStreamAttempt, ConnectionCommandError> {
        let request = self.open(Dir::Bi)?;
        Ok(QuicBidirectionalStreamAttempt::new_open(request, self.id()))
    }

    /// Opens a new send stream. If the peer's stream limit is reached, the
    /// attempt waits until the peer allows more.
    ///
    /// Returns [ConnectionCommandError::Closed] if the connection is closed.
    pub fn open_send_stream(
        &mut self,
    ) -> Result<QuicSendStreamAttempt, ConnectionCommandError> {
        let request = self.open(Dir::Uni)?;
        Ok(QuicSendStreamAttempt::new_open(request, self.id()))
    }

    fn open(&mut self, dir: Dir) -> Result<OpenRequest, ConnectionCommandError> {
        if !self.is_open() {
            return Err(ConnectionCommandError::Closed);
        }

        let request = self.next_request;
        self.next_request += 1;
        self.link.send(Command::OpenStream {
            conn: self.conn,
            dir,
            request,
        });
        Ok(OpenRequest::new(self.link.clone(), self.conn, request))
    }

    /// Closes the connection, sending `code` to the peer. Data queued on its
    /// streams that hasn't been sent yet is lost: close or finish the streams
    /// and wait for them to close first to deliver it.
    pub fn close(&self, code: AppErrorCode) {
        self.close_with_reason(code, Bytes::new());
    }

    /// Like [Self::close], also sending `reason` to the peer, which sees it
    /// in [ConnectionError::ApplicationClosed]. A reason too long to fit in
    /// a packet is cut short.
    pub fn close_with_reason(&self, code: AppErrorCode, reason: impl Into<Bytes>) {
        if !self.is_open() {
            return;
        }

        self.closing.store(true, Ordering::Relaxed);
        self.link.send(Command::Close {
            conn: self.conn,
            code,
            reason: reason.into(),
        });
    }

    /// Queues an unreliable datagram to send to the peer.
    ///
    /// Datagrams may be lost, duplicated or arrive out of order, and are
    /// never retransmitted. They are sent as soon as congestion control
    /// allows. If they're queued faster than that, the oldest queued
    /// datagrams are dropped to make room, and counted in
    /// [ConnectionStats::datagrams_dropped].
    ///
    /// Returns [DatagramError::TooLarge] if `data` is larger than
    /// [Self::max_datagram_size], [DatagramError::Unsupported] if the peer
    /// doesn't accept datagrams, and [DatagramError::Closed] if the
    /// connection is closed.
    pub fn send_datagram(&self, data: Bytes) -> Result<(), DatagramError> {
        if !self.is_open() {
            return Err(DatagramError::Closed);
        }
        let Some(max) = self.max_datagram_size else {
            return Err(DatagramError::Unsupported);
        };
        if data.len() > max {
            return Err(DatagramError::TooLarge {
                size: data.len(),
                max,
            });
        }

        self.send_datagrams(vec![data]);
        Ok(())
    }

    /// Queues datagrams without checking their size. The network thread
    /// drops, and counts, the ones that don't fit.
    pub(crate) fn send_datagrams(&self, data: Vec<Bytes>) {
        if self.is_open() && !data.is_empty() {
            self.link.send(Command::SendDatagrams {
                conn: self.conn,
                data,
            });
        }
    }

    /// Takes the oldest datagram received from the peer, if any.
    ///
    /// Datagrams are delivered once per frame. Up to a fixed number of bytes
    /// of them are kept until they're read; past that, the oldest are
    /// dropped.
    pub fn recv_datagram(&mut self) -> Option<RecvPacket> {
        let packet = self.datagrams.pop_front()?;
        self.datagram_bytes -= packet.payload.len();
        Some(packet)
    }

    /// Takes up to `limit` received datagrams, oldest first, and pushes them
    /// to `buffer`. Returns how many were pushed.
    pub fn recv_datagrams(&mut self, buffer: &mut Vec<RecvPacket>, limit: usize) -> usize {
        let count = limit.min(self.datagrams.len());
        let start = buffer.len();
        buffer.extend(self.datagrams.drain(..count));
        let bytes: usize = buffer[start..].iter().map(|p| p.payload.len()).sum();
        self.datagram_bytes -= bytes;
        count
    }

    /// The largest datagram [Self::send_datagram] accepts, or `None` if the
    /// peer doesn't accept datagrams. It grows as path MTU discovery finds
    /// larger sizes.
    pub fn max_datagram_size(&self) -> Option<usize> {
        self.max_datagram_size
    }

    /// Returns true if the connection is still open.
    pub fn is_open(&self) -> bool {
        self.lost.is_none() && !self.closing.load(Ordering::Relaxed)
    }

    /// Returns true if [accept_stream][Self::accept_stream()] has a stream to
    /// return.
    #[deprecated(
        since = "0.18.3",
        note = "Unnecessary since now all accept functions do this check internally and do so with atomic correctness. Resolves a bug and gets rid of unnecessary API."
    )]
    pub fn should_poll_accept(&self) -> bool {
        !self.accepted.is_empty() && self.is_open()
    }

    /// Gets the disconnect reason if the connection has closed.
    /// Returns `None` if the connection is still open.
    pub fn get_disconnect_reason(&mut self) -> Option<ConnectionDisconnectReason> {
        self.lost
            .clone()
            .map(ConnectionDisconnectReason::ConnectionError)
    }

    /// The peer's address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// The connection's statistics.
    pub fn stats(&self) -> ConnectionStats {
        self.stats
    }

    /// The current estimate of the round-trip time.
    pub fn rtt(&self) -> Duration {
        self.stats.rtt
    }

    /// The largest UDP payload the path currently supports. Grows as path MTU
    /// discovery finds larger sizes.
    pub fn current_mtu(&self) -> u16 {
        self.stats.current_mtu
    }

    /// Gets the ID information for the parent client or server for this connection
    pub fn parent_id(&self) -> QuicParentId {
        self.connection_id.parent_id()
    }

    /// Gets the ID information for both the parent and this connection
    pub fn id(&self) -> ConnectionId {
        self.connection_id
    }
}

impl Drop for QuicConnection {
    fn drop(&mut self) {
        if self.is_open() {
            self.link.send(Command::Close {
                conn: self.conn,
                code: AppErrorCode::UNKNOWN,
                reason: Bytes::new(),
            });
        }
        self.link.notify(Notice::ConnReleased { conn: self.conn });
    }
}

/// Errors from asking a connection to do something.
#[derive(Debug, Error, Clone, Copy)]
pub enum ConnectionCommandError {
    /// No longer returned: commands aren't limited.
    #[error("The connection's command queue is full.")]
    Full,
    /// The connection is closed.
    #[error("The connection is closed.")]
    Closed,
}
