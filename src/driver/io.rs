//! The sockets and clock the driver core runs on. The core only sees the
//! [`Io`] and [`Socket`] traits, so tests can swap in a simulated network and
//! a fake clock.

use std::{
    io::{self, IoSliceMut},
    net::SocketAddr,
    time::Instant,
};

use mio::{Interest, Registry, Token};
use quinn_udp::{RecvMeta, Transmit, UdpSocketState};

use super::protocol::EndpointId;

/// A UDP socket as the driver core uses it. Both calls must be non-blocking.
pub(crate) trait Socket {
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// How many datagrams one [`Transmit`] may carry (GSO).
    fn max_transmit_segments(&self) -> usize;

    /// How many datagrams one receive buffer may hold (GRO).
    fn max_receive_segments(&self) -> usize;

    /// Whether outgoing datagrams may be fragmented. Path MTU discovery is
    /// only enabled when they can't.
    fn may_fragment(&self) -> bool;

    /// Sends one transmit, or fails with [`io::ErrorKind::WouldBlock`].
    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()>;

    /// Receives up to `bufs.len()` buffers, or fails with
    /// [`io::ErrorKind::WouldBlock`].
    fn recv(&self, bufs: &mut [IoSliceMut<'_>], meta: &mut [RecvMeta]) -> io::Result<usize>;
}

/// The clock, and readiness notifications for the core's sockets.
pub(crate) trait Io {
    type Socket: Socket;

    fn now(&self) -> Instant;

    /// Starts or stops waiting for `socket` to become writable. Readable
    /// notifications are always on.
    fn set_writable_interest(&self, socket: &mut Self::Socket, endpoint: EndpointId, on: bool);
}

pub(super) fn token(endpoint: EndpointId) -> Token {
    Token(endpoint.0 as usize)
}

/// A real UDP socket, registered with a mio [`Registry`].
pub(crate) struct UdpSocket {
    io: mio::net::UdpSocket,
    state: UdpSocketState,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let socket = std::net::UdpSocket::bind(addr)?;
        let state = UdpSocketState::new((&socket).into())?;
        socket.set_nonblocking(true)?;

        Ok(Self {
            io: mio::net::UdpSocket::from_std(socket),
            state,
        })
    }

    pub fn register(&mut self, registry: &Registry, endpoint: EndpointId) -> io::Result<()> {
        registry.register(&mut self.io, token(endpoint), Interest::READABLE)
    }
}

impl Socket for UdpSocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.state.max_gso_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.state.gro_segments()
    }

    fn may_fragment(&self) -> bool {
        self.state.may_fragment()
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        self.state.try_send((&self.io).into(), transmit)
    }

    fn recv(&self, bufs: &mut [IoSliceMut<'_>], meta: &mut [RecvMeta]) -> io::Result<usize> {
        self.state.recv((&self.io).into(), bufs, meta)
    }
}

/// The system clock and a mio [`Registry`].
pub(crate) struct MioIo {
    pub registry: Registry,
}

impl Io for MioIo {
    type Socket = UdpSocket;

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn set_writable_interest(&self, socket: &mut UdpSocket, endpoint: EndpointId, on: bool) {
        let interest = if on {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };

        if let Err(e) = self
            .registry
            .reregister(&mut socket.io, token(endpoint), interest)
        {
            tracing::warn!("can't change interest for endpoint {}: {e}", endpoint.0);
        }
    }
}
