//! The sockets, clock and peer drivers the driver core runs on. The core
//! only sees the [`Io`], [`Socket`] and [`SocketSender`] traits, so tests can
//! swap in a simulated network, a fake clock and drivers on one thread.

use std::{
    io::{self, IoSliceMut},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use mio::{Interest, Registry, Token};
use quinn_udp::{RecvMeta, Transmit, UdpSocketState};

use super::{
    DriverGroup,
    protocol::{Command, EndpointId},
};

/// The sending side of a [`Socket`]. Every clone sends on the same socket, so
/// connections driven by other drivers can send on their endpoint's socket.
pub(crate) trait SocketSender: Clone {
    /// How many datagrams one [`Transmit`] may carry (GSO).
    fn max_transmit_segments(&self) -> usize;

    /// Sends one transmit, or fails with [`io::ErrorKind::WouldBlock`].
    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()>;
}

/// A UDP socket as the driver core uses it. Every call must be non-blocking.
pub(crate) trait Socket {
    type Sender: SocketSender;

    fn sender(&self) -> &Self::Sender;

    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// How many datagrams one receive buffer may hold (GRO).
    fn max_receive_segments(&self) -> usize;

    /// Whether outgoing datagrams may be fragmented. Path MTU discovery is
    /// only enabled when they can't.
    fn may_fragment(&self) -> bool;

    /// Receives up to `bufs.len()` buffers, or fails with
    /// [`io::ErrorKind::WouldBlock`].
    fn recv(&self, bufs: &mut [IoSliceMut<'_>], meta: &mut [RecvMeta]) -> io::Result<usize>;
}

/// The clock, readiness notifications for the core's sockets, and the other
/// drivers in the core's group.
///
/// Drivers in a group are numbered from 0. A server endpoint may hand its
/// connections to any driver in its group.
pub(crate) trait Io {
    type Socket: Socket;

    fn now(&self) -> Instant;

    /// Starts or stops waiting for `socket` to become writable. Readable
    /// notifications are always on.
    fn set_writable_interest(&self, socket: &mut Self::Socket, endpoint: EndpointId, on: bool);

    /// This driver's number in its group.
    fn driver_index(&self) -> usize;

    /// How many drivers the group has.
    fn driver_count(&self) -> usize;

    /// Queues a command for a driver in the group without waking it.
    fn send_to(&self, driver: usize, command: Command<Self::Socket>);

    /// Wakes a driver in the group if commands were queued for it.
    fn wake(&self, driver: usize);

    /// How many connections the driver has, counting ones handed to it that
    /// it hasn't taken yet.
    fn load(&self, driver: usize) -> usize;

    fn add_load(&self, driver: usize, delta: isize);
}

pub(super) fn token(endpoint: EndpointId) -> Token {
    Token(endpoint.0 as usize)
}

/// A real UDP socket, registered with a mio [`Registry`].
pub(crate) struct UdpSocket {
    io: mio::net::UdpSocket,
    sender: UdpSender,
}

/// A second handle to a [`UdpSocket`], for sending. It keeps the socket open
/// until the last clone is dropped, so a connection on another driver can
/// still send after the endpoint's driver has closed its handle.
#[derive(Clone)]
pub(crate) struct UdpSender(Arc<SenderInner>);

struct SenderInner {
    socket: std::net::UdpSocket,
    state: UdpSocketState,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let socket = std::net::UdpSocket::bind(addr)?;
        let state = UdpSocketState::new((&socket).into())?;
        grow_buffers(&socket, &state);
        socket.set_nonblocking(true)?;
        let send = socket.try_clone()?;
        // Shared with the original on Unix, but not on every platform.
        send.set_nonblocking(true)?;

        Ok(Self {
            io: mio::net::UdpSocket::from_std(socket),
            sender: UdpSender(Arc::new(SenderInner {
                socket: send,
                state,
            })),
        })
    }

    pub fn register(&mut self, registry: &Registry, endpoint: EndpointId) -> io::Result<()> {
        registry.register(&mut self.io, token(endpoint), Interest::READABLE)
    }
}

/// The socket buffer size to ask for. A server's connections all share one
/// socket, and the usual OS default (about 200 KB on Linux) overflows when
/// hundreds of clients send at once, so datagrams are dropped before the
/// driver reads them. The OS may cap the size lower (on Linux,
/// `net.core.rmem_max` and `net.core.wmem_max`).
const SOCKET_BUFFER: usize = 4 << 20;

fn grow_buffers(socket: &std::net::UdpSocket, state: &UdpSocketState) {
    let result = (|| {
        if state.recv_buffer_size(socket.into())? < SOCKET_BUFFER {
            state.set_recv_buffer_size(socket.into(), SOCKET_BUFFER)?;
        }
        if state.send_buffer_size(socket.into())? < SOCKET_BUFFER {
            state.set_send_buffer_size(socket.into(), SOCKET_BUFFER)?;
        }
        io::Result::Ok(())
    })();

    // The defaults still work, just with more loss under load.
    if let Err(e) = result {
        tracing::debug!("can't grow the socket buffers: {e}");
    }
}

impl SocketSender for UdpSender {
    fn max_transmit_segments(&self) -> usize {
        self.0.state.max_gso_segments()
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        self.0.state.try_send((&self.0.socket).into(), transmit)
    }
}

impl Socket for UdpSocket {
    type Sender = UdpSender;

    fn sender(&self) -> &UdpSender {
        &self.sender
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    fn max_receive_segments(&self) -> usize {
        self.sender.0.state.gro_segments()
    }

    fn may_fragment(&self) -> bool {
        self.sender.0.state.may_fragment()
    }

    fn recv(&self, bufs: &mut [IoSliceMut<'_>], meta: &mut [RecvMeta]) -> io::Result<usize> {
        self.sender.0.state.recv((&self.io).into(), bufs, meta)
    }
}

/// The system clock, a mio [`Registry`], and the driver's group.
pub(crate) struct MioIo {
    pub registry: Registry,
    pub group: Arc<DriverGroup>,
    pub index: usize,
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

    fn driver_index(&self) -> usize {
        self.index
    }

    fn driver_count(&self) -> usize {
        self.group.len()
    }

    fn send_to(&self, driver: usize, command: Command<UdpSocket>) {
        if let Some(handle) = self.group.handle(driver) {
            handle.send(command);
        }
    }

    fn wake(&self, driver: usize) {
        if let Some(handle) = self.group.handle(driver) {
            handle.wake_from_peer();
        }
    }

    fn load(&self, driver: usize) -> usize {
        self.group.load(driver)
    }

    fn add_load(&self, driver: usize, delta: isize) {
        self.group.add_load(driver, delta);
    }
}
