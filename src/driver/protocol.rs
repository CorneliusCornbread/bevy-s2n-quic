//! The messages exchanged between the driver thread and its owner, and the
//! byte budgets they share.

use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;

use crate::{
    client::Connect,
    common::error::{AppErrorCode, ConnectionError},
};

use super::config::EndpointSetup;

/// Identifies an endpoint (one socket) on a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct EndpointId(pub u64);

/// Identifies a connection on a driver. Never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ConnId(pub u64);

/// Identifies a stream within its connection. This is the QUIC stream ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StreamId(pub u64);

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream {}", self.0)
    }
}

/// Which ways data flows on a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Dir {
    Bi,
    Uni,
}

/// Hands out endpoint and connection IDs. Shared by a driver and its handles,
/// so either side can name a new endpoint or connection.
#[derive(Debug, Default)]
pub(crate) struct IdAllocator(AtomicU64);

impl IdAllocator {
    pub fn endpoint(&self) -> EndpointId {
        EndpointId(self.next())
    }

    pub fn conn(&self) -> ConnId {
        ConnId(self.next())
    }

    fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Bytes queued for a send stream that quinn hasn't accepted yet.
///
/// The owner reserves bytes before sending a [`Command::Write`], and the driver
/// releases them as quinn accepts them.
#[derive(Debug)]
pub(crate) struct SendBudget {
    queued: AtomicUsize,
    limit: usize,
}

impl SendBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            queued: AtomicUsize::new(0),
            limit,
        }
    }

    /// Reserves `len` bytes, or returns `false` if they don't fit. A write
    /// larger than the whole budget is accepted when nothing else is queued,
    /// so it can't block forever.
    pub fn try_reserve(&self, len: usize) -> bool {
        let queued = self.queued.load(Ordering::Acquire);
        if queued > 0 && queued.saturating_add(len) > self.limit {
            return false;
        }
        self.queued.fetch_add(len, Ordering::AcqRel);
        true
    }

    pub fn release(&self, len: usize) {
        self.queued.fetch_sub(len, Ordering::AcqRel);
    }

    pub fn queued(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Bytes the driver has delivered for a receive stream that the owner hasn't
/// consumed yet.
///
/// The driver stops reading once the budget is full, which lets QUIC flow
/// control slow the peer down. Before stopping it sets `paused`, and whoever
/// clears `paused` while bytes are free is responsible for resuming the reads.
#[derive(Debug)]
pub(crate) struct RecvBudget {
    unread: AtomicUsize,
    paused: AtomicBool,
    limit: usize,
}

impl RecvBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            unread: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
            limit,
        }
    }

    /// Marks `len` delivered bytes as consumed. Returns `true` if the driver
    /// had paused reading, in which case the caller must send
    /// [`Command::ResumeRead`].
    pub fn consume(&self, len: usize) -> bool {
        self.unread.fetch_sub(len, Ordering::SeqCst);
        self.paused.swap(false, Ordering::SeqCst)
    }

    pub fn unread(&self) -> usize {
        self.unread.load(Ordering::SeqCst)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// How many more bytes the driver may deliver.
    pub(super) fn room(&self) -> usize {
        self.limit.saturating_sub(self.unread.load(Ordering::SeqCst))
    }

    pub(super) fn deliver(&self, len: usize) {
        self.unread.fetch_add(len, Ordering::SeqCst);
    }

    /// Pauses reading. Returns `true` if reading should stop, or `false` if
    /// bytes were freed in the meantime and reading can go on.
    pub(super) fn pause(&self) -> bool {
        self.paused.store(true, Ordering::SeqCst);
        if self.room() == 0 {
            return true;
        }
        self.paused.store(false, Ordering::SeqCst);
        false
    }
}

/// A newly opened or accepted stream. Holds the budgets for the directions
/// this side can use.
#[derive(Debug, Clone)]
pub(crate) struct StreamInfo {
    pub id: StreamId,
    pub dir: Dir,
    pub send: Option<Arc<SendBudget>>,
    pub recv: Option<Arc<RecvBudget>>,
}

/// A request from the owner to the driver.
pub(crate) enum Command<S> {
    /// Starts driving `socket` as a server or client endpoint.
    OpenEndpoint {
        endpoint: EndpointId,
        socket: S,
        setup: Box<EndpointSetup>,
    },
    /// Closes every connection on the endpoint and removes it once they've
    /// drained. A server refuses new connections meanwhile.
    CloseEndpoint { endpoint: EndpointId },
    /// Starts a connection from a client endpoint. `conn` comes from the
    /// driver's [`IdAllocator`].
    Connect {
        endpoint: EndpointId,
        conn: ConnId,
        connect: Connect,
    },
    /// Closes the connection. Queued stream data may be lost: finish streams
    /// and wait for [`Event::SendFinished`] first to deliver it.
    Close {
        conn: ConnId,
        code: AppErrorCode,
        reason: Bytes,
    },
    /// Opens a stream, waiting for the peer's stream limit if needed. Answered
    /// with [`Event::StreamOpened`] carrying the same `request`.
    OpenStream { conn: ConnId, dir: Dir, request: u64 },
    /// Queues data on a send stream. The bytes must already be reserved in
    /// the stream's [`SendBudget`].
    Write {
        conn: ConnId,
        stream: StreamId,
        data: Bytes,
    },
    /// Finishes the send stream once all queued data has been written.
    Finish { conn: ConnId, stream: StreamId },
    /// Abandons the send stream, discarding queued data.
    Reset {
        conn: ConnId,
        stream: StreamId,
        code: AppErrorCode,
    },
    /// Asks the peer to stop sending, discarding unread data.
    Stop {
        conn: ConnId,
        stream: StreamId,
        code: AppErrorCode,
    },
    /// Resumes reading after [`RecvBudget::consume`] returned `true`.
    ResumeRead { conn: ConnId, stream: StreamId },
    /// Sends an unreliable datagram, dropping the oldest queued one if the
    /// send buffer is full.
    SendDatagram { conn: ConnId, data: Bytes },
    /// Stops the driver without closing anything gracefully.
    Shutdown,
}

/// A notification from the driver to its owner.
#[derive(Debug, Clone)]
pub(crate) enum Event {
    /// The endpoint's connections have all drained and its socket is closed.
    EndpointClosed { endpoint: EndpointId },
    /// A server endpoint accepted a connection. The handshake is still in
    /// progress; [`Event::Connected`] follows.
    ConnectionAccepted {
        endpoint: EndpointId,
        conn: ConnId,
        remote: SocketAddr,
    },
    /// The handshake completed.
    Connected { conn: ConnId },
    /// The connection can no longer be used. This is the last event for the
    /// connection, and it's also sent when connecting fails.
    ConnectionLost {
        conn: ConnId,
        reason: ConnectionError,
    },
    /// A stream requested with [`Command::OpenStream`] is open.
    StreamOpened {
        conn: ConnId,
        request: u64,
        stream: StreamInfo,
    },
    /// The peer opened a stream.
    StreamAccepted { conn: ConnId, stream: StreamInfo },
    /// Data received on a stream, in order. Counted against its
    /// [`RecvBudget`].
    StreamData {
        conn: ConnId,
        stream: StreamId,
        chunks: Vec<Bytes>,
    },
    /// The peer finished the stream and all of its data has been delivered.
    StreamFinished { conn: ConnId, stream: StreamId },
    /// The peer reset the stream. Data not yet delivered is lost.
    StreamReset {
        conn: ConnId,
        stream: StreamId,
        code: AppErrorCode,
    },
    /// The peer asked this side to stop sending. Queued data was discarded.
    StreamStopped {
        conn: ConnId,
        stream: StreamId,
        code: AppErrorCode,
    },
    /// Everything written to a finished stream was acknowledged by the peer.
    SendFinished { conn: ConnId, stream: StreamId },
    /// An unreliable datagram arrived.
    Datagram { conn: ConnId, data: Bytes },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_budget_rejects_past_the_limit() {
        let budget = SendBudget::new(10);
        assert!(budget.try_reserve(6));
        assert!(!budget.try_reserve(5));
        assert!(budget.try_reserve(4));
        budget.release(10);
        // An oversized write fits when nothing else is queued.
        assert!(budget.try_reserve(25));
        assert!(!budget.try_reserve(1));
    }

    #[test]
    fn recv_budget_resumes_only_when_paused() {
        let budget = RecvBudget::new(10);
        budget.deliver(4);
        assert!(!budget.consume(4));

        budget.deliver(10);
        assert!(budget.pause());
        assert!(budget.consume(3));
        // The flag was taken, so a second consume doesn't resume again.
        assert!(!budget.consume(1));
        assert_eq!(budget.room(), 4);
        assert!(!budget.pause());
    }
}
