use aeronet_io::packet::RecvPacket;
use bevy::{
    ecs::{component::Component, lifecycle::HookContext, world::DeferredWorld},
    log::error,
};
use std::{collections::VecDeque, fmt, sync::Arc};

use crate::{
    common::{
        QuicParentId,
        connection::disconnect::ConnectionDisconnectReason,
        error::AppErrorCode,
        router::{self, Half, Notice, RecvDelivery},
        runtime::Link,
        stream::id::StreamId,
    },
    driver::{Command, ConnId, RecvBudget, StreamId as DriverStreamId},
};

/// The receiving half of a QUIC stream.
///
/// Data is delivered once per frame, in
/// [IoSystems::Poll](https://docs.rs/aeronet_io/latest/aeronet_io/enum.IoSystems.html).
/// At most a fixed number of unread bytes are buffered; past that, the
/// stream stops reading and QUIC flow control slows the peer down until the
/// data is read.
///
/// Dropping the component asks the peer to stop sending.
#[derive(Component)]
#[component(on_insert, on_discard)]
pub struct QuicReceiveStream {
    link: Link,
    conn: ConnId,
    stream: DriverStreamId,
    budget: Arc<RecvBudget>,
    queue: VecDeque<RecvPacket>,
    /// Why the stream ended. It's reported once `queue` is empty.
    end: Option<ConnectionDisconnectReason>,
    stream_id: StreamId,
    error_logged: bool,
}

impl fmt::Debug for QuicReceiveStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicReceiveStream")
            .field("id", &self.stream_id)
            .field("open", &self.is_open())
            .field("unread_packets", &self.queue.len())
            .finish_non_exhaustive()
    }
}

impl QuicReceiveStream {
    pub(crate) fn new(
        link: Link,
        conn: ConnId,
        stream: DriverStreamId,
        stream_id: StreamId,
        budget: Arc<RecvBudget>,
    ) -> Self {
        Self {
            link,
            conn,
            stream,
            budget,
            queue: VecDeque::new(),
            end: None,
            stream_id,
            error_logged: false,
        }
    }

    fn on_insert(world: DeferredWorld, context: HookContext) {
        router::bind::<Self>(world, context);
    }

    fn on_discard(world: DeferredWorld, context: HookContext) {
        router::unbind::<Self>(world, context);
    }

    pub(crate) fn key(&self) -> (ConnId, DriverStreamId) {
        (self.conn, self.stream)
    }

    pub(crate) fn deliver(&mut self, delivery: RecvDelivery) {
        let end = match delivery {
            RecvDelivery::Data(chunks, recv_at) => {
                // Data still in flight when the stream was stopped.
                if self.end.is_none() {
                    self.queue.extend(
                        chunks
                            .into_iter()
                            .map(|payload| RecvPacket { recv_at, payload }),
                    );
                }
                return;
            }
            RecvDelivery::Finished => ConnectionDisconnectReason::PeerClosed,
            RecvDelivery::Reset(code) => ConnectionDisconnectReason::Reset(code),
            RecvDelivery::Lost(reason) => {
                ConnectionDisconnectReason::ConnectionError(reason)
            }
        };
        self.end.get_or_insert(end);
    }

    /// Releases `bytes` from the receive buffer, resuming reads if they were
    /// paused.
    fn consumed(&mut self, bytes: usize) {
        if bytes > 0 && self.budget.consume(bytes) && self.end.is_none() {
            self.link.send(Command::ResumeRead {
                conn: self.conn,
                stream: self.stream,
            });
        }
    }

    /// Receives a single packet from the QUIC stream.
    pub fn recv(&mut self) -> Option<RecvPacket> {
        let packet = self.queue.pop_front()?;
        self.consumed(packet.payload.len());
        Some(packet)
    }

    /// Receive up to `limit` packets that are already waiting and push them
    /// to the given buffer for reading.
    ///
    /// Never blocks. Returns the number of packets pushed, which is `0` when
    /// no data is waiting or the stream is closed and drained. Use
    /// [`Self::is_open`] to tell those apart.
    pub fn recv_many(&mut self, buffer: &mut Vec<RecvPacket>, limit: usize) -> usize {
        let count = limit.min(self.queue.len());
        let start = buffer.len();
        buffer.extend(self.queue.drain(..count));

        let bytes = buffer[start..].iter().map(|p| p.payload.len()).sum();
        self.consumed(bytes);
        count
    }

    /// Returns `true` until the stream has ended and all of its data has been
    /// read.
    pub fn is_open(&self) -> bool {
        self.end.is_none() || !self.queue.is_empty()
    }

    /// Gets the disconnect reason once the stream has ended and all of its
    /// data has been read. Returns `None` until then.
    pub fn get_disconnect_reason(&mut self) -> Option<ConnectionDisconnectReason> {
        if self.queue.is_empty() {
            self.end.clone()
        } else {
            None
        }
    }

    /// Asks the peer to stop sending, with `err_code`, and discards any data
    /// that hasn't been read.
    pub fn stop_send(&mut self, err_code: AppErrorCode) {
        if self.end.is_some() {
            return;
        }

        self.link.send(Command::Stop {
            conn: self.conn,
            stream: self.stream,
            code: err_code,
        });
        self.end = Some(ConnectionDisconnectReason::UserClosed);
        self.queue.clear();
    }

    /// Logs why the stream closed, once, if it closed because of an error.
    pub fn log_outstanding_errors(&mut self) {
        if let Some(reason) = &self.end
            && !self.error_logged
            && !matches!(
                reason,
                ConnectionDisconnectReason::UserClosed
                    | ConnectionDisconnectReason::PeerClosed
            )
        {
            self.error_logged = true;
            error!(
                "Receiver ID: {}, encountered error:\n{:?}",
                self.stream_id, reason
            );
        }
    }

    /// Gets the ID information for the parent client or server for this stream
    pub fn parent_id(&self) -> QuicParentId {
        self.stream_id.parent_id()
    }

    /// Gets the the full ID information for this stream.
    pub fn id(&self) -> StreamId {
        self.stream_id
    }
}

impl Drop for QuicReceiveStream {
    fn drop(&mut self) {
        if self.end.is_none() {
            self.link.send(Command::Stop {
                conn: self.conn,
                stream: self.stream,
                code: AppErrorCode::UNKNOWN,
            });
        }
        self.link.notify(Notice::HalfReleased {
            conn: self.conn,
            stream: self.stream,
            half: Half::Recv,
        });
    }
}
