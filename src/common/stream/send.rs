use bevy::{
    ecs::{component::Component, lifecycle::HookContext, world::DeferredWorld},
    log::error,
};
use bytes::Bytes;
use std::{fmt, sync::Arc};

use crate::{
    common::{
        QuicParentId,
        connection::disconnect::ConnectionDisconnectReason,
        error::SendError,
        router::{self, Half, Notice, SendDelivery},
        runtime::Link,
        stream::id::StreamId,
    },
    driver::{Command, ConnId, SendBudget, StreamId as DriverStreamId},
};

/// The sending half of a QUIC stream.
///
/// Data passed to [Self::send] is queued for the network thread, which sends
/// it as fast as flow and congestion control allow. At most a fixed number of
/// bytes can be queued; past that, `send` returns [SendError::Full].
///
/// The stream's state is mirrored from the network thread once per frame, in
/// [IoSystems::Poll](https://docs.rs/aeronet_io/latest/aeronet_io/enum.IoSystems.html),
/// so [Self::is_open] can be up to one frame out of date.
///
/// Dropping the component finishes the stream: data already queued is still
/// sent.
#[derive(Component)]
#[component(on_insert, on_discard)]
pub struct QuicSendStream {
    link: Link,
    conn: ConnId,
    stream: DriverStreamId,
    budget: Arc<SendBudget>,
    state: SendState,
    stream_id: StreamId,
    error_logged: bool,
}

enum SendState {
    Open,
    /// [QuicSendStream::close] was called; queued data is still being sent.
    Finishing,
    Closed(ConnectionDisconnectReason),
}

impl fmt::Debug for QuicSendStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicSendStream")
            .field("id", &self.stream_id)
            .field("open", &self.is_open())
            .field("queued", &self.budget.queued())
            .finish_non_exhaustive()
    }
}

impl QuicSendStream {
    pub(crate) fn new(
        link: Link,
        conn: ConnId,
        stream: DriverStreamId,
        stream_id: StreamId,
        budget: Arc<SendBudget>,
    ) -> Self {
        Self {
            link,
            conn,
            stream,
            budget,
            state: SendState::Open,
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

    pub(crate) fn deliver(&mut self, delivery: SendDelivery) {
        if matches!(self.state, SendState::Closed(_)) {
            return;
        }

        let reason = match delivery {
            SendDelivery::Finished => ConnectionDisconnectReason::UserClosed,
            SendDelivery::Stopped(code) => ConnectionDisconnectReason::Reset(code),
            SendDelivery::Lost(reason) => {
                ConnectionDisconnectReason::ConnectionError(reason)
            }
        };
        self.state = SendState::Closed(reason);
    }

    /// Finishes the stream once the data already queued with [Self::send]
    /// has been sent. Later `send` calls return [SendError::Closed].
    ///
    /// The stream closes, with [ConnectionDisconnectReason::UserClosed], once
    /// the peer has acknowledged all of the data.
    ///
    /// Returns `None` if the stream is already closed.
    pub fn close(&mut self) -> Option<()> {
        match self.state {
            SendState::Open => {
                self.link.send(Command::Finish {
                    conn: self.conn,
                    stream: self.stream,
                });
                self.state = SendState::Finishing;
                Some(())
            }
            SendState::Finishing => Some(()),
            SendState::Closed(_) => None,
        }
    }

    /// Does nothing: data is sent as fast as flow and congestion control
    /// allow. Returns `None` if the stream is closed.
    #[deprecated(note = "Data is always sent as soon as possible, so this does nothing.")]
    pub fn flush(&mut self) -> Option<()> {
        self.is_open().then_some(())
    }

    /// Returns `true` until the stream closes: the peer acknowledged
    /// everything after [Self::close], stopped the stream, or the connection
    /// closed.
    pub fn is_open(&self) -> bool {
        !matches!(self.state, SendState::Closed(_))
    }

    /// Queues `data` to be sent.
    ///
    /// Returns [SendError::Full] if too many bytes are already queued, and
    /// [SendError::Closed] if the stream is closed or closing. A single
    /// write larger than the whole buffer is accepted when nothing else is
    /// queued.
    pub fn send(&mut self, data: Bytes) -> Result<(), SendError<Bytes>> {
        if !matches!(self.state, SendState::Open) {
            return Err(SendError::Closed(data));
        }
        if !self.budget.try_reserve(data.len()) {
            return Err(SendError::Full(data));
        }

        self.link.send(Command::Write {
            conn: self.conn,
            stream: self.stream,
            data,
        });
        Ok(())
    }

    /// Take a vector of bytes and send bytes until an error is hit
    /// or until the vector is emptied.
    pub fn send_many_drain(
        &mut self,
        data: &mut Vec<Bytes>,
    ) -> Result<(), SendError<Bytes>> {
        let mut sent = 0;
        let mut result = Ok(());

        for item in data.iter() {
            result = self.send(item.clone());
            if result.is_err() {
                break;
            }
            sent += 1;
        }

        data.drain(..sent);
        result
    }

    /// Logs why the stream closed, once, if it closed because of an error.
    pub fn log_outstanding_errors(&mut self) {
        if let SendState::Closed(reason) = &self.state
            && !self.error_logged
            && !matches!(reason, ConnectionDisconnectReason::UserClosed)
        {
            self.error_logged = true;
            error!(
                "Sender ID: {}, encountered error:\n{:?}",
                self.stream_id, reason
            );
        }
    }

    /// Gets the disconnect reason if the stream has closed.
    /// Returns `None` if the stream is still open.
    pub fn get_disconnect_reason(&mut self) -> Option<ConnectionDisconnectReason> {
        match &self.state {
            SendState::Closed(reason) => Some(reason.clone()),
            _ => None,
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

impl Drop for QuicSendStream {
    fn drop(&mut self) {
        if matches!(self.state, SendState::Open) {
            self.link.send(Command::Finish {
                conn: self.conn,
                stream: self.stream,
            });
        }
        self.link.notify(Notice::HalfReleased {
            conn: self.conn,
            stream: self.stream,
            half: Half::Send,
        });
    }
}
