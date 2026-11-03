use bevy::ecs::component::Component;
use std::{sync::Arc, time::SystemTime};
use thiserror::Error as ThisError;

use crate::{
    common::{
        QuicParentId,
        connection::QuicConnection,
        error::ConnectionError,
        router::{ConnDelivery, Notice, OpenResult, StreamObjects},
        runtime::Link,
    },
    driver::ConnId,
};

/// An action that completes on the network thread, such as connecting or
/// opening a stream. Check it with
/// [attempt_result][QuicActionAttempt::attempt_result()].
///
/// Attempts are resolved once per frame, in
/// [IoSystems::Poll](https://docs.rs/aeronet_io/latest/aeronet_io/enum.IoSystems.html).
pub struct QuicActionAttempt<T, I>
where
    I: Copy,
{
    state: AttemptState<T>,
    id: I,
}

enum AttemptState<T> {
    /// A client connection that hasn't finished its handshake.
    Connecting(Box<QuicConnection>),
    /// A stream open request the driver hasn't answered.
    Opening(OpenRequest),
    Ready(Result<T, QuicActionError>),
    /// The result was taken. Holds what to return from now on.
    Done(QuicActionError),
}

impl<T, I> QuicActionAttempt<T, I>
where
    I: Copy,
{
    pub(crate) fn new_ready(value: T, id: I) -> Self {
        Self {
            state: AttemptState::Ready(Ok(value)),
            id,
        }
    }

    pub(crate) fn new_open(request: OpenRequest, id: I) -> Self {
        Self {
            state: AttemptState::Opening(request),
            id,
        }
    }

    /// Attempt to get the result
    pub fn attempt_result(&mut self) -> Result<T, QuicActionError> {
        match &self.state {
            AttemptState::Connecting(_) | AttemptState::Opening(_) => {
                return Err(QuicActionError::Pending);
            }
            AttemptState::Done(error) => return Err(error.clone()),
            AttemptState::Ready(_) => {}
        }

        let AttemptState::Ready(result) = std::mem::replace(
            &mut self.state,
            AttemptState::Done(QuicActionError::Consumed),
        ) else {
            unreachable!("checked above");
        };
        if let Err(error) = &result {
            self.state = AttemptState::Done(error.clone());
        }
        result
    }

    pub fn id(&self) -> I {
        self.id
    }

    /// The open request this attempt is waiting on, if any.
    pub(crate) fn opening(&self) -> Option<(ConnId, u64)> {
        match &self.state {
            AttemptState::Opening(request) => Some((request.conn, request.request)),
            _ => None,
        }
    }

    fn resolve(&mut self, result: Result<T, QuicActionError>) {
        if let AttemptState::Opening(request) = &mut self.state {
            request.armed = false;
        }
        self.state = AttemptState::Ready(result);
    }
}

impl<I: Copy> QuicActionAttempt<QuicConnection, I> {
    pub(crate) fn new_connect(conn: QuicConnection, id: I) -> Self {
        Self {
            state: AttemptState::Connecting(Box::new(conn)),
            id,
        }
    }

    /// The connection this attempt is waiting on, if any.
    pub(crate) fn connecting(&self) -> Option<ConnId> {
        match &self.state {
            AttemptState::Connecting(conn) => Some(conn.conn()),
            _ => None,
        }
    }

    /// Applies what arrived for the connection, and resolves the attempt
    /// once it has connected or failed.
    pub(crate) fn deliver(&mut self, deliveries: Vec<ConnDelivery>) {
        let AttemptState::Connecting(conn) = &mut self.state else {
            return;
        };
        for delivery in deliveries {
            conn.deliver(delivery);
        }

        if let Some(reason) = conn.lost_reason() {
            let error = QuicActionError::ConnectionFailed(reason.clone());
            self.resolve(Err(error));
        } else if conn.is_connected() {
            let AttemptState::Connecting(conn) = std::mem::replace(
                &mut self.state,
                AttemptState::Done(QuicActionError::Consumed),
            ) else {
                unreachable!("checked above");
            };
            self.resolve(Ok(*conn));
        }
    }
}

impl<S, I: Copy> QuicActionAttempt<Option<S>, I> {
    /// Resolves an open attempt. `take` picks this attempt's stream out of
    /// the opened objects; any it doesn't take are dropped, which releases
    /// them.
    pub(crate) fn resolve_open(
        &mut self,
        result: OpenResult,
        take: impl FnOnce(StreamObjects) -> Option<S>,
    ) {
        let result = match result {
            OpenResult::Opened(objects) => take(*objects).map(Some).ok_or_else(|| {
                QuicActionError::ConnectionFailed(ConnectionError::Transport(
                    "the stream opened in the wrong direction".to_owned(),
                ))
            }),
            OpenResult::Failed(reason) => Err(QuicActionError::ConnectionFailed(reason)),
        };
        self.resolve(result);
    }
}

/// A stream open request sent to the driver. Dropping it before the stream
/// opens tells the router to release the stream when it does.
pub(crate) struct OpenRequest {
    link: Link,
    conn: ConnId,
    request: u64,
    armed: bool,
}

impl OpenRequest {
    pub(crate) fn new(link: Link, conn: ConnId, request: u64) -> Self {
        Self {
            link,
            conn,
            request,
            armed: true,
        }
    }
}

impl Drop for OpenRequest {
    fn drop(&mut self) {
        if self.armed {
            self.link.notify(Notice::OpenCancelled {
                conn: self.conn,
                request: self.request,
            });
        }
    }
}

/// An enum representing all the ways a [QuicActionAttempt] can fail.
/// See [QuicActionErrorComponent] for more details.
#[derive(Clone, Debug, ThisError)]
#[error("Quic action attempt failed to get a result")]
pub enum QuicActionError {
    #[error("Pending")]
    Pending,
    #[error("Consumed")]
    Consumed,
    #[error("ConnectionFailed: {0}")]
    ConnectionFailed(ConnectionError),
    #[error("Crashed: {0}")]
    Crashed(Arc<dyn std::error::Error + Send + Sync>),
}

/// This is a Bevy component which is added to an entity
/// in the event a [QuicActionAttempt] fails.
///
/// These will only be added to entities when either the
/// `connection-errors` or `stream-errors` feature flags are enabled for
/// [QuicConnectionAttempt][crate::common::connection::QuicConnectionAttempt]
/// related errors or
/// [stream attempts][crate::common::stream]
/// respectively.
#[derive(Component, Debug, Clone)]
pub struct QuicActionErrorComponent {
    error: QuicActionError,
    timestamp: SystemTime,
    parent_id: QuicParentId,
}

impl QuicActionErrorComponent {
    pub fn new(
        error: QuicActionError,
        timestamp: SystemTime,
        parent_id: QuicParentId,
    ) -> Self {
        Self {
            error,
            timestamp,
            parent_id,
        }
    }

    /// Returns the actual [QuicActionError] held by this component.
    pub fn error(&self) -> &QuicActionError {
        &self.error
    }

    /// The [SystemTime] at which this error was received by the sync (Bevy) side.
    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }

    /// Returns the parent client or server which is responsible for this error.
    pub fn parent_id(&self) -> QuicParentId {
        self.parent_id
    }
}
