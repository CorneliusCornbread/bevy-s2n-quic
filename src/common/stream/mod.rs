use aeronet_io::SessionEndpoint;
use bevy::{
    ecs::component::Component,
    prelude::{Deref, DerefMut},
};
use s2n_quic::stream::PeerStream;
use tokio::runtime::Handle;

use crate::common::{
    QuicParentId,
    attempt::{QuicActionAttempt, TaskResult},
    connection::id::ConnectionId,
    orchestrator::handle::OrchestratorHandle,
    stream::{receive::QuicReceiveStream, send::QuicSendStream},
};

pub mod id;
pub mod plugin;
pub mod receive;
pub mod send;
pub mod session;
pub(crate) mod task_state;

/// This is a structure which represents an in progress receive stream.
/// Any pending streams this attempt may hold will be closed
/// if this structure is dropped without being handled.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a full [QuicReceiveStream].
///
/// In the event of a failure, with the `stream-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Deref, DerefMut, Component)]
#[component(storage = "SparseSet")]
#[require(SessionEndpoint)]
pub struct QuicReceiveStreamAttempt(
    QuicActionAttempt<Option<QuicReceiveStream>, ConnectionId>,
);

impl QuicReceiveStreamAttempt {
    pub fn new(
        handle: Handle,
        task: impl TaskResult<Option<QuicReceiveStream>> + 'static + Send + Sync,
        id: ConnectionId,
    ) -> Self {
        Self(QuicActionAttempt::new(handle, task, id))
    }
}

/// This is a structure which represents an in progress send stream.
/// Any pending streams this attempt may hold will be closed
/// if this structure is dropped without being handled.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a full [QuicSendStream].
///
/// In the event of a failure, with the `stream-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Deref, DerefMut, Component)]
#[component(storage = "SparseSet")]
#[require(SessionEndpoint)]
pub struct QuicSendStreamAttempt(QuicActionAttempt<Option<QuicSendStream>, ConnectionId>);

impl QuicSendStreamAttempt {
    pub fn new(
        handle: Handle,
        task: impl TaskResult<Option<QuicSendStream>> + 'static + Send + Sync,
        id: ConnectionId,
    ) -> Self {
        Self(QuicActionAttempt::new(handle, task, id))
    }
}

/// This is a structure which represents an in progress bidirectional stream.
/// Any pending streams this attempt may hold will be closed
/// if this structure is dropped without being handled.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a [QuicSendStream] and a
/// [QuicReceiveStream].
///
/// In the event of a failure, with the `stream-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Deref, DerefMut, Component)]
#[component(storage = "SparseSet")]
#[require(SessionEndpoint)]
pub struct QuicBidirectionalStreamAttempt(
    QuicActionAttempt<Option<(QuicReceiveStream, QuicSendStream)>, ConnectionId>,
);

impl QuicBidirectionalStreamAttempt {
    pub fn new(
        handle: Handle,
        task: impl TaskResult<Option<(QuicReceiveStream, QuicSendStream)>>
        + 'static
        + Send
        + Sync,
        id: ConnectionId,
    ) -> Self {
        Self(QuicActionAttempt::new(handle, task, id))
    }
}

/// This is a structure which represents an in progress peer stream.
/// Any pending streams this attempt may hold will be closed
/// if this structure is dropped without being handled.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a either a [QuicSendStream],
/// [QuicReceiveStream] or both in the case of a bidirectional stream.
///
/// In the event of a failure, with the `stream-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Component, Deref, DerefMut)]
#[component(storage = "SparseSet")]
#[require(SessionEndpoint)]
pub struct QuicPeerStreamAttempt(QuicActionAttempt<Option<QuicPeerStream>, ConnectionId>);

impl QuicPeerStreamAttempt {
    pub fn new(
        handle: Handle,
        task: impl TaskResult<Option<QuicPeerStream>> + 'static + Send + Sync,
        conn_id: ConnectionId,
    ) -> Self {
        Self(QuicActionAttempt::new(handle, task, conn_id))
    }
}

pub enum QuicPeerStream {
    Bidirectional(QuicReceiveStream, QuicSendStream),
    Receive(QuicReceiveStream),
}

impl QuicPeerStream {
    pub fn new(
        runtime: Handle,
        orchestrator: OrchestratorHandle,
        peer_stream: PeerStream,
        conn_id: ConnectionId,
    ) -> Self {
        match peer_stream {
            PeerStream::Bidirectional(bidirectional_stream) => {
                let (rec, send) = bidirectional_stream.split();
                let quic_rec = QuicReceiveStream::new(orchestrator.clone(), rec, conn_id);
                let quic_send = QuicSendStream::new(orchestrator, send, conn_id);

                QuicPeerStream::Bidirectional(quic_rec, quic_send)
            }
            PeerStream::Receive(rec) => {
                let quic_rec = QuicReceiveStream::new(orchestrator, rec, conn_id);

                QuicPeerStream::Receive(quic_rec)
            }
        }
    }
}
