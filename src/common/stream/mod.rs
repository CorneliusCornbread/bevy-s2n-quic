use bevy::{
    ecs::component::Component,
    prelude::{Deref, DerefMut},
};

use crate::common::{
    attempt::{OpenRequest, QuicActionAttempt},
    connection::id::ConnectionId,
    stream::{receive::QuicReceiveStream, send::QuicSendStream},
};

pub(crate) mod framing;
pub mod id;
pub mod plugin;
pub mod receive;
pub mod send;
pub mod session;

/// This is a structure which represents an accepted receive stream that
/// hasn't been spawned yet.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a full [QuicReceiveStream].
///
/// In the event of a failure, with the `stream-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Deref, DerefMut, Component)]
#[component(storage = "SparseSet")]
pub struct QuicReceiveStreamAttempt(
    QuicActionAttempt<Option<QuicReceiveStream>, ConnectionId>,
);

impl QuicReceiveStreamAttempt {
    pub(crate) fn new_ready(stream: Option<QuicReceiveStream>, id: ConnectionId) -> Self {
        Self(QuicActionAttempt::new_ready(stream, id))
    }
}

/// This is a structure which represents an in progress send stream.
/// The stream is closed if this is dropped before it opens.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a full [QuicSendStream].
///
/// In the event of a failure, with the `stream-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Deref, DerefMut, Component)]
#[component(storage = "SparseSet")]
pub struct QuicSendStreamAttempt(QuicActionAttempt<Option<QuicSendStream>, ConnectionId>);

impl QuicSendStreamAttempt {
    pub(crate) fn new_open(request: OpenRequest, id: ConnectionId) -> Self {
        Self(QuicActionAttempt::new_open(request, id))
    }
}

/// This is a structure which represents an in progress bidirectional stream.
/// The stream is closed if this is dropped before it opens.
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
pub struct QuicBidirectionalStreamAttempt(
    QuicActionAttempt<Option<(QuicReceiveStream, QuicSendStream)>, ConnectionId>,
);

impl QuicBidirectionalStreamAttempt {
    pub(crate) fn new_open(request: OpenRequest, id: ConnectionId) -> Self {
        Self(QuicActionAttempt::new_open(request, id))
    }

    pub(crate) fn new_ready(
        streams: Option<(QuicReceiveStream, QuicSendStream)>,
        id: ConnectionId,
    ) -> Self {
        Self(QuicActionAttempt::new_ready(streams, id))
    }
}

/// This is a structure which represents an accepted stream that hasn't been
/// spawned yet.
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
pub struct QuicPeerStreamAttempt(QuicActionAttempt<Option<QuicPeerStream>, ConnectionId>);

impl QuicPeerStreamAttempt {
    pub(crate) fn new_ready(
        stream: Option<QuicPeerStream>,
        conn_id: ConnectionId,
    ) -> Self {
        Self(QuicActionAttempt::new_ready(stream, conn_id))
    }
}

/// A stream the peer opened.
pub enum QuicPeerStream {
    Bidirectional(QuicReceiveStream, QuicSendStream),
    Receive(QuicReceiveStream),
}
