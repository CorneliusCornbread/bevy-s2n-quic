//! Aeronet sessions over QUIC. See [QuicSessionMode] for how packets are
//! carried.

use aeronet_io::{
    AeronetIoPlugin, IoSystems, Session, SessionEndpoint,
    anyhow::anyhow,
    connection::{Disconnect, DisconnectReason, Disconnected, PeerAddr},
    packet::RecvPacket,
};
use bevy::{
    app::{App, Plugin, PostUpdate, PreUpdate},
    ecs::{
        component::Component,
        entity::Entity,
        lifecycle::Add,
        observer::On,
        query::With,
        schedule::IntoScheduleConfigs,
        system::{Commands, Local, Query},
    },
    log::{tracing, warn},
};
use std::{mem, num::Saturating, time::Instant};

use crate::common::{
    connection::{QuicConnection, QuicConnectionAttempt},
    error::{AppErrorCode, SendError},
    router::poll_driver_events,
    stream::{
        QuicBidirectionalStreamAttempt, QuicPeerStreamAttempt, QuicReceiveStreamAttempt,
        QuicSendStreamAttempt,
        framing::{self, FrameDecoder},
        receive::QuicReceiveStream,
        send::QuicSendStream,
    },
};

/// The largest packet a [QuicSessionMode::Streams] session can send. It's
/// also the session's MTU.
pub const STREAM_SESSION_MTU: usize = framing::MAX_PACKET;

/// The disconnect code which will be sent by QUIC when a disconnect is called
/// via the [Disconnect](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnect.html)
/// event on a session.
pub const AERONET_DISCONNECT_CODE: AppErrorCode = AppErrorCode::from_u32(12345);

/// How [QuicAeronetPacketPlugin] carries aeronet packets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QuicSessionMode {
    /// One session per connection, on the [QuicConnection] entity. Each
    /// packet is sent as a QUIC datagram, which is unreliable and unordered
    /// like an aeronet packet, so a lost packet never holds up the others.
    ///
    /// The session's MTU follows the path MTU, and its minimum MTU is what
    /// fits at QUIC's smallest path MTU, a little over 1100 bytes. A packet
    /// larger than the MTU is dropped. If packets are queued faster than the
    /// connection can send them, the oldest are dropped. Both are counted in
    /// [ConnectionStats::datagrams_dropped][crate::common::connection::stats::ConnectionStats::datagrams_dropped].
    #[default]
    Datagrams,
    /// One session per stream, on each stream entity (marked with
    /// [QuicSession]). Each packet is sent on the stream after a 2-byte
    /// length, so packets arrive whole, reliably and in order, but a lost
    /// packet holds up the ones after it. Packets can be up to
    /// [STREAM_SESSION_MTU] bytes.
    ///
    /// If the stream's send buffer is full, the frame's packets are
    /// dropped.
    Streams,
}

/// The marker added to a stream entity once its stream has opened. In
/// [QuicSessionMode::Streams], it gets an aeronet
/// [Session](https://docs.rs/aeronet_io/latest/aeronet_io/struct.Session.html)
/// too.
#[derive(Component, Default)]
pub struct QuicSession;

// NOTE:
// Aeronet_IO is being left as a hard dependency as of now,
// the cost of having to convert to Aeronet structures is
// far too much of a maintenance burden due to unsafe requirements
// for efficient vec transmutation. We're using std::mem::swap
// to efficiency move buffers of data around and doing that
// efficiently when Aeronet is not a hard dep is a nightmare.
//
// Dropping Aeronet from your app is as simple as not including
// the Aeronet plugin in your app.
//
// The IO layer is a couple hundred lines of code, you may consume
// mine weiner if this is upsetting to you. - Cornelius
/// The plugin which adds aeronet
/// [Session](https://docs.rs/aeronet_io/latest/aeronet_io/struct.Session.html)s
/// and moves packets between them and QUIC, in the way set by
/// [QuicSessionMode].
///
/// Received packets are pushed to `Session::recv` in
/// [IoSystems::Poll](https://docs.rs/aeronet_io/latest/aeronet_io/enum.IoSystems.html),
/// and `Session::send` is drained in `IoSystems::Flush`. Adds aeronet's
/// `AeronetIoPlugin` if it isn't already added.
#[derive(Debug, Default)]
pub struct QuicAeronetPacketPlugin {
    mode: QuicSessionMode,
}

impl QuicAeronetPacketPlugin {
    pub fn new(mode: QuicSessionMode) -> Self {
        Self { mode }
    }

    pub fn mode(&self) -> QuicSessionMode {
        self.mode
    }
}

impl Plugin for QuicAeronetPacketPlugin {
    fn build(&self, app: &mut App) {
        add_aeronet_io(app);

        match self.mode {
            QuicSessionMode::Datagrams => {
                app.add_observer(add_session_endpoint::<QuicConnectionAttempt>)
                    .add_observer(open_datagram_session)
                    .add_systems(
                        PreUpdate,
                        datagram_session_recv
                            .in_set(IoSystems::Poll)
                            .after(poll_driver_events),
                    )
                    .add_systems(
                        PostUpdate,
                        datagram_session_send.in_set(IoSystems::Flush),
                    );
            }
            QuicSessionMode::Streams => {
                app.add_observer(add_session_endpoint::<QuicBidirectionalStreamAttempt>)
                    .add_observer(add_session_endpoint::<QuicSendStreamAttempt>)
                    .add_observer(add_session_endpoint::<QuicReceiveStreamAttempt>)
                    .add_observer(add_session_endpoint::<QuicPeerStreamAttempt>)
                    .add_observer(open_stream_session)
                    .add_systems(
                        PreUpdate,
                        stream_session_recv
                            .in_set(IoSystems::Poll)
                            .after(poll_driver_events),
                    )
                    .add_systems(PostUpdate, stream_session_send.in_set(IoSystems::Flush));
            }
        }
    }
}

/// The plugin which handles sending and receiving the [Disconnect](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnect.html)
/// and [Disconnected](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnected.html) events.
///
/// A [Disconnect](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnect.html)
/// on a connection closes it with [AERONET_DISCONNECT_CODE] and the
/// disconnect reason, which the peer receives as
/// `DisconnectReason::ByPeer`. On a stream, it stops the receiving half and
/// finishes the sending half; streams can't carry a reason.
///
/// A stream session is disconnected once either half of its stream ends.
/// Connection sessions are disconnected by
/// [DisconnectHandlerPlugin][crate::common::plugin::DisconnectHandlerPlugin].
pub struct QuicAeronetEventPlugin;

impl Plugin for QuicAeronetEventPlugin {
    fn build(&self, app: &mut App) {
        add_aeronet_io(app);

        app.add_systems(PostUpdate, fire_stream_disconnect_events)
            .add_observer(on_disconnect);
    }
}

fn add_aeronet_io(app: &mut App) {
    if !app.is_plugin_added::<AeronetIoPlugin>() {
        app.add_plugins(AeronetIoPlugin);
    }
}

/// Marks an entity as a session that's still connecting.
fn add_session_endpoint<C: Component>(add: On<Add, C>, mut commands: Commands) {
    commands.entity(add.entity).try_insert(SessionEndpoint);
}

// ─── Datagram sessions ───────────────────────────────────────────────────────

fn open_datagram_session(
    add: On<Add, QuicConnection>,
    connections: Query<&QuicConnection>,
    mut commands: Commands,
) {
    let entity = add.entity;
    let Ok(connection) = connections.get(entity) else {
        return;
    };

    let Some(min_mtu) = connection.min_datagram_size() else {
        warn!(
            "{} doesn't accept QUIC datagrams, so it can't have an aeronet session",
            connection.id()
        );
        connection.close(AERONET_DISCONNECT_CODE);
        commands.trigger(Disconnected {
            entity,
            reason: DisconnectReason::by_error(anyhow!(
                "the peer doesn't accept QUIC datagrams"
            )),
        });
        return;
    };

    let mut session = Session::new(Instant::now(), min_mtu);
    if let Some(mtu) = connection.max_datagram_size() {
        // Can't fail: the minimum is what fits at QUIC's smallest path MTU.
        let _ = session.set_mtu(mtu);
    }
    commands
        .entity(entity)
        .try_insert((session, PeerAddr(connection.remote_addr())));
}

#[tracing::instrument(skip_all)]
fn datagram_session_recv(sessions: Query<(&mut Session, &mut QuicConnection)>) {
    for (mut session, mut connection) in sessions {
        if let Some(mtu) = connection.max_datagram_size()
            && mtu != session.mtu()
        {
            let _ = session.set_mtu(mtu);
        }

        if !connection.has_datagrams() {
            continue;
        }
        let session = &mut *session;
        let start = session.recv.len();
        connection.recv_datagrams(&mut session.recv, usize::MAX);

        let received = &session.recv[start..];
        session.stats.packets_recv += Saturating(received.len());
        session.stats.bytes_recv +=
            Saturating(received.iter().map(|p| p.payload.len()).sum());
    }
}

#[tracing::instrument(skip_all)]
fn datagram_session_send(sessions: Query<(&mut Session, &QuicConnection)>) {
    for (mut session, connection) in sessions {
        if session.send.is_empty() {
            continue;
        }
        let packets = mem::take(&mut session.send);
        if !connection.is_open() {
            continue;
        }

        session.stats.packets_sent += Saturating(packets.len());
        session.stats.bytes_sent += Saturating(packets.iter().map(|p| p.len()).sum());
        connection.send_datagrams(packets);
    }
}

// ─── Stream sessions ─────────────────────────────────────────────────────────

fn open_stream_session(add: On<Add, QuicSession>, mut commands: Commands) {
    commands.entity(add.entity).try_insert((
        Session::new(Instant::now(), STREAM_SESSION_MTU),
        FrameDecoder::default(),
    ));
}

#[tracing::instrument(skip_all)]
fn stream_session_recv(
    sessions: Query<
        (&mut Session, &mut QuicReceiveStream, &mut FrameDecoder),
        With<QuicSession>,
    >,
    mut chunks: Local<Vec<RecvPacket>>,
) {
    for (mut session, mut stream, mut decoder) in sessions {
        if !stream.has_unread() {
            continue;
        }
        // The stream's receive buffer bounds how much this reads.
        stream.recv_many(&mut chunks, usize::MAX);

        let session = &mut *session;
        for chunk in chunks.drain(..) {
            decoder.push(chunk.payload, |payload| {
                session.stats.packets_recv += Saturating(1);
                session.stats.bytes_recv += Saturating(payload.len());
                session.recv.push(RecvPacket {
                    recv_at: chunk.recv_at,
                    payload,
                });
            });
        }
    }
}

#[tracing::instrument(skip_all)]
fn stream_session_send(
    sessions: Query<(&mut Session, &mut QuicSendStream), With<QuicSession>>,
) {
    for (mut session, mut stream) in sessions {
        if session.send.is_empty() {
            continue;
        }
        let packets = mem::take(&mut session.send);
        let encoded = framing::encode(&packets);

        #[cfg(feature = "performance-warns")]
        if encoded.oversized > 0 {
            warn!(
                "{} packets on {} were larger than {STREAM_SESSION_MTU} bytes and were dropped",
                encoded.oversized,
                stream.id()
            );
        }

        match stream.send(encoded.data) {
            Ok(()) => {
                session.stats.packets_sent += Saturating(encoded.packets);
                session.stats.bytes_sent += Saturating(encoded.payload);
            }
            Err(SendError::Full(_)) => {
                #[cfg(feature = "performance-warns")]
                warn!(
                    "Send stream for '{}' is full, {} packets were dropped. Is the network keeping up?",
                    stream.parent_id(),
                    encoded.packets
                );
            }
            Err(SendError::Closed(_)) => {}
        }
    }
}

// ─── Disconnects ─────────────────────────────────────────────────────────────

/// Disconnects a stream session once either half of its stream has ended.
fn fire_stream_disconnect_events(
    mut commands: Commands,
    query: Query<
        (
            Entity,
            Option<&mut QuicReceiveStream>,
            Option<&mut QuicSendStream>,
        ),
        With<Session>,
    >,
) {
    for (entity, recv, send) in query {
        let reason = recv
            .and_then(|mut recv| recv.get_disconnect_reason())
            .or_else(|| send.and_then(|mut send| send.get_disconnect_reason()));
        if let Some(reason) = reason {
            commands.trigger(Disconnected {
                entity,
                reason: reason.into(),
            });
        }
    }
}

fn on_disconnect(
    disconnect: On<Disconnect>,
    connections: Query<&QuicConnection>,
    mut receivers: Query<&mut QuicReceiveStream>,
    mut senders: Query<&mut QuicSendStream>,
) {
    let entity = disconnect.entity;

    if let Ok(connection) = connections.get(entity) {
        connection.close_with_reason(AERONET_DISCONNECT_CODE, disconnect.reason.clone());
    }
    if let Ok(mut stream) = receivers.get_mut(entity) {
        stream.stop_send(AERONET_DISCONNECT_CODE);
    }
    if let Ok(mut stream) = senders.get_mut(entity) {
        stream.close();
    }
}
