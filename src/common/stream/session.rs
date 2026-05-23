use aeronet_io::{
    Session,
    connection::{Disconnect, Disconnected},
};
use bevy::{
    app::{Plugin, PostUpdate, PreUpdate, Startup},
    ecs::{
        component::Component,
        entity::Entity,
        observer::On,
        query::With,
        system::{Commands, Query},
        world::World,
    },
    log::{tracing, warn},
};
use std::time::Instant;

use crate::common::{
    connection::disconnect::ConnectionDisconnectReason,
    stream::{receive::QuicReceiveStream, send::QuicSendStream},
};

const MIN_MTU: usize = 1200;
const MAX_PACKET_TRANSFER: usize = 512;
const PACKET_WARN_THRESH: usize = 400;

/// The disconnect code which will be sent by QUIC when a disconnect is called
/// via the [Disconnect](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnect.html)
/// event on any ReceiveStream.
pub const AERONET_DISCONNECT_CODE: u32 = 12345;

/// The component which is added once a stream of any kind has been
/// successfully made.
#[derive(Component, Default)]
#[require(Session::new(Instant::now(), MIN_MTU))]
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
/// The plugin which handles pushing and draining packets to and from the [Session](https://docs.rs/aeronet_io/latest/aeronet_io/struct.Session.html)
/// packet buffers.
pub struct QuicAeronetPacketPlugin;

impl Plugin for QuicAeronetPacketPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(PreUpdate, (aeronet_session_recv, aeronet_session_send));
    }
}

/// The plugin which handles sending and receiving the [Disconnect](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnect.html)
/// and [Disconnected](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnected.html) events.
/// Disconnects triggered via the [Disconnect](https://docs.rs/aeronet_io/latest/aeronet_io/connection/struct.Disconnect.html)
/// event will use the default
pub struct QuicAeronetEventPlugin;

impl Plugin for QuicAeronetEventPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(
            PostUpdate,
            (fire_rec_disconnect_events, fire_send_disconnect_events),
        )
        .add_systems(Startup, add_disconnect_handler);
    }
}

#[tracing::instrument(skip_all)]
fn aeronet_session_recv(
    query: Query<(&mut Session, &mut QuicReceiveStream), With<QuicSession>>,
) {
    let mut buffer = Vec::new();

    for entity in query {
        let (mut session, mut rec) = entity;

        let size = rec.recv_many(&mut buffer, MAX_PACKET_TRANSFER);

        if size >= PACKET_WARN_THRESH {
            #[cfg(feature = "performance-warns")]
            warn!(
                "Packet input is unexpectedly high. If the max transfer rate is exceeded this could cause delays in packet delivery. \nMax: {}\nCurrent: {}",
                MAX_PACKET_TRANSFER, size
            );
        }

        if session.recv.is_empty() {
            buffer.truncate(size);
            std::mem::swap(&mut buffer, &mut session.recv);
        } else {
            #[cfg(feature = "performance-warns")]
            warn!(
                "Session packet buffer is not empty. Are we falling behind on packet processing? Waiting packets: {}",
                session.recv.len()
            );

            session.recv.extend(buffer.drain(..size));
        }
    }
}

#[tracing::instrument(skip_all)]
fn aeronet_session_send(
    query: Query<(&mut Session, &mut QuicSendStream), With<QuicSession>>,
) {
    for entity in query {
        let (mut session, mut send) = entity;
        let parent_id = send.parent_id();

        let res = send.send_many_drain(&mut session.send);

        #[cfg(feature = "performance-warns")]
        if res.is_err() {
            warn!(
                "Drain send unable to fully drain send buffer. Remaining items in buffer: {}\nHas the state of '{}' been corrupted?",
                session.send.len(),
                parent_id
            )
        }
    }
}

fn fire_send_disconnect_events(
    mut cmd: Commands,
    query: Query<(&mut QuicReceiveStream, Entity), With<Session>>,
) {
    for (mut rec, entity) in query {
        if let Some(reason) = rec.get_disconnect_reason() {
            fire_disconnect(&mut cmd, entity, reason);
        }
    }
}

fn fire_rec_disconnect_events(
    mut cmd: Commands,
    query: Query<(&mut QuicSendStream, Entity), With<Session>>,
) {
    for (mut send, entity) in query {
        if let Some(reason) = send.get_disconnect_reason() {
            fire_disconnect(&mut cmd, entity, reason);
        }
    }
}

fn fire_disconnect(
    cmd: &mut Commands,
    entity: Entity,
    reason: ConnectionDisconnectReason,
) {
    cmd.trigger(Disconnected {
        entity,
        reason: reason.into(),
    });
}

fn add_disconnect_handler(world: &mut World) {
    world.add_observer(
        |event: On<Disconnect>,
         rec_query: Query<(&mut QuicReceiveStream, Entity)>,
         send_query: Query<(&mut QuicSendStream, Entity)>| {
            for (mut stream, entity) in rec_query {
                if entity == event.entity {
                    stream.stop_send(AERONET_DISCONNECT_CODE.into());
                }
            }

            for (mut stream, entity) in send_query {
                if entity == event.entity {
                    stream.close();
                }
            }
        },
    );
}
