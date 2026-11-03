use aeronet_io::{Session, connection::DisconnectReason};
use bevy::{
    app::{Plugin, Update},
    ecs::{
        entity::Entity,
        query::Without,
        system::{Commands, Query},
    },
};

use crate::common::{
    connection::QuicConnection,
    stream::{receive::QuicReceiveStream, send::QuicSendStream},
};

/// A plugin which handles any connection or stream components which have been disconnected.
///
/// Connection disconnects will trigger aeronet's [Disconnected][aeronet_io::connection::Disconnected]
/// event.
///
/// Streams will be despawned without an event firing, except streams that
/// are aeronet sessions (see
/// [QuicSessionMode::Streams][crate::common::stream::session::QuicSessionMode::Streams]):
/// [QuicAeronetEventPlugin][crate::common::stream::session::QuicAeronetEventPlugin]
/// triggers `Disconnected` for those, and aeronet despawns them.
pub struct DisconnectHandlerPlugin;

impl Plugin for DisconnectHandlerPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(
            Update,
            (
                handle_connection_disconnections,
                handle_rec_stream_disconnections,
                handle_send_stream_disconnections,
            ),
        );
    }
}

fn handle_connection_disconnections(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicConnection)>,
) {
    for (entity, mut connection) in query {
        if let Some(reason) = connection.get_disconnect_reason() {
            let disconnect_reason = DisconnectReason::from(reason);
            commands.trigger(aeronet_io::connection::Disconnected {
                entity,
                reason: disconnect_reason,
            });
            commands.entity(entity).try_remove::<QuicConnection>();
        }
    }
}

fn handle_rec_stream_disconnections(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicReceiveStream), Without<Session>>,
) {
    for (entity, mut stream) in query {
        if let Some(_reason) = stream.get_disconnect_reason() {
            commands.entity(entity).try_despawn();
        }
    }
}

fn handle_send_stream_disconnections(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicSendStream), Without<Session>>,
) {
    for (entity, mut stream) in query {
        if let Some(_reason) = stream.get_disconnect_reason() {
            commands.entity(entity).try_despawn();
        }
    }
}
