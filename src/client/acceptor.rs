use bevy::{
    app::{Plugin, Update},
    ecs::{
        entity::Entity,
        query::With,
        system::{Commands, Query},
    },
    log::{error, tracing},
};

use crate::{client::marker::QuicClientMarker, common::connection::QuicConnection};

/// This plugin makes all clients accept all incoming streams and spawns them
/// as components parented to their [QuicConnection]s in the ECS world.
#[derive(Debug)]
pub struct SimpleClientAcceptorPlugin;

// TODO: probably switch this to a single acceptor for both client and server implementations
impl Plugin for SimpleClientAcceptorPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(Update, accept_streams);
    }
}

fn accept_streams(
    mut commands: Commands,
    connection_query: Query<(Entity, &mut QuicConnection), With<QuicClientMarker>>,
) {
    for (connection_entity, mut connection) in connection_query {
        handle_stream_accept(&mut commands, connection_entity, &mut connection);
    }
}

#[tracing::instrument(name = "accept_client_stream", skip_all, fields(parent_id = %connection.parent_id()))]
fn handle_stream_accept(
    commands: &mut Commands,
    connection_entity: Entity,
    connection: &mut QuicConnection,
) {
    match connection.accept_stream() {
        Ok(Some(peer_attempt)) => {
            commands.entity(connection_entity).with_children(|parent| {
                parent.spawn((peer_attempt, QuicClientMarker));
            });
        }
        Err(err) => {
            error!("Error accepting stream from connection: {}", err);
        }
        _ => {}
    }
}
