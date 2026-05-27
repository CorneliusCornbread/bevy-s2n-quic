use bevy::{
    app::{Plugin, Update},
    ecs::{
        entity::Entity,
        hierarchy::ChildOf,
        query::With,
        system::{Commands, Query},
    },
    log::{error, tracing},
};

use crate::{
    common::connection::QuicConnection,
    server::{QuicServer, marker::QuicServerMarker},
};

/// This plugin makes servers automatically accept all incoming
/// connections and streams and spawns them as components parented
/// to their [QuicServer]s and [QuicConnection]s in the ECS world.
#[derive(Debug)]
pub struct SimpleServerAcceptorPlugin;

impl Plugin for SimpleServerAcceptorPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(Update, accept_connections)
            .add_systems(Update, accept_streams);
    }
}

fn accept_connections(mut commands: Commands, servers: Query<(&mut QuicServer, Entity)>) {
    for (mut server, entity) in servers {
        let res = server.accept_connection();

        if let Err(e) = res {
            error!("Error handling server connection: {}", e);
            continue;
        }

        let conn = res.unwrap();

        match conn {
            super::ConnectionPoll::None => continue,
            super::ConnectionPoll::ServerClosed => continue,
            super::ConnectionPoll::NewConnection(quic_connection) => {
                let bundle = (quic_connection, QuicServerMarker, ChildOf(entity));
                commands.spawn(bundle);
            }
        }
    }
}

fn accept_streams(
    mut commands: Commands,
    connection_query: Query<(Entity, &mut QuicConnection), With<QuicServerMarker>>,
) {
    for (connection_entity, mut connection) in connection_query {
        handle_stream_accept(&mut commands, connection_entity, &mut connection);
    }
}

#[tracing::instrument(name = "accept_server_stream", skip_all, fields(id = %connection.id()))]
fn handle_stream_accept(
    commands: &mut Commands,
    connection_entity: Entity,
    connection: &mut QuicConnection,
) {
    match connection.accept_stream() {
        Ok(Some(peer_attempt)) => {
            commands.entity(connection_entity).with_children(|parent| {
                parent.spawn((peer_attempt, QuicServerMarker));
            });
        }
        Err(err) => {
            error!("Error accepting stream from connection: {}", err);
        }
        _ => {}
    }
}
