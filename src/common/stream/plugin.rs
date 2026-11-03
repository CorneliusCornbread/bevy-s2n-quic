use std::ops::DerefMut;

use bevy::{
    app::{Plugin, Update},
    ecs::{
        component::{Component, Mutable},
        entity::Entity,
        system::{Commands, EntityCommands, Query},
    },
    log::{error, info, tracing},
};

use crate::common::{
    attempt::{QuicActionAttempt, QuicActionError},
    connection::id::ConnectionId,
    stream::{
        QuicBidirectionalStreamAttempt, QuicPeerStream, QuicPeerStreamAttempt,
        QuicReceiveStreamAttempt, QuicSendStreamAttempt, session::QuicSession,
    },
};

#[derive(Debug)]
pub struct StreamAttemptPlugin;

impl Plugin for StreamAttemptPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(
            Update,
            (
                handle_bidir_stream_attempt,
                handle_send_stream_attempt,
                handle_rec_stream_attempt,
                handle_peer_stream_attempt,
            ),
        );
    }
}

#[tracing::instrument(skip_all)]
fn handle_bidir_stream_attempt(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicBidirectionalStreamAttempt)>,
) {
    handle_attempts(&mut commands, query, |entity, (rec, send)| {
        info!("Spawning bidirectional stream with {}", rec.id());
        entity.insert((rec, send, QuicSession));
    });
}

#[tracing::instrument(skip_all)]
fn handle_send_stream_attempt(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicSendStreamAttempt)>,
) {
    handle_attempts(&mut commands, query, |entity, send| {
        info!("Spawning send stream with {}", send.id());
        entity.insert((send, QuicSession));
    });
}

#[tracing::instrument(skip_all)]
fn handle_rec_stream_attempt(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicReceiveStreamAttempt)>,
) {
    handle_attempts(&mut commands, query, |entity, rec| {
        info!("Spawning receive stream with {}", rec.id());
        entity.insert((rec, QuicSession));
    });
}

#[tracing::instrument(skip_all)]
fn handle_peer_stream_attempt(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicPeerStreamAttempt)>,
) {
    handle_attempts(
        &mut commands,
        query,
        |entity, peer_stream| match peer_stream {
            QuicPeerStream::Bidirectional(rec, send) => {
                info!("Spawning peer (bidirectional) stream with {}", rec.id());
                entity.insert((rec, send, QuicSession));
            }
            QuicPeerStream::Receive(rec) => {
                info!("Spawning peer (receive) stream with {}", rec.id());
                entity.insert((rec, QuicSession));
            }
        },
    );
}

/// Replaces each resolved attempt with its stream, using `insert`. Failed
/// attempts are removed, and attempts that resolved to no stream are
/// despawned.
fn handle_attempts<A, T>(
    commands: &mut Commands,
    query: Query<(Entity, &mut A)>,
    insert: impl Fn(&mut EntityCommands, T),
) where
    A: Component<Mutability = Mutable>
        + DerefMut<Target = QuicActionAttempt<Option<T>, ConnectionId>>,
{
    for (entity, mut attempt) in query {
        let res = attempt.attempt_result();

        let e = match res {
            Ok(Some(stream)) => {
                let mut entity = commands.entity(entity);
                entity.remove::<A>();
                insert(&mut entity, stream);
                continue;
            }
            Ok(None) => {
                info!("No pending incoming streams, deleting attempt.");
                commands.entity(entity).despawn();
                continue;
            }
            Err(QuicActionError::Pending) => continue,
            Err(e) => e,
        };

        match &e {
            QuicActionError::Consumed => {
                error!("Stream attempt consumed for entity: {:?}", entity)
            }
            QuicActionError::ConnectionFailed(error) => {
                error!("Stream attempt failed: {:?}", error)
            }
            QuicActionError::Crashed(join_error) => {
                error!("Stream attempt crashed: {:?}", join_error)
            }
            QuicActionError::Pending => unreachable!("handled above"),
        }

        let mut error_entity = commands.entity(entity);

        #[cfg(feature = "stream-errors")]
        {
            use {
                crate::common::attempt::QuicActionErrorComponent, std::time::SystemTime,
            };

            let err_comp = QuicActionErrorComponent::new(
                e,
                SystemTime::now(),
                attempt.id().parent_id(),
            );
            error_entity.insert(err_comp);
        }

        error_entity.remove::<A>();
    }
}
