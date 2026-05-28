use bevy::{
    app::{Plugin, Update},
    ecs::{
        entity::Entity,
        system::{Commands, Query},
    },
    log::{error, info, tracing},
};

use crate::common::{
    attempt::QuicActionError,
    stream::{
        QuicBidirectionalStreamAttempt, QuicPeerStream, QuicPeerStreamAttempt,
        QuicReceiveStreamAttempt, session::QuicSession,
    },
};

#[derive(Debug)]
pub struct StreamAttemptPlugin;

// TODO: create systems for send/receive versions
impl Plugin for StreamAttemptPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(Update, handle_bidir_stream_attempt)
            .add_systems(Update, handle_rec_stream_attempt)
            .add_systems(Update, handle_peer_stream_attempt);
    }
}

#[tracing::instrument(skip_all)]
fn handle_bidir_stream_attempt(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicBidirectionalStreamAttempt)>,
) {
    for entity_bundle in query {
        let (entity, mut attempt) = entity_bundle;

        let res = attempt.attempt_result();

        if let Err(e) = res {
            match &e {
                QuicActionError::Pending => continue,
                QuicActionError::Consumed => {
                    error!("Stream attempt consumed for entity: {:?}", entity)
                }
                QuicActionError::ConnectionFailed(error) => {
                    error!("Stream attempt failed: {:?}", error)
                }
                QuicActionError::Crashed(join_error) => {
                    error!("Stream attempt crashed: {:?}", join_error)
                }
            }

            let mut error_entity = commands.entity(entity);

            #[cfg(feature = "stream-errors")]
            {
                use {
                    crate::common::attempt::QuicActionErrorComponent,
                    std::time::SystemTime,
                };

                let err_comp = QuicActionErrorComponent::new(e, SystemTime::now());
                let err_bundle = (err_comp, *parent_id);

                error_entity.insert(err_bundle);
            }

            error_entity.remove::<QuicBidirectionalStreamAttempt>();

            continue;
        }

        if let Some((rec, send)) = res.unwrap() {
            let id = rec.id();
            info!("Spawning bidirectional stream with {id}");

            commands
                .entity(entity)
                .remove::<QuicBidirectionalStreamAttempt>()
                .insert((rec, send, QuicSession));
        }
        // No new streams, delete attempt
        else {
            info!("No pending incoming streams, deleting attempt.");
            commands.entity(entity).despawn();
        }
    }
}

#[tracing::instrument(skip_all)]
fn handle_rec_stream_attempt(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicReceiveStreamAttempt)>,
) {
    for entity_bundle in query {
        let (entity, mut attempt) = entity_bundle;
        let id = attempt.id();

        let res = attempt.attempt_result();

        if let Err(e) = res {
            match &e {
                QuicActionError::Pending => continue,
                QuicActionError::Consumed => {
                    error!("Stream attempt consumed for entity: {:?}", entity)
                }
                QuicActionError::ConnectionFailed(error) => {
                    error!("Stream attempt failed: {:?}", error)
                }
                QuicActionError::Crashed(join_error) => {
                    error!("Stream attempt crashed: {:?}", join_error)
                }
            }

            let mut error_entity = commands.entity(entity);

            #[cfg(feature = "stream-errors")]
            {
                use {
                    crate::common::attempt::QuicActionErrorComponent,
                    std::time::SystemTime,
                };

                let err_comp = QuicActionErrorComponent::new(e, SystemTime::now());
                let err_bundle = (err_comp, *id);

                error_entity.insert(err_bundle);
            }

            error_entity.remove::<QuicReceiveStreamAttempt>();

            continue;
        }

        if let Some(rec) = res.unwrap() {
            info!("Spawning receive stream with {id}");

            commands
                .entity(entity)
                .remove::<QuicReceiveStreamAttempt>()
                .insert((rec, QuicSession));
        }
        // No new streams, delete attempt
        else {
            info!("No pending incoming streams, deleting attempt.");
            commands.entity(entity).despawn();
        }
    }
}

#[tracing::instrument(skip_all)]
fn handle_peer_stream_attempt(
    mut commands: Commands,
    query: Query<(Entity, &mut QuicPeerStreamAttempt)>,
) {
    for entity_bundle in query {
        let (entity, mut attempt) = entity_bundle;

        let res = attempt.attempt_result();

        if let Err(e) = res {
            match &e {
                QuicActionError::Pending => continue,
                QuicActionError::Consumed => {
                    error!("Stream attempt consumed for entity: {:?}", entity)
                }
                QuicActionError::ConnectionFailed(error) => {
                    error!("Stream attempt failed: {:?}", error)
                }
                QuicActionError::Crashed(join_error) => {
                    error!("Stream attempt crashed: {:?}", join_error)
                }
            }

            let mut error_entity = commands.entity(entity);

            #[cfg(feature = "stream-errors")]
            {
                use {
                    crate::common::attempt::QuicActionErrorComponent,
                    std::time::SystemTime,
                };

                let err_comp = QuicActionErrorComponent::new(e, SystemTime::now());
                let err_bundle = (err_comp, *parent_id);

                error_entity.insert(err_bundle);
            }

            error_entity.remove::<QuicPeerStreamAttempt>();

            continue;
        }

        if let Some(peer_stream) = res.unwrap() {
            let mut attempt_entity = commands.entity(entity);
            attempt_entity.remove::<QuicPeerStreamAttempt>();

            match peer_stream {
                QuicPeerStream::Bidirectional(quic_receive_stream, quic_send_stream) => {
                    let id = quic_receive_stream.id();
                    info!("Spawning peer (bidirectional) stream stream with {id}");
                    attempt_entity.insert((
                        quic_receive_stream,
                        quic_send_stream,
                        QuicSession,
                    ));
                }
                QuicPeerStream::Receive(quic_receive_stream) => {
                    let id = quic_receive_stream.id();
                    info!("Spawning peer (receive) stream stream with {id}");
                    attempt_entity.insert((quic_receive_stream, QuicSession));
                }
            }
        }
        // No new streams, delete attempt
        else {
            info!("No pending incoming streams, deleting attempt.");
            commands.entity(entity).despawn();
        }
    }
}
