use bevy::{
    app::{Plugin, Update},
    ecs::{
        entity::Entity,
        system::{Commands, Query, Res},
    },
    log::{error, info, tracing},
};

use crate::common::{
    attempt::QuicActionError,
    connection::{QuicConnection, QuicConnectionAttempt},
    runtime::TokioRuntime,
};

#[derive(Debug)]
pub struct ConnectionAttemptPlugin;

impl Plugin for ConnectionAttemptPlugin {
    fn build(&self, app: &mut bevy::app::App) {
        app.add_systems(Update, handle_connection_attempts);
    }
}

#[tracing::instrument(skip_all)]
fn handle_connection_attempts(
    mut commands: Commands,
    runtime: Res<TokioRuntime>,
    query: Query<(Entity, &mut QuicConnectionAttempt)>,
) {
    let handle_ref = runtime.handle();
    let orchestrator = runtime.orchestrator();

    for entity_bundle in query {
        let (entity, mut attempt) = entity_bundle;
        let parent_id = attempt.id();

        let res = attempt.attempt_result();

        if let Err(e) = res {
            match e {
                QuicActionError::Pending => {
                    continue;
                }
                QuicActionError::Consumed => {
                    info!(
                        "Already consumed connection attempt hasn't been cleaned up: {entity}"
                    );
                }
                QuicActionError::ConnectionFailed(error) => {
                    error!("Error handling connection attempt: {:?}", error)
                }
                QuicActionError::Crashed(ref join_error) => {
                    error!("Error joining connection attempt: {:?}", join_error)
                }
            }

            let mut error_entity = commands.entity(entity);

            #[cfg(feature = "connection-errors")]
            {
                use {
                    crate::common::attempt::QuicActionErrorComponent,
                    std::time::SystemTime,
                };

                let err_comp =
                    QuicActionErrorComponent::new(e, SystemTime::now(), parent_id);
                let err_bundle = err_comp;

                error_entity.insert(err_bundle);
            }

            error_entity.remove::<QuicConnectionAttempt>();

            continue;
        }

        let conn = res.unwrap();
        let quic_conn = QuicConnection::new(
            handle_ref.clone(),
            orchestrator.clone(),
            conn,
            parent_id,
        );
        let id = quic_conn.id();
        info!("New connection entity with {id}");

        commands
            .entity(entity)
            .remove::<QuicConnectionAttempt>()
            .insert(quic_conn);
    }
}
