use bevy::ecs::component::Component;
use s2n_quic::connection::Error as ConnectionError;
use std::{error::Error, sync::Arc, time::SystemTime};
use thiserror::Error as ThisError;
use tokio::{
    runtime::Handle,
    sync::oneshot::{self, error::TryRecvError},
    task::JoinHandle,
};

use crate::common::QuicParentId;

/// This is a the trait which allows us to implement
/// async attempt behaviour on any provider (e.g.)
/// a [oneshot::Receiver] or [JoinHandle] have default
/// implementations.
///
/// See [QuicActionAttempt] for more details.
pub trait TaskResult<T> {
    fn resolve_result(&mut self, handle: &Handle) -> Option<Result<T, TaskError>>;
}

impl<T> TaskResult<T> for oneshot::Receiver<Result<T, TaskError>> {
    fn resolve_result(&mut self, _handle: &Handle) -> Option<Result<T, TaskError>> {
        if self.is_empty() {
            return None;
        }

        match self.try_recv() {
            Ok(res) => Some(res),
            Err(e) => match e {
                TryRecvError::Empty => None,
                TryRecvError::Closed => Some(Err(TaskError::TaskFailed(Arc::new(e)))),
            },
        }
    }
}

impl<T> TaskResult<T> for JoinHandle<Result<T, TaskError>> {
    fn resolve_result(&mut self, handle: &Handle) -> Option<Result<T, TaskError>> {
        if !self.is_finished() {
            return None;
        }

        let join = handle.block_on(self);

        match join {
            Ok(res) => Some(res),
            Err(e) => Some(Err(TaskError::TaskFailed(Arc::new(e)))),
        }
    }
}

/// This is the structure which represents the async task
/// being attempted and waited upon via sync polling with
/// [attempt_result][QuicActionAttempt::attempt_result()]
pub struct QuicActionAttempt<T, I>
where
    I: Copy,
{
    runtime: Handle,
    task_res: Box<dyn TaskResult<T> + Send + Sync>,
    /// A flag checking if the action state has returned a success value already
    returned_value: Option<QuicActionError>,
    id: I,
}

impl<T, I> QuicActionAttempt<T, I>
where
    I: Copy,
{
    pub fn new(
        runtime: Handle,
        task: impl TaskResult<T> + 'static + Send + Sync,
        id: I,
    ) -> Self {
        Self {
            runtime,
            task_res: Box::new(task),
            returned_value: None,
            id,
        }
    }

    /// Attempt to get the result
    pub fn attempt_result(&mut self) -> Result<T, QuicActionError> {
        if let Some(ret) = &self.returned_value {
            return Err(ret.clone());
        }

        let value = self.task_res.resolve_result(&self.runtime);

        let Some(res) = value else {
            return Err(QuicActionError::Pending);
        };

        match res {
            Ok(value) => {
                self.returned_value = Some(QuicActionError::Consumed);
                Ok(value)
            }
            Err(e) => match e {
                TaskError::ConnectionFailed(err) => {
                    self.returned_value = Some(QuicActionError::ConnectionFailed(err));
                    Err(self.returned_value.as_ref().unwrap().clone())
                }
                TaskError::TaskFailed(err) => {
                    self.returned_value = Some(QuicActionError::Crashed(Arc::new(err)));
                    Err(self.returned_value.as_ref().unwrap().clone())
                }
            },
        }
    }

    pub fn id(&self) -> I {
        self.id
    }
}

/// An enum representing all the ways a [QuicActionAttempt] can fail.
/// See [QuicActionErrorComponent] for more details.
#[derive(Clone, Debug, ThisError)]
#[error("Quic action attempt failed to get a result")]
pub enum QuicActionError {
    #[error("Pending")]
    Pending,
    #[error("Consumed")]
    Consumed,
    #[error("ConnectionFailed: {0}")]
    ConnectionFailed(ConnectionError),
    #[error("Crashed: {0}")]
    Crashed(Arc<dyn std::error::Error + Send + Sync>),
}

/// An enum representing all the ways a [TaskResult] can fail.
#[derive(Clone, Debug, ThisError)]
#[error("Quic task failed")]
#[non_exhaustive]
pub enum TaskError {
    #[error("ConnectionFailed: {0}")]
    ConnectionFailed(ConnectionError),
    #[error("Crashed: {0}")]
    TaskFailed(Arc<dyn Error + Send + Sync>),
}

impl From<ConnectionError> for TaskError {
    fn from(value: ConnectionError) -> Self {
        Self::ConnectionFailed(value)
    }
}

/// This is a Bevy component which is added to an entity
/// in the event a [QuicActionAttempt] fails.
///
/// These will only be added to entities when either the
/// `connection-errors` or `stream-errors` feature flags are enabled for
/// [QuicConnectionAttempt][crate::common::connection::QuicConnectionAttempt]
/// related errors or
/// [stream attempts][crate::common::stream]
/// respectively.
#[derive(Component, Debug, Clone)]
pub struct QuicActionErrorComponent {
    error: QuicActionError,
    timestamp: SystemTime,
}

impl QuicActionErrorComponent {
    pub fn new(error: QuicActionError, timestamp: SystemTime) -> Self {
        Self { error, timestamp }
    }

    pub fn error(&self) -> &QuicActionError {
        &self.error
    }

    /// The [SystemTime] at which this error was received by the sync (Bevy) side.
    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}
