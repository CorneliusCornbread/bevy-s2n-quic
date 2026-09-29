use bevy::log::error;
use std::{fmt, future::Future, sync::Arc};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
};

use crate::common::{
    connection::disconnect::ConnectionDisconnectReason, status_code::StatusCode,
    task_state::OnceLockState,
};

/// Error code used to close a stream or connection when the [TaskSpawner]
/// refuses to start its task.
pub(crate) const SPAWN_REJECTED_ERROR_CODE: StatusCode = StatusCode::ServiceUnavailable;

/// A long-lived async task driving one connection or stream until it disconnects.
///
/// `run` loops until disconnect and is never dropped mid-await by this crate, so
/// the tasks don't need to be cancellation safe. The future can be spawned
/// directly on Tokio or polled alongside others (for example in a
/// `FuturesUnordered` shard).
pub(crate) trait QuicTask: fmt::Display + Send + 'static {
    /// Drives the task until it disconnects, returning the reason.
    fn run(self) -> impl Future<Output = ConnectionDisconnectReason> + Send + 'static;

    /// Closes the underlying stream or connection when the task could not be
    /// started.
    fn reject(self);
}

/// Why a task stopped without returning a disconnect reason.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum TaskAborted {
    /// The task panicked. The panic message is logged by Tokio's panic hook.
    #[error("The async task panicked.")]
    Panicked,
    /// The task was dropped before it finished, usually because the Tokio
    /// runtime was shut down.
    #[error("The async task was cancelled before it finished.")]
    Cancelled,
}

/// Spawns the long-lived async task behind every connection and stream.
///
/// Each task runs as its own Tokio task. If a task limit is set (see
/// [QuicAsyncPlugin::with_max_tasks][crate::async_plugin::QuicAsyncPlugin::with_max_tasks]),
/// new tasks are rejected once the limit is reached: the stream or connection
/// is closed and reports [ConnectionDisconnectReason::OrchestratorError].
#[derive(Clone, Debug)]
pub struct TaskSpawner {
    handle: Handle,
    limit: Option<Arc<Semaphore>>,
}

impl TaskSpawner {
    /// Creates a spawner that starts tasks on the given runtime, allowing at most
    /// `max_tasks` connection and stream tasks at once (`None` for no limit).
    pub fn new(handle: Handle, max_tasks: Option<usize>) -> Self {
        Self {
            handle,
            limit: max_tasks.map(|n| Arc::new(Semaphore::new(n))),
        }
    }

    /// Returns a handle to the Tokio runtime used for running async tasks.
    pub fn tokio_handle(&self) -> &Handle {
        &self.handle
    }

    /// Spawns the task, setting `state` to the task's disconnect reason once it
    /// exits. If the task limit is reached the task is rejected instead and
    /// `state` is set to [ConnectionDisconnectReason::OrchestratorError].
    ///
    /// Returns `false` if the task was rejected.
    pub(crate) fn spawn<T: QuicTask>(
        &self,
        task: T,
        mut state: OnceLockState<ConnectionDisconnectReason>,
    ) -> bool {
        let permit = match &self.limit {
            None => None,
            Some(limit) => match limit.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(e) => {
                    error!("Unable to start task for {task}, with reason: {e}");
                    let _ = state.set(ConnectionDisconnectReason::OrchestratorError);
                    task.reject();
                    return false;
                }
            },
        };

        self.spawn_guarded(task.run(), state, permit);
        true
    }

    /// Spawns `fut` and always sets `state` when it exits, including when it
    /// panics or is dropped before completing.
    fn spawn_guarded(
        &self,
        fut: impl Future<Output = ConnectionDisconnectReason> + Send + 'static,
        state: OnceLockState<ConnectionDisconnectReason>,
        permit: Option<OwnedSemaphorePermit>,
    ) {
        // Created outside the future so it still fires if the future is
        // dropped without ever being polled.
        let guard = FinalStateGuard(Some(state));

        self.handle.spawn(async move {
            let _permit = permit;
            let reason = fut.await;
            guard.finish(reason);
        });
    }
}

/// Sets the task's final state on drop if [Self::finish] wasn't called.
struct FinalStateGuard(Option<OnceLockState<ConnectionDisconnectReason>>);

impl FinalStateGuard {
    fn finish(mut self, reason: ConnectionDisconnectReason) {
        if let Some(mut state) = self.0.take() {
            let _ = state.set(reason);
        }
    }
}

impl Drop for FinalStateGuard {
    fn drop(&mut self) {
        let Some(mut state) = self.0.take() else {
            return;
        };

        let aborted = if std::thread::panicking() {
            TaskAborted::Panicked
        } else {
            TaskAborted::Cancelled
        };

        let _ = state.set(ConnectionDisconnectReason::InternalError(Arc::new(aborted)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::task_state::TaskState;
    use std::time::Duration;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn wait_finished(state: &OnceLockState<ConnectionDisconnectReason>) {
        for _ in 0..500 {
            if state.is_finished() {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        panic!("task state was never set");
    }

    #[test]
    fn sets_returned_reason() {
        let rt = runtime();
        let spawner = TaskSpawner::new(rt.handle().clone(), None);
        let mut state = OnceLockState::new();

        spawner.spawn_guarded(
            async { ConnectionDisconnectReason::PeerClosed },
            state.clone(),
            None,
        );

        wait_finished(&state);
        assert!(matches!(
            state.get_disconnect_reason(),
            Some(ConnectionDisconnectReason::PeerClosed)
        ));
    }

    #[test]
    fn sets_reason_on_panic() {
        let rt = runtime();
        let spawner = TaskSpawner::new(rt.handle().clone(), None);
        let mut state = OnceLockState::new();

        spawner.spawn_guarded(async { panic!("test panic") }, state.clone(), None);

        wait_finished(&state);
        let Some(ConnectionDisconnectReason::InternalError(err)) =
            state.get_disconnect_reason()
        else {
            panic!("expected an internal error");
        };
        assert_eq!(err.to_string(), TaskAborted::Panicked.to_string());
    }

    #[test]
    fn sets_reason_on_runtime_shutdown() {
        let rt = runtime();
        let spawner = TaskSpawner::new(rt.handle().clone(), None);
        let mut state = OnceLockState::new();

        spawner.spawn_guarded(std::future::pending(), state.clone(), None);
        drop(rt);

        assert!(matches!(
            state.get_disconnect_reason(),
            Some(ConnectionDisconnectReason::InternalError(_))
        ));
    }

    #[test]
    fn permit_released_on_exit() {
        let rt = runtime();
        let spawner = TaskSpawner::new(rt.handle().clone(), Some(1));
        let limit = spawner.limit.clone().unwrap();
        let state = OnceLockState::new();

        let permit = limit.clone().try_acquire_owned().unwrap();
        assert!(limit.clone().try_acquire_owned().is_err());

        spawner.spawn_guarded(
            async { ConnectionDisconnectReason::UserClosed },
            state.clone(),
            Some(permit),
        );

        wait_finished(&state);
        // The permit is dropped right after the state is set.
        for _ in 0..500 {
            if limit.available_permits() == 1 {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        panic!("permit was never released");
    }
}
