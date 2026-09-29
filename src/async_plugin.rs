use bevy::app::Plugin;

use crate::common::runtime::TokioRuntime;

/// Plugin which manages inserting the [TokioRuntime] resource.
///
/// This will default to using 25% of the available logical
/// processors for async work, with no limit on the number of tasks.
#[derive(Default)]
pub struct QuicAsyncPlugin {
    worker_threads: Option<usize>,
    max_tasks: Option<usize>,
}

impl Plugin for QuicAsyncPlugin {
    fn build(&self, app: &mut bevy::prelude::App) {
        let threads = self
            .worker_threads
            .unwrap_or_else(TokioRuntime::default_worker_count);

        app.insert_resource(TokioRuntime::new(threads, self.max_tasks));
    }
}

impl QuicAsyncPlugin {
    /// Used if you want to specify the number of worker threads
    /// to be used when handling the async work of the Quic tasks.
    ///
    /// Use the [Default] implementation if you don't need a custom thread count.
    pub fn new_with_threads(worker_threads: usize) -> Self {
        Self {
            worker_threads: Some(worker_threads),
            max_tasks: None,
        }
    }

    /// Limits how many connection and stream tasks can run at once. Every
    /// connection, send stream and receive stream uses one task.
    ///
    /// Once the limit is reached, new connections and streams are closed with
    /// [StatusCode::ServiceUnavailable][crate::common::status_code::StatusCode::ServiceUnavailable]
    /// and report
    /// [ConnectionDisconnectReason::OrchestratorError][crate::common::connection::disconnect::ConnectionDisconnectReason::OrchestratorError].
    pub fn with_max_tasks(mut self, max_tasks: usize) -> Self {
        self.max_tasks = Some(max_tasks);
        self
    }
}
