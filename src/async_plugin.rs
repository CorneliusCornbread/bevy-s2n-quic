use bevy::app::Plugin;

use crate::common::runtime::TokioRuntime;

/// Plugin which manages inserting the [TokioRuntime] resource.
///
/// This will default to using 25% of the available logical
/// processors for async work.
#[derive(Default)]
pub struct QuicAsyncPlugin {
    worker_threads: Option<usize>,
}

impl Plugin for QuicAsyncPlugin {
    fn build(&self, app: &mut bevy::prelude::App) {
        // If we set a worker thread count pass it to the runtime
        if let Some(threads) = self.worker_threads {
            let tokio_runtime = TokioRuntime::new(threads);
            app.insert_resource(tokio_runtime);
            return;
        }

        app.init_resource::<TokioRuntime>();
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
        }
    }
}
