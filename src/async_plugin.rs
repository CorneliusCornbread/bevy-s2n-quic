use std::sync::Mutex;

use bevy::app::Plugin;
use tokio::runtime::Builder;

use crate::common::runtime::{BuilderHook, TokioRuntime};

/// Plugin which manages inserting the [TokioRuntime] resource.
///
/// This will default to using 25% of the available logical
/// processors for async work, with no limit on the number of tasks.
#[derive(Default)]
pub struct QuicAsyncPlugin {
    worker_threads: Option<usize>,
    max_tasks: Option<usize>,
    // Taken on build. A `Mutex` keeps the plugin `Sync` without requiring
    // the hook to be `Sync`.
    runtime_builder: Mutex<Option<BuilderHook>>,
}

impl Plugin for QuicAsyncPlugin {
    fn build(&self, app: &mut bevy::prelude::App) {
        let threads = self
            .worker_threads
            .unwrap_or_else(TokioRuntime::default_worker_count);

        let configure = self
            .runtime_builder
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        app.insert_resource(TokioRuntime::new(threads, self.max_tasks, configure));
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
            ..Default::default()
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

    /// Customizes the multi-threaded Tokio [Builder] before the runtime is
    /// built, for example to set `thread_name`, `on_thread_start` (thread
    /// pinning, priority) or `event_interval`.
    ///
    /// The hook runs after the plugin's own settings (the worker count,
    /// `thread_name("bevy-s2n-quic")` and `enable_all()`), so it can override
    /// them, including the worker count. s2n-quic needs the IO and time
    /// drivers, so keep them enabled.
    ///
    /// Calling this more than once runs every hook, in the order they were added.
    ///
    /// ```no_run
    /// use bevy_s2n_quic::async_plugin::QuicAsyncPlugin;
    ///
    /// let plugin = QuicAsyncPlugin::default().with_runtime_builder(|builder| {
    ///     builder.worker_threads(2).on_thread_start(|| {
    ///         // Pin the worker, lower its priority, etc.
    ///     });
    /// });
    /// ```
    pub fn with_runtime_builder(
        self,
        hook: impl FnOnce(&mut Builder) + Send + 'static,
    ) -> Self {
        let mut slot = self
            .runtime_builder
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let hook: BuilderHook = match slot.take() {
            None => Box::new(hook),
            Some(previous) => Box::new(move |builder| {
                previous(builder);
                hook(builder);
            }),
        };
        *slot = Some(hook);
        drop(slot);

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy::app::App;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn runtime_builder_hooks_run_in_order() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let (first, second) = (order.clone(), order.clone());

        let plugin = QuicAsyncPlugin::new_with_threads(3)
            .with_runtime_builder(move |b| {
                first.lock().unwrap().push(1);
                b.worker_threads(2);
            })
            .with_runtime_builder(move |b| {
                second.lock().unwrap().push(2);
                b.worker_threads(1);
            });

        let mut app = App::new();
        app.add_plugins(plugin);

        assert_eq!(*order.lock().unwrap(), [1, 2]);
        let runtime = app.world().resource::<TokioRuntime>();
        assert_eq!(runtime.handle().metrics().num_workers(), 1);
    }

    #[test]
    fn runtime_builder_on_thread_start() {
        let started = Arc::new(AtomicUsize::new(0));
        let counter = started.clone();

        let mut app = App::new();
        app.add_plugins(QuicAsyncPlugin::new_with_threads(2).with_runtime_builder(
            move |b| {
                b.on_thread_start(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            },
        ));

        let runtime = app.world().resource::<TokioRuntime>();
        let handle = runtime.handle();
        let task =
            handle.spawn(async { std::thread::current().name().map(str::to_owned) });
        let name = handle.block_on(task).unwrap();

        assert_eq!(name.as_deref(), Some("bevy-s2n-quic"));
        // The worker that ran the task has started; others may still be starting.
        assert!(started.load(Ordering::Relaxed) >= 1);
    }
}
