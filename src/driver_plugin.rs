use std::sync::Arc;

use bevy::app::Plugin;

use crate::common::runtime::{QuicRuntime, RuntimeOptions, ThreadStartHook};

/// Plugin which inserts the [QuicRuntime] resource.
///
/// This will default to using 25% of the available logical processors for
/// network threads, with no limit on the number of connections and streams.
#[derive(Default)]
pub struct QuicDriverPlugin {
    threads: Option<usize>,
    max_tasks: Option<usize>,
    thread_name: Option<String>,
    on_thread_start: Option<ThreadStartHook>,
}

impl Plugin for QuicDriverPlugin {
    fn build(&self, app: &mut bevy::prelude::App) {
        app.insert_resource(QuicRuntime::with_options(RuntimeOptions {
            threads: self.threads,
            max_tasks: self.max_tasks,
            thread_name: self.thread_name.clone(),
            on_thread_start: self.on_thread_start.clone(),
        }));
    }
}

impl QuicDriverPlugin {
    /// Sets the number of network threads.
    ///
    /// Defaults to [QuicRuntime::default_thread_count].
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Limits how many connections and streams can be open at once.
    ///
    /// Once the limit is reached, new connections and streams are closed with
    /// [StatusCode::ServiceUnavailable][crate::common::status_code::StatusCode::ServiceUnavailable]
    /// and report
    /// [ConnectionDisconnectReason::OrchestratorError][crate::common::connection::disconnect::ConnectionDisconnectReason::OrchestratorError].
    pub fn with_max_tasks(mut self, max_tasks: usize) -> Self {
        self.max_tasks = Some(max_tasks);
        self
    }

    /// Names the network threads. Defaults to `bevy-s2n-quic`.
    pub fn with_thread_name(mut self, name: impl Into<String>) -> Self {
        self.thread_name = Some(name.into());
        self
    }

    /// Runs `hook` on each network thread when it starts, for example to pin
    /// it to a CPU or change its priority.
    ///
    /// Calling this more than once runs every hook, in the order they were added.
    ///
    /// ```no_run
    /// use bevy_s2n_quic::driver_plugin::QuicDriverPlugin;
    ///
    /// let plugin = QuicDriverPlugin::default().with_threads(2).on_thread_start(|| {
    ///     // Pin the thread, lower its priority, etc.
    /// });
    /// ```
    pub fn on_thread_start(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        let hook: ThreadStartHook = match self.on_thread_start.take() {
            None => Arc::new(hook),
            Some(previous) => Arc::new(move || {
                previous();
                hook();
            }),
        };
        self.on_thread_start = Some(hook);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy::app::App;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    fn thread_name(runtime: &QuicRuntime) -> Option<String> {
        let handle = runtime.handle();
        let task =
            handle.spawn(async { std::thread::current().name().map(str::to_owned) });
        handle.block_on(task).unwrap()
    }

    #[test]
    fn sets_thread_count_and_name() {
        let mut app = App::new();
        app.add_plugins(
            QuicDriverPlugin::default()
                .with_threads(3)
                .with_thread_name("net"),
        );

        let runtime = app.world().resource::<QuicRuntime>();
        assert_eq!(runtime.handle().metrics().num_workers(), 3);
        assert_eq!(thread_name(runtime).as_deref(), Some("net"));
    }

    #[test]
    fn thread_start_hooks_run_in_order() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let (first, second) = (order.clone(), order.clone());
        let started = Arc::new(AtomicUsize::new(0));
        let counter = started.clone();

        let mut app = App::new();
        app.add_plugins(
            QuicDriverPlugin::default()
                .with_threads(1)
                .on_thread_start(move || first.lock().unwrap().push(1))
                .on_thread_start(move || {
                    second.lock().unwrap().push(2);
                    counter.fetch_add(1, Ordering::Relaxed);
                }),
        );

        let runtime = app.world().resource::<QuicRuntime>();
        assert_eq!(thread_name(runtime).as_deref(), Some("bevy-s2n-quic"));
        // The worker that ran the task has started.
        assert!(started.load(Ordering::Relaxed) >= 1);
        assert_eq!(order.lock().unwrap()[..2], [1, 2]);
    }
}
