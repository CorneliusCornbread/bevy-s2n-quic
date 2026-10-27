use std::sync::Arc;

use aeronet_io::IoSystems;
use bevy::{
    app::{Last, Plugin, PreUpdate},
    ecs::schedule::IntoScheduleConfigs,
};

use crate::common::{
    router::{DriverRouter, flush_driver_commands, poll_driver_events},
    runtime::{QuicRuntime, RuntimeOptions, ThreadStartHook},
};

/// Plugin which inserts the [QuicRuntime] resource and moves data between it
/// and the QUIC components.
///
/// Events from the network threads are applied to components once per frame,
/// in [IoSystems::Poll] in `PreUpdate`. Commands from components (data to
/// send, closes, new streams) are handed to the network threads at the end of
/// the frame, in `Last`, with at most one wake per thread.
///
/// This will default to [QuicRuntime::default_thread_count] network threads,
/// with no limit on the number of connections.
#[derive(Default)]
pub struct QuicDriverPlugin {
    threads: Option<usize>,
    max_connections: Option<usize>,
    thread_name: Option<String>,
    on_thread_start: Option<ThreadStartHook>,
}

impl Plugin for QuicDriverPlugin {
    fn build(&self, app: &mut bevy::prelude::App) {
        app.insert_resource(QuicRuntime::with_options(RuntimeOptions {
            threads: self.threads,
            max_connections: self.max_connections,
            thread_name: self.thread_name.clone(),
            on_thread_start: self.on_thread_start.clone(),
        }))
        .init_resource::<DriverRouter>()
        .add_systems(PreUpdate, poll_driver_events.in_set(IoSystems::Poll))
        .add_systems(Last, flush_driver_commands);
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

    /// Limits how many connections each server has open at once. Past the
    /// limit, new connections are refused, and the client's
    /// [QuicConnectionAttempt][crate::common::connection::QuicConnectionAttempt]
    /// fails.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = Some(max_connections);
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
    use std::{
        sync::Mutex,
        time::{Duration, Instant},
    };

    /// Waits until `count` network threads have recorded something.
    fn wait_for<T: Clone>(records: &Mutex<Vec<T>>, count: usize) -> Vec<T> {
        let start = Instant::now();
        loop {
            let records = records.lock().unwrap();
            if records.len() >= count {
                return records.clone();
            }
            drop(records);
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "threads didn't start"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn sets_thread_count_and_name() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let record = names.clone();

        let mut app = App::new();
        app.add_plugins(
            QuicDriverPlugin::default()
                .with_threads(3)
                .with_thread_name("net")
                .on_thread_start(move || {
                    let name = std::thread::current().name().map(str::to_owned);
                    record.lock().unwrap().push(name);
                }),
        );

        let runtime = app.world().resource::<QuicRuntime>();
        assert_eq!(runtime.thread_count(), 3);
        assert_eq!(wait_for(&names, 3), vec![Some("net".to_owned()); 3]);
    }

    #[test]
    fn thread_start_hooks_run_in_order() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let (first, second) = (order.clone(), order.clone());

        let mut app = App::new();
        app.add_plugins(
            QuicDriverPlugin::default()
                .with_threads(1)
                .on_thread_start(move || first.lock().unwrap().push("first".to_owned()))
                .on_thread_start(move || {
                    let name =
                        std::thread::current().name().unwrap_or_default().to_owned();
                    second.lock().unwrap().push(name);
                }),
        );

        assert_eq!(wait_for(&order, 2), ["first", "bevy-s2n-quic"]);
    }
}
