use std::{cmp::max, sync::Arc};

use bevy::{ecs::resource::Resource, tasks::available_parallelism};
use tokio::runtime::{Builder, Handle, Runtime};

use crate::common::spawner::TaskSpawner;

/// The number of threads determined by a percentage of total threads available.
const DEFAULT_THREAD_PERCENT: f32 = 0.25;

const MIN_THREADS: usize = 2;

/// Name given to the runtime's threads unless one is configured.
const DEFAULT_THREAD_NAME: &str = "bevy-s2n-quic";

pub(crate) type ThreadStartHook = Arc<dyn Fn() + Send + Sync>;

/// Settings for building a [QuicRuntime].
#[derive(Default)]
pub(crate) struct RuntimeOptions {
    pub threads: Option<usize>,
    pub max_tasks: Option<usize>,
    pub thread_name: Option<String>,
    pub on_thread_start: Option<ThreadStartHook>,
}

/// The background threads that run all QUIC network IO.
///
/// [QuicDriverPlugin][crate::driver_plugin::QuicDriverPlugin] inserts this
/// resource. Servers and clients are created with it.
#[derive(Resource)]
pub struct QuicRuntime {
    runtime: Runtime,
    spawner: TaskSpawner,
}

impl Default for QuicRuntime {
    fn default() -> Self {
        Self::with_options(RuntimeOptions::default())
    }
}

impl QuicRuntime {
    /// Creates a runtime with `threads` network threads.
    pub fn new(threads: usize) -> Self {
        Self::with_options(RuntimeOptions {
            threads: Some(threads),
            ..Default::default()
        })
    }

    /// The number of network threads used when none is configured: 25% of
    /// the available logical processors, and at least 2.
    ///
    /// Useful for sizing other thread pools (such as Bevy's) around the runtime.
    pub fn default_thread_count() -> usize {
        let thread_count =
            ((available_parallelism() as f32) * DEFAULT_THREAD_PERCENT).ceil() as usize;

        max(MIN_THREADS, thread_count)
    }

    pub(crate) fn with_options(options: RuntimeOptions) -> Self {
        let threads = options.threads.unwrap_or_else(Self::default_thread_count);
        let thread_name = options
            .thread_name
            .unwrap_or_else(|| DEFAULT_THREAD_NAME.to_owned());

        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(threads)
            .thread_name(thread_name)
            .enable_all();

        if let Some(hook) = options.on_thread_start {
            builder.on_thread_start(move || hook());
        }

        let runtime = builder.build().expect("Unable to create async runtime.");
        let spawner = TaskSpawner::new(runtime.handle().clone(), options.max_tasks);

        Self { runtime, spawner }
    }

    pub(crate) fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    pub(crate) fn spawner(&self) -> &TaskSpawner {
        &self.spawner
    }
}
