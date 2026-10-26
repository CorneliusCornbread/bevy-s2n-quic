use std::cmp::max;

use bevy::{ecs::resource::Resource, tasks::available_parallelism};
use tokio::runtime::{Builder, Handle, Runtime};

use crate::common::spawner::TaskSpawner;

/// The number of workers determined by a percentage of total threads available.
const DEFAULT_WORKER_PERCENT: f32 = 0.25;

const MIN_WORKERS: usize = 2;

/// Name given to the runtime's threads unless the builder hook overrides it.
const DEFAULT_THREAD_NAME: &str = "bevy-s2n-quic";

/// Customizes the runtime [Builder] before it is built.
pub(crate) type BuilderHook = Box<dyn FnOnce(&mut Builder) + Send>;

#[derive(Resource)]
pub struct TokioRuntime {
    pub(crate) runtime: Runtime,
    spawner: TaskSpawner,
}

impl Default for TokioRuntime {
    fn default() -> Self {
        Self::new(Self::default_worker_count(), None, None)
    }
}

impl TokioRuntime {
    /// Get the Tokio runtime [Handle] used for all
    /// async tasks.
    pub fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    /// The number of worker threads used when none is configured: 25% of the
    /// available logical processors, and at least 2.
    ///
    /// Useful for sizing other thread pools (such as Bevy's) around the runtime.
    pub fn default_worker_count() -> usize {
        let worker_count =
            ((available_parallelism() as f32) * DEFAULT_WORKER_PERCENT).ceil() as usize;

        max(MIN_WORKERS, worker_count)
    }

    /// Builds the runtime with `worker_threads` workers, then applies
    /// `configure`, which may override any of the defaults.
    pub(crate) fn new(
        worker_threads: usize,
        max_tasks: Option<usize>,
        configure: Option<BuilderHook>,
    ) -> Self {
        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(worker_threads)
            .thread_name(DEFAULT_THREAD_NAME)
            .enable_all();

        if let Some(configure) = configure {
            configure(&mut builder);
        }

        let runtime = builder.build().expect("Unable to create async runtime.");
        let spawner = TaskSpawner::new(runtime.handle().clone(), max_tasks);

        Self { runtime, spawner }
    }

    /// Returns the spawner used to start the async task behind each
    /// connection and stream.
    pub fn spawner(&self) -> &TaskSpawner {
        &self.spawner
    }
}
