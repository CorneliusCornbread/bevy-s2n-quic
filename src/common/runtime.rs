use std::cmp::max;

use bevy::{ecs::resource::Resource, tasks::available_parallelism};
use tokio::runtime::{Handle, Runtime};

use crate::common::spawner::TaskSpawner;

/// The number of workers determined by a percentage of total threads available.
const DEFAULT_WORKER_PERCENT: f32 = 0.25;

const MIN_WORKERS: usize = 2;

#[derive(Resource)]
pub struct TokioRuntime {
    pub(crate) runtime: Runtime,
    spawner: TaskSpawner,
}

impl Default for TokioRuntime {
    fn default() -> Self {
        Self::new(Self::default_worker_count(), None)
    }
}

impl TokioRuntime {
    /// Get the Tokio runtime [Handle] used for all
    /// async tasks.
    pub fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    pub(crate) fn default_worker_count() -> usize {
        let worker_count =
            ((available_parallelism() as f32) * DEFAULT_WORKER_PERCENT).ceil() as usize;

        max(MIN_WORKERS, worker_count)
    }

    pub(crate) fn new(worker_threads: usize, max_tasks: Option<usize>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
            .expect("Unable to create async runtime.");

        let spawner = TaskSpawner::new(runtime.handle().clone(), max_tasks);

        Self { runtime, spawner }
    }

    /// Returns the spawner used to start the async task behind each
    /// connection and stream.
    pub fn spawner(&self) -> &TaskSpawner {
        &self.spawner
    }
}
