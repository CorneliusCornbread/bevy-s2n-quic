use std::cmp::max;

use bevy::{ecs::resource::Resource, tasks::available_parallelism};
use tokio::runtime::{Handle, Runtime};

use crate::common::orchestrator::{AsyncOrchestrator, handle::OrchestratorHandle};

/// The number of workers determined by a percentage of total threads available.
const DEFAULT_WORKER_PERCENT: f32 = 0.25;

const MIN_WORKERS: usize = 2;

#[derive(Resource)]
pub struct TokioRuntime {
    pub(crate) runtime: Runtime,
    orchestrator: AsyncOrchestrator,
}

impl Default for TokioRuntime {
    fn default() -> Self {
        let mut worker_count =
            ((available_parallelism() as f32) * DEFAULT_WORKER_PERCENT).ceil() as usize;

        worker_count = max(MIN_WORKERS, worker_count);

        Self::new(worker_count)
    }
}

impl TokioRuntime {
    /// Get the Tokio runtime [Handle] used for all
    /// async tasks.
    pub fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    pub(crate) fn new(worker_threads: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
            .expect("Unable to create async runtime.");

        let orchestrator =
            AsyncOrchestrator::new(runtime.handle().clone(), worker_threads);

        Self {
            runtime,
            orchestrator,
        }
    }

    /// Returns a handle to the async task orchestrator.
    /// This allows for the management and scheduling of async tasks.
    pub(crate) fn orchestrator(&self) -> &OrchestratorHandle {
        self.orchestrator.handle()
    }
}
