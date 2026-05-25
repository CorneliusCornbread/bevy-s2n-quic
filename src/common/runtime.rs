use bevy::ecs::resource::Resource;
use tokio::runtime::{Handle, Runtime};

use crate::common::orchestrator::{AsyncOrchestrator, handle::OrchestratorHandle};

#[derive(Resource)]
pub struct TokioRuntime {
    pub(crate) runtime: Runtime,
    orchestrator: AsyncOrchestrator,
}

impl Default for TokioRuntime {
    fn default() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Unable to create async runtime.");

        let orchestrator = AsyncOrchestrator::new(runtime.handle().clone());

        Self {
            runtime,
            orchestrator,
        }
    }
}

impl TokioRuntime {
    /// Get the Tokio runtime [Handle] used for all
    /// async tasks.
    pub fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    /// Returns a handle to the async task orchestrator.
    /// This allows for the management and scheduling of async tasks.
    pub(crate) fn orchestrator(&self) -> &OrchestratorHandle {
        self.orchestrator.handle()
    }
}
