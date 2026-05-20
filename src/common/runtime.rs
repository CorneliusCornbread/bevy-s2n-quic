use bevy::ecs::resource::Resource;
use tokio::runtime::{Handle, Runtime};

use crate::common::orchestrator::AsyncOrchestrator;

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
    pub fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    // TODO: Finalize return type
    pub fn get_orchestrator_state(&self) -> bool {
        todo!()
    }
}
