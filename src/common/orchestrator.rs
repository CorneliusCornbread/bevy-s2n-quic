//! Deprecated. The orchestrator has been replaced by [TaskSpawner], which runs
//! each connection and stream as its own long-lived Tokio task.

use crate::common::spawner::TaskSpawner;

/// Deprecated alias for [TaskSpawner].
#[deprecated(
    note = "Replaced by `common::spawner::TaskSpawner`. Will be removed in the next breaking release."
)]
pub type OrchestratorHandle = TaskSpawner;

/// Deprecated, use [TaskSpawner] instead.
pub mod handle {
    #[allow(deprecated)]
    pub use super::OrchestratorHandle;
}
