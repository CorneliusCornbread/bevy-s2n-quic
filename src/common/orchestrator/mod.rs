use futures::stream::FuturesUnordered;
use tokio::{
    runtime::Handle,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::common::{
    connection::task::ConnectionTask,
    orchestrator::{self, handle::OrchestratorHandle},
    stream::{receive::RecTask, send::SendTask},
};

pub mod handle;

/// Size of the orchestrator channel buffer before it is considered full.
const ORCHESTRATOR_CHANNEL_SIZE: usize = 128;

pub(crate) enum QuicTask {
    Connection(ConnectionTask),
    Send(SendTask),
    Receive(RecTask),
}

pub(crate) struct AsyncOrchestrator {
    runtime: Handle,
    task_join: JoinHandle<()>,
    orchestrator: OrchestratorHandle,
}

impl AsyncOrchestrator {
    pub(crate) fn new(runtime: Handle) -> Self {
        let (tx, rx) = mpsc::channel(ORCHESTRATOR_CHANNEL_SIZE);

        let task = AsyncOrchestratorTask::new(rx);
        let task_join = runtime.spawn(task.start());
        let orchestrator = OrchestratorHandle::new(tx);

        Self {
            runtime,
            task_join,
            orchestrator,
        }
    }

    pub(crate) fn handle(&self) -> &OrchestratorHandle {
        &self.orchestrator
    }
}

struct AsyncOrchestratorTask {
    task_rec: Receiver<QuicTask>,
}

impl AsyncOrchestratorTask {
    fn new(task_rec: Receiver<QuicTask>) -> Self {
        Self { task_rec }
    }

    async fn start(self) {
        'running: loop {}
    }
}
