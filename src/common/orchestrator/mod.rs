use bevy::log::info;
use futures::FutureExt;
use std::fmt;
use tokio::{
    runtime::Handle,
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use crate::common::{
    connection::task::ConnectionTask,
    orchestrator::handle::OrchestratorHandle,
    status_code::StatusCode,
    stream::{receive::RecTask, send::SendTask},
};

pub mod handle;

/// Size of the orchestrator channel buffer before it is considered full.
const ORCHESTRATOR_CHANNEL_SIZE: usize = 128;

/// Error code to use when our orchestrator can't handle a new task
pub(crate) const ORCHESTRATOR_ERROR_CODE: StatusCode = StatusCode::ServiceUnavailable;

#[derive(Debug)]
pub(crate) enum QuicTask {
    Connection(ConnectionTask),
    Send(SendTask),
    Receive(RecTask),
}

impl fmt::Display for QuicTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuicTask::Connection(task) => {
                write!(f, "QuicTask::Connection({})", task.id())
            }
            QuicTask::Send(task) => {
                write!(f, "QuicTask::Send({})", task.id())
            }
            QuicTask::Receive(task) => {
                write!(f, "QuicTask::Receive({})", task.id())
            }
        }
    }
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
    tasks: Vec<QuicTask>,
}

const MAX_TASKS: usize = 32768;
const MIN_TASKS_SIZE: usize = 32;

impl AsyncOrchestratorTask {
    fn new(task_rec: Receiver<QuicTask>) -> Self {
        Self {
            task_rec,
            tasks: Vec::with_capacity(MIN_TASKS_SIZE),
        }
    }

    async fn start(mut self) {
        loop {
            if !self.task_rec.is_empty() {
                self.task_rec.recv_many(&mut self.tasks, MAX_TASKS).await;
            }

            let mut finished_ind = Vec::new();

            for (i, task) in self.tasks.iter_mut().enumerate() {
                // If the task finishes and we get a disconnect flag
                // push it to be marked for removal
                if let (i, Some(Some(_))) = match task {
                    QuicTask::Connection(connection_task) => {
                        (i, connection_task.poll_once().now_or_never())
                    }
                    QuicTask::Send(send_task) => {
                        (i, send_task.poll_once().now_or_never())
                    }
                    QuicTask::Receive(rec_task) => {
                        (i, rec_task.poll_once().now_or_never())
                    }
                } {
                    finished_ind.push(i);
                }
            }

            for idx in finished_ind.into_iter().rev() {
                let removed = self.tasks.swap_remove(idx);
                info!("Removed quic task: {}", removed);
            }

            tokio::task::yield_now().await;
        }
    }
}
