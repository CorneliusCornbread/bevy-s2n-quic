use bevy::log::{error, info};
use futures::FutureExt;
use std::fmt;
use tokio::{
    runtime::Handle,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::common::{
    connection::task::ConnectionTask,
    orchestrator::handle::OrchestratorHandle,
    status_code::StatusCode,
    stream::{receive::RecTask, send::SendTask},
};

pub mod handle;

/// Size of the shared incoming task channel.
const ORCHESTRATOR_CHANNEL_SIZE: usize = 128;

/// Size of each individual worker's incoming task channel.
const WORKER_CHANNEL_SIZE: usize = 512;

/// Maximum number of tasks drained from a channel in a single batch.
const MAX_TASKS_PER_BATCH: usize = 32768;

/// Initial task-list capacity allocated for each worker.
const MIN_TASKS_SIZE: usize = 32;

/// Error code used when the orchestrator cannot accept a new task.
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
            QuicTask::Send(task) => write!(f, "QuicTask::Send({})", task.id()),
            QuicTask::Receive(task) => {
                write!(f, "QuicTask::Receive({})", task.id())
            }
        }
    }
}

pub(crate) struct AsyncOrchestrator {
    _task_joins: Vec<JoinHandle<()>>,
    orchestrator: OrchestratorHandle,
}

impl AsyncOrchestrator {
    pub(crate) fn new(runtime: Handle, worker_count: usize) -> Self {
        let (tx, rx) = mpsc::channel(ORCHESTRATOR_CHANNEL_SIZE);

        // One channel per worker; the dispatcher holds the senders.
        let mut task_joins = Vec::with_capacity(worker_count + 1);
        let mut worker_senders = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let (worker_tx, worker_rx) = mpsc::channel(WORKER_CHANNEL_SIZE);
            worker_senders.push(worker_tx);
            task_joins.push(runtime.spawn(OrchestratorWorker::new(worker_rx).start()));
        }

        task_joins
            .push(runtime.spawn(OrchestratorDispatcher::new(rx, worker_senders).start()));

        let orchestrator = OrchestratorHandle::new(tx, runtime.clone());

        Self {
            _task_joins: task_joins,
            orchestrator,
        }
    }

    pub(crate) fn handle(&self) -> &OrchestratorHandle {
        &self.orchestrator
    }
}

/// Receives every incoming [`QuicTask`] and forwards it to one of the workers
/// using round-robin assignment.  If the target worker's channel is full it
/// falls back to the remaining workers in order before giving up.
struct OrchestratorDispatcher {
    task_rec: Receiver<QuicTask>,
    worker_senders: Vec<Sender<QuicTask>>,
    next_worker: usize,
}

impl OrchestratorDispatcher {
    fn new(task_rec: Receiver<QuicTask>, worker_senders: Vec<Sender<QuicTask>>) -> Self {
        Self {
            task_rec,
            worker_senders,
            next_worker: 0,
        }
    }

    #[tracing::instrument(skip_all, name = "orchestrator_dispatcher")]
    async fn start(mut self) {
        let worker_count = self.worker_senders.len();

        info!("Async orchestrator started with {worker_count} worker(s)");

        let mut batch = Vec::new();

        loop {
            // Block until at least one task arrives.
            let count = self
                .task_rec
                .recv_many(&mut batch, MAX_TASKS_PER_BATCH)
                .await;

            if count == 0 {
                // Every sender has been dropped; nothing left to dispatch.
                break;
            }

            let worker_count = self.worker_senders.len();

            for task in batch.drain(..) {
                let start_idx = self.next_worker % worker_count;
                self.next_worker = self.next_worker.wrapping_add(1);

                // Try the assigned worker first, then spill to others.
                let mut pending = Some(task);
                for off in 0..worker_count {
                    let idx = (start_idx + off) % worker_count;
                    match self.worker_senders[idx].try_send(pending.take().unwrap()) {
                        Ok(()) => break,
                        Err(e) => pending = Some(e.into_inner()),
                    }
                }

                if pending.is_some() {
                    error!(
                        "All worker channels are full; task dropped. \
                         Consider increasing WORKER_CHANNEL_SIZE."
                    );
                }
            }
        }
    }
}

/// Owns and polls a subset of [`QuicTask`]s assigned by the dispatcher.
///
/// When its task list is empty the worker blocks on its channel rather than
/// spinning, so it consumes no CPU while idle.
struct OrchestratorWorker {
    task_rec: Receiver<QuicTask>,
    tasks: Vec<QuicTask>,
}

impl OrchestratorWorker {
    fn new(task_rec: Receiver<QuicTask>) -> Self {
        Self {
            task_rec,
            tasks: Vec::with_capacity(MIN_TASKS_SIZE),
        }
    }

    #[tracing::instrument(skip_all, name = "orchestrator_worker", fields(task_count = self.tasks.len()))]
    async fn start(mut self) {
        loop {
            if self.tasks.is_empty() {
                // No active tasks
                let count = self
                    .task_rec
                    .recv_many(&mut self.tasks, MAX_TASKS_PER_BATCH)
                    .await;
                if count == 0 {
                    // Dispatcher dropped all senders; this worker is done.
                    break;
                }
            } else {
                while let Ok(task) = self.task_rec.try_recv() {
                    self.tasks.push(task);
                }
            }

            let mut finished = Vec::new();

            for (i, task) in self.tasks.iter_mut().enumerate() {
                let done = match task {
                    QuicTask::Connection(t) => {
                        matches!(t.poll_once().now_or_never(), Some(Some(_)))
                    }
                    QuicTask::Send(t) => {
                        matches!(t.poll_once().now_or_never(), Some(Some(_)))
                    }
                    QuicTask::Receive(t) => {
                        matches!(t.poll_once().now_or_never(), Some(Some(_)))
                    }
                };

                if done {
                    finished.push(i);
                }
            }

            for idx in finished.into_iter().rev() {
                let removed = self.tasks.swap_remove(idx);
                info!("Removed quic task: {removed}");
            }

            tokio::task::yield_now().await;
        }
    }
}
