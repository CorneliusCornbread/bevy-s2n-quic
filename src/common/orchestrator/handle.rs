use tokio::{
    runtime::Handle,
    sync::mpsc::{Sender, error::TrySendError},
};

use crate::common::{
    connection::task::ConnectionTask,
    orchestrator::QuicTask,
    stream::{receive::RecTask, send::SendTask},
};

#[derive(Clone, Debug)]
pub struct OrchestratorHandle {
    runtime: Handle,
    sender: Sender<QuicTask>,
}

impl OrchestratorHandle {
    pub(crate) fn new(sender: Sender<QuicTask>, runtime: Handle) -> Self {
        Self { sender, runtime }
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn push_connection(
        &self,
        conn_task: ConnectionTask,
    ) -> Result<(), TrySendError<ConnectionTask>> {
        let task = QuicTask::Connection(conn_task);
        let res = self.sender.try_send(task);

        if let Err(e) = res {
            return match e {
                TrySendError::Full(task) => match task {
                    QuicTask::Connection(connection_task) => {
                        Err(TrySendError::Full(connection_task))
                    }

                    _ => unreachable!(),
                },
                TrySendError::Closed(task) => match task {
                    QuicTask::Connection(connection_task) => {
                        Err(TrySendError::Full(connection_task))
                    }
                    _ => unreachable!(),
                },
            };
        };

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn push_receive(
        &self,
        task: RecTask,
    ) -> Result<(), TrySendError<RecTask>> {
        let task = QuicTask::Receive(task);
        let res = self.sender.try_send(task);

        if let Err(e) = res {
            return match e {
                TrySendError::Full(quic_task) => match quic_task {
                    QuicTask::Receive(task) => Err(TrySendError::Full(task)),

                    _ => unreachable!(),
                },
                TrySendError::Closed(quic_task) => match quic_task {
                    QuicTask::Receive(task) => Err(TrySendError::Full(task)),
                    _ => unreachable!(),
                },
            };
        };

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn push_send(&self, task: SendTask) -> Result<(), TrySendError<SendTask>> {
        let task = QuicTask::Send(task);
        let res = self.sender.try_send(task);

        if let Err(e) = res {
            return match e {
                TrySendError::Full(quic_task) => match quic_task {
                    QuicTask::Send(task) => Err(TrySendError::Full(task)),

                    _ => unreachable!(),
                },
                TrySendError::Closed(quic_task) => match quic_task {
                    QuicTask::Send(task) => Err(TrySendError::Full(task)),
                    _ => unreachable!(),
                },
            };
        };

        Ok(())
    }

    /// Returns a handle to the Tokio runtime used for
    /// running async tasks.
    pub fn tokio_handle(&self) -> &Handle {
        &self.runtime
    }
}
