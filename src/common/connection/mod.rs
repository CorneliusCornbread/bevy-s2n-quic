use bevy::{
    ecs::component::Component,
    log::{
        error,
        tracing::{self},
        warn,
    },
    prelude::{Deref, DerefMut},
};
use s2n_quic::{Connection, application, connection::Handle as ConnectionHandle};
use std::sync::Arc;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{self},
        oneshot,
    },
    task::JoinHandle,
};

use crate::common::{
    QuicParentId,
    attempt::{QuicActionAttempt, TaskError},
    connection::{
        disconnect::ConnectionDisconnectReason,
        id::ConnectionId,
        open_flag::OpenFlag,
        stream_flag::StreamFlag,
        task::{
            ConnectionCommand, ConnectionCommandError, ConnectionHandleTask,
            ConnectionTask, ConnectionTaskState,
        },
    },
    orchestrator::{self, handle::OrchestratorHandle},
    stream::{
        QuicBidirectionalStreamAttempt, QuicPeerStreamAttempt, QuicReceiveStreamAttempt,
        QuicSendStreamAttempt,
    },
    task_state::{OnceLockState, TaskState},
};

pub mod disconnect;
pub mod id;
pub(super) mod open_flag;
pub mod plugin;
pub(super) mod stream_flag;
pub mod task;

/// Number of messages that can sit unhandled by the connection task
const CONNECTION_CTRL_CHANNEL_SIZE: usize = 1024;

type ConnectionResponse<T> = Result<Option<T>, TaskError>;

/// This is a structure which represents an in progress connection.
/// Any pending connections this attempt may hold will be closed
/// if this structure is dropped without being handled.
///
/// Attempts are all handled internally. Put them on an entity
/// and they will be replaced with a full [QuicConnection]
/// on the same entity.
///
/// In the event of a failure, with the `connection-errors` feature
/// flag enabled, a [QuicActionErrorComponent][crate::common::attempt::QuicActionErrorComponent]
/// will be added on the entity.
#[derive(Deref, DerefMut, Component)]
#[component(storage = "SparseSet")]
pub struct QuicConnectionAttempt(QuicActionAttempt<Connection>);

impl QuicConnectionAttempt {
    pub(crate) fn new(
        handle: Handle,
        conn_task: JoinHandle<Result<Connection, TaskError>>,
        parent_id: QuicParentId,
    ) -> Self {
        Self(QuicActionAttempt::new(handle, conn_task, parent_id))
    }
}

/// The component analogue to [Connection] in s2n-quic.
/// This component manages the async behaviour of our Quic connection.
#[derive(Debug, Component)]
pub struct QuicConnection {
    runtime: Handle,
    orchestrator: OrchestratorHandle,
    conn_handle: ConnectionHandle,
    task_state: OnceLockState<ConnectionDisconnectReason>,
    conn_command_channel: mpsc::Sender<ConnectionCommand>,
    is_open: OpenFlag,
    connection_id: ConnectionId,
    /// Flag set by async wakers as soon as there's a new stream
    pending_stream: Arc<StreamFlag>,
}

impl QuicConnection {
    #[tracing::instrument(
        name = "new_quic_connection"
        skip(runtime),
    )]
    pub fn new(
        runtime: Handle,
        orchestrator: OrchestratorHandle,
        mut connection: Connection,
        parent_id: QuicParentId,
    ) -> Self {
        let res = connection.keep_alive(true);
        let (send, rec) = mpsc::channel(CONNECTION_CTRL_CHANNEL_SIZE);
        let connection_id = ConnectionId::new(connection.id(), parent_id);

        let pending_stream = Arc::new(StreamFlag::new(false));

        if let Err(e) = res {
            warn!(
                "Unable to mark new connection with keep alive, is the connection already closed? Reason: \"{}\"",
                e
            );
        }

        let task_state = OnceLockState::new();

        let is_open = OpenFlag::new(true);
        let conn_handle = connection.handle();
        let task = ConnectionTask::new(
            connection,
            rec,
            connection_id,
            is_open.clone(),
            pending_stream.clone(),
            task_state.clone(),
            orchestrator.clone(),
        );

        // TODO: move to task orchestrator
        let handle = runtime.spawn(task.start());

        Self {
            runtime: runtime.clone(),
            orchestrator,
            conn_handle,
            task_state,
            conn_command_channel: send,
            is_open,
            connection_id,
            pending_stream,
        }
    }

    /// Accepts any incoming streams, this will always return an [QuicPeerStreamAttempt] even if
    /// there are no pending streams.
    ///
    /// There's also a chance that an accept will successfully get a stream even if there aren't
    /// any pending streams due to network timings.
    ///
    /// Returns an error if the async communication channel errors out due to being full.
    pub fn accept_stream(
        &mut self,
    ) -> Result<QuicPeerStreamAttempt, ConnectionCommandError> {
        self.pending_stream.set_false();

        let (send, rec) = oneshot::channel();

        let cmd = ConnectionCommand::Accept { respond_to: send };
        let send_res = self.conn_command_channel.try_send(cmd);

        if let Err(err) = send_res {
            return Err(err.into());
        }

        let attempt =
            QuicPeerStreamAttempt::new(self.runtime.clone(), rec, self.parent_id());

        Ok(attempt)
    }

    /// Accepts incoming receive streams, this will always return an [QuicReceiveStreamAttempt] even if
    /// there are no pending streams.
    ///
    /// There's also a chance that an accept will successfully get a stream even if there aren't
    /// any pending streams due to network timings.
    ///
    /// Returns an error if the async communication channel errors out due to being full.
    pub fn accept_receive_stream(
        &mut self,
    ) -> Result<QuicReceiveStreamAttempt, ConnectionCommandError> {
        self.pending_stream.set_false();

        let (send, rec) = oneshot::channel();

        let cmd = ConnectionCommand::AcceptReceive { respond_to: send };
        let send_res = self.conn_command_channel.try_send(cmd);

        if let Err(err) = send_res {
            return Err(err.into());
        }

        let attempt =
            QuicReceiveStreamAttempt::new(self.runtime.clone(), rec, self.parent_id());

        Ok(attempt)
    }

    /// Accepts incoming bidirectional streams, this will always return an [QuicBidirectionalStreamAttempt] even if
    /// there are no pending streams.
    ///
    /// There's also a chance that an accept will successfully get a stream even if there aren't
    /// any pending streams due to network timings.
    ///
    /// Returns an error if the async communication channel errors out due to being full.
    pub fn accept_bidirectional_stream(
        &mut self,
    ) -> Result<QuicBidirectionalStreamAttempt, ConnectionCommandError> {
        self.pending_stream.set_false();

        let (send, rec) = oneshot::channel();

        let cmd = ConnectionCommand::AcceptBidirectional { respond_to: send };
        let send_res = self.conn_command_channel.try_send(cmd);

        if let Err(err) = send_res {
            return Err(err.into());
        }

        let attempt = QuicBidirectionalStreamAttempt::new(
            self.runtime.clone(),
            rec,
            self.parent_id(),
        );

        Ok(attempt)
    }

    /// Attempts to open a new bidirectional stream to be accepted by the remote peer.
    pub fn open_bidrectional_stream(
        &mut self,
    ) -> Result<QuicBidirectionalStreamAttempt, ConnectionCommandError> {
        let task = ConnectionHandleTask::new(
            self.conn_handle.clone(),
            self.orchestrator.clone(),
            self.is_open.clone(),
            self.connection_id,
        );

        let join = self.runtime.spawn(task.open_bidirectional());

        Ok(QuicBidirectionalStreamAttempt::new(
            self.runtime.clone(),
            join,
            self.parent_id(),
        ))
    }

    /// Attempts to open a new send stream to be accepted by the remote peer.
    pub fn open_send_stream(
        &mut self,
    ) -> Result<QuicSendStreamAttempt, ConnectionCommandError> {
        let task = ConnectionHandleTask::new(
            self.conn_handle.clone(),
            self.orchestrator.clone(),
            self.is_open.clone(),
            self.connection_id,
        );

        let join = self.runtime.spawn(task.open_send());

        Ok(QuicSendStreamAttempt::new(
            self.runtime.clone(),
            join,
            self.parent_id(),
        ))
    }

    #[tracing::instrument(skip(self), fields(connection_id = %self.connection_id, remote_addr = ?self.conn_handle.remote_addr()))]
    pub fn close(&self, code: application::Error) {
        if !self.is_open() {
            return;
        }

        let res = self
            .conn_command_channel
            .try_send(ConnectionCommand::Close(code));

        let Err(err) = res else {
            return;
        };

        match err {
            mpsc::error::TrySendError::Full(_) => {
                error!(
                    "Unable to normally close connection due to full communication channel. Forcefully closing connection..."
                );
            }
            mpsc::error::TrySendError::Closed(_) => error!(
                "Connection command channel is closed but our connection is still open? Something has gone horribly wrong, this is a bug. Forcefully closing connection..."
            ),
        }

        self.conn_handle.close(code);
    }

    /// Returns true if the connection is still open.
    pub fn is_open(&self) -> bool {
        !self.task_state.is_finished() && self.is_open.get()
    }

    /// Returns true if calling [accept_stream][Self::accept_stream()] will return something different.
    /// This doesn't necessarily mean there's a pending connection, just that
    /// calling accept() will return something different.
    ///
    /// This flag will always return true, if [accept_stream][Self::accept_stream()]
    /// hasn't been called yet. This is because the waker isn't registered until accept
    /// is called.
    ///
    /// This also doesn't necessarily mean the [accept_bidirectional_stream][Self::accept_bidirectional_stream()]
    /// and [accept_receive_stream][Self::accept_receive_stream()] will both return something new.
    ///
    /// This flag is set by the general [s2n_quic::Connection::poll_accept()] poll,
    /// so the specific variants *could* be a receive stream, bidirectional stream, or
    /// it could be an error.
    pub fn should_poll_accept(&self) -> bool {
        self.pending_stream.get() && self.is_open()
    }

    /// Gets the disconnect reason if the stream has closed.
    /// Returns `None` if the stream is still open.
    pub fn get_disconnect_reason(&mut self) -> Option<ConnectionDisconnectReason> {
        self.task_state.get_disconnect_reason()
    }

    /// Gets the ID information for the parent client or server for this connection
    pub fn parent_id(&self) -> QuicParentId {
        self.connection_id.parent_id()
    }

    /// Gets the ID information for both the parent and this connection
    pub fn id(&self) -> ConnectionId {
        self.connection_id
    }
}
