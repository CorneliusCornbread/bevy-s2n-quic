use bevy::log::{
    info,
    tracing::{self},
    warn,
};
use s2n_quic::{
    Connection, application,
    connection::{Error as ConnectionError, Handle as ConnectionHandle},
    stream::PeerStream,
};
use std::{error::Error, fmt, net::SocketAddr, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    select,
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot,
    },
    time::timeout,
};

use crate::common::{
    attempt::TaskError,
    connection::{
        ConnectionResponse,
        disconnect::{ConnectionDisconnectReason, ConnectionErrorDisconnected},
        id::ConnectionId,
        open_flag::OpenFlag,
        stream_flag::StreamFlag,
    },
    orchestrator::{self, handle::OrchestratorHandle},
    stream::{QuicPeerStream, receive::QuicReceiveStream, send::QuicSendStream},
    task_state::{JoinHandleState, OnceLockState},
};

/// Timeout used when the buffered stream type doesn't match what the command
/// asked for, so we do a short poll to see if the right type is available.
const ACCEPT_MISMATCH_TIMEOUT: Duration = Duration::from_millis(1);

/// Connection command channel message
const CONN_CMD_MSG: &str = "Connection command channel";

pub(crate) enum ConnectionCommand {
    AcceptReceive {
        respond_to: oneshot::Sender<ConnectionResponse<QuicReceiveStream>>,
    },
    AcceptBidirectional {
        respond_to:
            oneshot::Sender<ConnectionResponse<(QuicReceiveStream, QuicSendStream)>>,
    },
    Accept {
        respond_to: oneshot::Sender<ConnectionResponse<QuicPeerStream>>,
    },
    Close(application::Error),
}

// TODO: This could be made public and used elsewhere as a async way to open new connections
// or get information about a connection
#[derive(Debug)]
pub(crate) struct ConnectionHandleTask {
    connection: ConnectionHandle,
    orchestrator: OrchestratorHandle,
    is_open: OpenFlag,
    remote_addr: Result<SocketAddr, ConnectionError>,
    connection_id: ConnectionId,
}

impl fmt::Display for ConnectionHandleTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConnectionHandleTask(connection_id: {}, remote: {:?}, open: {})",
            self.connection_id,
            self.remote_addr,
            self.is_open.get(),
        )
    }
}

impl ConnectionHandleTask {
    pub(super) fn new(
        connection: ConnectionHandle,
        orchestrator: OrchestratorHandle,
        is_open: OpenFlag,
        connection_id: ConnectionId,
    ) -> Self {
        let remote_addr = connection.remote_addr();

        Self {
            connection,
            is_open,
            remote_addr,
            connection_id,
            orchestrator,
        }
    }

    /// This always will return Some in the Ok case,
    /// this is done to allow accept and open to have the same functionality
    #[tracing::instrument]
    pub(crate) async fn open_bidirectional(
        mut self,
    ) -> Result<Option<(QuicReceiveStream, QuicSendStream)>, TaskError> {
        let bidir_res = self.connection.open_bidirectional_stream().await;

        match bidir_res {
            Ok(stream) => {
                let (rec_stream, send_stream) = stream.split();

                let quic_send = QuicSendStream::new(
                    Handle::current(),
                    self.orchestrator.clone(),
                    send_stream,
                    self.connection_id.parent_id(),
                );
                let quic_rec = QuicReceiveStream::new(
                    Handle::current(),
                    self.orchestrator.clone(),
                    rec_stream,
                    self.connection_id.parent_id(),
                );

                Ok(Some((quic_rec, quic_send)))
            }
            Err(err) => Err(err.into()),
        }
    }

    /// This always will return Some in the Ok case,
    /// this is done to allow accept and open to have the same functionality
    #[tracing::instrument]
    pub(crate) async fn open_send(mut self) -> Result<Option<QuicSendStream>, TaskError> {
        let send_res = self.connection.open_send_stream().await;

        match send_res {
            Ok(stream) => {
                let quic_send = QuicSendStream::new(
                    Handle::current(),
                    self.orchestrator.clone(),
                    stream,
                    self.connection_id.parent_id(),
                );
                Ok(Some(quic_send))
            }
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionTask {
    connection: Connection,
    cmd_receiver: mpsc::Receiver<ConnectionCommand>,
    disconnect_flag: Option<ConnectionDisconnectReason>,
    is_open: OpenFlag,
    pending_stream: Arc<StreamFlag>,
    connection_id: ConnectionId,
    task_state: OnceLockState<ConnectionDisconnectReason>,
    /// Holds a stream that arrived before a matching command was ready to consume it.
    buffered_stream: Option<PeerStream>,
    orchestrator: OrchestratorHandle,
}

impl ConnectionTask {
    pub(crate) fn new(
        connection: Connection,
        cmd_receiver: mpsc::Receiver<ConnectionCommand>,
        connection_id: ConnectionId,
        is_open: OpenFlag,
        pending_stream: Arc<StreamFlag>,
        task_state: OnceLockState<ConnectionDisconnectReason>,
        orchestrator: OrchestratorHandle,
    ) -> Self {
        Self {
            connection,
            cmd_receiver,
            disconnect_flag: None,
            is_open,
            pending_stream,
            connection_id,
            buffered_stream: None,
            task_state,
            orchestrator,
        }
    }

    pub(crate) fn id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(crate) fn close(&self, error_code: application::Error) {
        self.connection.close(error_code);
    }

    #[tracing::instrument(
        name = "quic_connection_task"
        skip(self),
        fields(
            connection_id = %self.connection_id,
            remote_address = ?self.connection.remote_addr()
        )
    )]
    pub(crate) async fn start(mut self) -> ConnectionDisconnectReason {
        info!("New connection opened");

        while self.disconnect_flag.is_none() {
            self.poll_once().await;
        }

        self.disconnect_flag
            .unwrap_or(ConnectionDisconnectReason::InternalError(Arc::new(
                MissingErrorData,
            )))
    }

    pub(crate) async fn poll_once(&mut self) -> &Option<ConnectionDisconnectReason> {
        if self.disconnect_flag.is_some() {
            return &self.disconnect_flag;
        }

        // If we have a buffered stream, we only need to wait for a command
        // that will consume it.
        if self.buffered_stream.is_some() {
            match self.cmd_receiver.recv().await {
                Some(cmd) => {
                    let res = self.handle_command(cmd).await;
                    self.handle_cmd_result(res).await;
                }
                None => {
                    self.disconnect_flag =
                        Some(ConnectionDisconnectReason::MspcChannelClosed {
                            channel_name: CONN_CMD_MSG,
                        });
                }
            }
        } else {
            // No buffered stream: race commands against an incoming stream.
            select! {
                biased;

                cmd_opt = self.cmd_receiver.recv() => {
                    match cmd_opt {
                        Some(cmd) => {
                            let res = self.handle_command(cmd).await;
                            self.handle_cmd_result(res).await;
                        }
                        None => {
                            self.disconnect_flag = Some(
                                ConnectionDisconnectReason::MspcChannelClosed {
                                    channel_name: CONN_CMD_MSG
                                },
                            );
                        }
                    }
                }

                accept_res = self.connection.accept() => {
                    match accept_res {
                        Ok(Some(stream)) => {
                            // Buffer it, the next command will consume it.
                            self.buffer_stream(stream);
                        }
                        Ok(None) => {
                            self.disconnect_flag = Some(
                                ConnectionDisconnectReason::PeerClosed
                            );
                        }
                        Err(err) => {
                            if err.is_closed() {
                                self.is_open.set_closed();
                            }
                            self.disconnect_flag =
                                Some(ConnectionDisconnectReason::ConnectionError(err));
                        }
                    }
                }
            }
        }

        if let Some(disconnect) = &self.disconnect_flag {
            let _ = self.task_state.set(disconnect.clone());
        }

        &self.disconnect_flag
    }

    async fn handle_command(
        &mut self,
        cmd: ConnectionCommand,
    ) -> Result<(), ConnectionError> {
        match cmd {
            ConnectionCommand::Accept { respond_to } => {
                if let Some(stream) = self.buffered_stream.take() {
                    let peer_stream = QuicPeerStream::new(
                        Handle::current(),
                        self.orchestrator.clone(),
                        stream,
                        self.connection_id.parent_id(),
                    );
                    if respond_to.send(Ok(Some(peer_stream))).is_err() {
                        warn!(
                            "Accept response handler closed before stream could be sent."
                        );
                    }
                    Ok(())
                } else {
                    let _ = respond_to.send(Ok(None));
                    Ok(())
                }
            }

            ConnectionCommand::AcceptReceive { respond_to } => {
                match self.buffered_stream.take() {
                    Some(PeerStream::Receive(stream)) => {
                        let rec = QuicReceiveStream::new(
                            Handle::current(),
                            self.orchestrator.clone(),
                            stream,
                            self.connection_id.parent_id(),
                        );
                        if respond_to.send(Ok(Some(rec))).is_err() {
                            warn!(
                                "Accept receive response handler closed before stream \
                                could be sent."
                            );
                        }
                        Ok(())
                    }
                    Some(other) => {
                        // Wrong type, put it back and do a short poll for the right one.
                        self.buffered_stream = Some(other);
                        self.accept_receive(respond_to).await
                    }
                    None => self.accept_receive(respond_to).await,
                }
            }

            ConnectionCommand::AcceptBidirectional { respond_to } => {
                match self.buffered_stream.take() {
                    Some(PeerStream::Bidirectional(stream)) => {
                        let (rec, send) = stream.split();
                        let rec = QuicReceiveStream::new(
                            Handle::current(),
                            self.orchestrator.clone(),
                            rec,
                            self.connection_id.parent_id(),
                        );
                        let send = QuicSendStream::new(
                            Handle::current(),
                            self.orchestrator.clone(),
                            send,
                            self.connection_id.parent_id(),
                        );
                        if respond_to.send(Ok(Some((rec, send)))).is_err() {
                            warn!(
                                "Accept bidir response handler closed before stream \
                                could be sent."
                            );
                        }
                        Ok(())
                    }
                    Some(other) => {
                        self.buffered_stream = Some(other);
                        self.accept_bidirectional(respond_to).await
                    }
                    None => self.accept_bidirectional(respond_to).await,
                }
            }

            ConnectionCommand::Close(code) => {
                self.connection.close(code);
                Ok(())
            }
        }
    }

    async fn accept_receive(
        &mut self,
        respond_to: oneshot::Sender<ConnectionResponse<QuicReceiveStream>>,
    ) -> Result<(), ConnectionError> {
        let res = timeout(
            ACCEPT_MISMATCH_TIMEOUT,
            self.connection.accept_receive_stream(),
        )
        .await;

        let Ok(accept_res) = res else {
            let _ = respond_to.send(Ok(None));
            return Ok(());
        };

        match accept_res {
            Ok(opt) => {
                let mapped = opt.map(|s| {
                    QuicReceiveStream::new(
                        Handle::current(),
                        self.orchestrator.clone(),
                        s,
                        self.connection_id.parent_id(),
                    )
                });
                if respond_to.send(Ok(mapped)).is_err() {
                    warn!(
                        "Accept receive stream opened but response handler was already \
                        closed."
                    );
                }
                Ok(())
            }
            Err(err) => {
                if respond_to
                    .send(Err(TaskError::ConnectionFailed(err)))
                    .is_err()
                {
                    warn!(
                        "Accept receive errored and response handler was already closed."
                    );
                }
                if err.is_closed() {
                    self.is_open.set_closed();
                }
                Err(err)
            }
        }
    }

    async fn accept_bidirectional(
        &mut self,
        respond_to: oneshot::Sender<
            ConnectionResponse<(QuicReceiveStream, QuicSendStream)>,
        >,
    ) -> Result<(), ConnectionError> {
        let res = timeout(
            ACCEPT_MISMATCH_TIMEOUT,
            self.connection.accept_bidirectional_stream(),
        )
        .await;

        let Ok(accept_res) = res else {
            let _ = respond_to.send(Ok(None));
            return Ok(());
        };

        match accept_res {
            Ok(opt) => {
                let mapped = opt.map(|bidir| {
                    let (rec, send) = bidir.split();
                    let rec = QuicReceiveStream::new(
                        Handle::current(),
                        self.orchestrator.clone(),
                        rec,
                        self.connection_id.parent_id(),
                    );
                    let send = QuicSendStream::new(
                        Handle::current(),
                        self.orchestrator.clone(),
                        send,
                        self.connection_id.parent_id(),
                    );
                    (rec, send)
                });
                if respond_to.send(Ok(mapped)).is_err() {
                    warn!(
                        "Accept bidir stream opened but response handler was already \
                        closed."
                    );
                }
                Ok(())
            }
            Err(err) => {
                if respond_to
                    .send(Err(TaskError::ConnectionFailed(err)))
                    .is_err()
                {
                    warn!(
                        "Accept bidir errored and response handler was already closed."
                    );
                }
                if err.is_closed() {
                    self.is_open.set_closed();
                }
                Err(err)
            }
        }
    }

    async fn handle_cmd_result(&mut self, cmd_res: Result<(), ConnectionError>) {
        let Err(err) = cmd_res else {
            return;
        };

        self.disconnect_flag = Some(ConnectionDisconnectReason::ConnectionError(err));
    }

    fn buffer_stream(&mut self, stream: PeerStream) {
        self.pending_stream.set_true();
        self.buffered_stream = Some(stream);
    }
}

/// Errors that arise when communicaitons with the async connection task fail.
#[derive(Debug, Error, Clone, Copy)]
pub enum ConnectionCommandError {
    /// The communication channel for the async task is full.
    #[error("The communication channel for the async connection task is full.")]
    Full,
    /// The communication channel for the async task has been closed.
    ///
    /// This is likely due to the async task for the connection quitting unexpectedly.
    #[error("The communication channel for the async connection task has been closed.")]
    Closed,
}

impl<T> From<TrySendError<T>> for ConnectionCommandError {
    fn from(value: TrySendError<T>) -> Self {
        match value {
            TrySendError::Full(_) => Self::Full,
            TrySendError::Closed(_) => Self::Closed,
        }
    }
}

#[derive(Debug)]
pub struct MissingErrorData;

impl fmt::Display for MissingErrorData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Connection task exited without a given reason. This is a bug!"
        )
    }
}

impl Error for MissingErrorData {}
