use bevy::{
    ecs::component::Component,
    log::{
        error, info,
        tracing::{self},
        warn,
    },
};
use bytes::Bytes;
use s2n_quic::{application, stream::SendStream};
use std::error::Error;
use tokio::{
    runtime::Handle,
    select,
    sync::mpsc::{self, Receiver, Sender, error::TrySendError},
};

use crate::common::{
    HandleChannelError, QuicParentId,
    connection::disconnect::ConnectionDisconnectReason,
    orchestrator::{self, ORCHESTRATOR_ERROR_CODE, handle::OrchestratorHandle},
    stream::{id::StreamId, task_state::StreamTaskState},
    task_state::{OnceLockState, TaskState},
};

type AddrResult = Result<std::net::SocketAddr, s2n_quic::connection::Error>;

/// How many errors can be sent at a single time without being dropped
const DEBUG_CHANNEL_SIZE: usize = 32;
/// How many commands can be sent to the send socket without being processed before being dropped
const CONTROL_CHANNEL_SIZE: usize = 32;
/// How many messages can sit between async and bevy before being dropped
const OUTBOUND_CHANNEL_SIZE: usize = 512;

/// Minimum size of the send buffer of Bytes chunks we can receive at once is to send to bevy
const MIN_OUTBOUND_BUF_SIZE: usize = 64;
/// Maximum size of the send buffer of Bytes chunks we can receive at once is to send to bevy
const MAX_OUTBOUND_BUF_SIZE: usize = 128;

const OUTBOUND_CHANNEL_NAME: &str = "Outbound channel";
const CONTROL_CHANNEL_NAME: &str = "Control channel";

#[derive(Debug, Component)]
pub struct QuicSendStream {
    task_state: OnceLockState<ConnectionDisconnectReason>,
    outbound_data: Sender<Bytes>,
    outbound_control: Sender<SendControlMessage>,
    send_errors: Receiver<Box<dyn Error + Send + Sync>>,
    stream_id: StreamId,
    orchestrator: OrchestratorHandle,
}

impl QuicSendStream {
    pub fn new(
        runtime: Handle,
        orchestrator: OrchestratorHandle,
        send: SendStream,
        parent_id: QuicParentId,
    ) -> Self {
        let stream_id = StreamId::new(parent_id, send.id());

        let (send_error_sender, send_errors) = mpsc::channel(DEBUG_CHANNEL_SIZE);
        let (outbound_control, outbound_control_receiver) =
            mpsc::channel(CONTROL_CHANNEL_SIZE);
        let (outbound_data, outbound_data_receiver) =
            mpsc::channel(OUTBOUND_CHANNEL_SIZE);

        let mut task_state = OnceLockState::new();

        let task = SendTask::new(
            send,
            outbound_control_receiver,
            task_state.clone(),
            outbound_data_receiver,
            send_error_sender,
            stream_id,
        );

        let res = orchestrator.push_send(task);

        if let Err(e) = res {
            error!(
                "Unable to push new task for stream {}, with reason: {}",
                stream_id, e
            );

            let _ = task_state.set(ConnectionDisconnectReason::OrchestratorError);

            match e {
                mpsc::error::TrySendError::Full(mut task)
                | mpsc::error::TrySendError::Closed(mut task) => {
                    task.early_close();
                }
            }
        }

        Self {
            task_state,
            outbound_data,
            outbound_control,
            send_errors,
            stream_id,
            orchestrator,
        }
    }

    /// Returns `Some(())` in the event the close event was successful, if it wasn't
    /// it's due to the Receiver of the message being dropped. In which case
    /// it's likely the async task has been shut down, already quit, or crashed.
    pub fn close(&mut self) -> Option<()> {
        self.outbound_control
            .blocking_send(SendControlMessage::CloseAndQuit)
            .ok()
    }

    /// Returns `Some(())` in the event the flush event was successful, if it wasn't
    /// it's due to the Receiver of the message being dropped. In which case
    /// it's likely the async task has been shut down, already quit, or crashed.
    pub fn flush(&mut self) -> Option<()> {
        self.outbound_control
            .blocking_send(SendControlMessage::Flush)
            .ok()
    }

    /// Checks if the async task for the stream is still running, in which case
    /// the stream should still be open, if not the task should finish on its own.
    pub fn is_open(&self) -> bool {
        !self.task_state.is_finished()
    }

    /// Tries to send one set of bytes
    pub fn send(&mut self, data: Bytes) -> Result<(), TrySendError<Bytes>> {
        self.outbound_data.try_send(data)
    }

    /// Take a vector of bytes and send bytes until an error is hit
    /// or until the vector is emptied.
    pub fn send_many_drain(
        &mut self,
        data: &mut Vec<Bytes>,
    ) -> Result<(), TrySendError<Bytes>> {
        let mut sent_count = 0;
        let mut res = Ok(());

        for item in data.iter() {
            res = self.outbound_data.try_send(item.clone());
            if res.is_err() {
                break;
            }

            sent_count += 1;
        }

        if sent_count == data.len() {
            data.clear();
        } else {
            data.drain(..sent_count);
        }

        res
    }

    /// Outputs any outstanding errors that have happened on the
    /// async side of this stream.
    pub fn log_outstanding_errors(&mut self) {
        while !self.send_errors.is_empty() {
            let Some(err) = self.send_errors.blocking_recv() else {
                continue;
            };

            error!("Sender ID: {}, encountered error:\n{}", self.stream_id, err);
        }
    }

    /// Gets the disconnect reason if the stream has closed.
    /// Returns `None` if the stream is still open.
    pub fn get_disconnect_reason(&mut self) -> Option<ConnectionDisconnectReason> {
        self.task_state.get_disconnect_reason()
    }

    /// Gets the ID information for the parent client or server for this stream
    pub fn parent_id(&self) -> QuicParentId {
        self.stream_id.parent_id()
    }

    /// Gets the the full ID information for this stream.
    pub fn id(&self) -> StreamId {
        self.stream_id
    }
}

#[derive(Debug)]
pub(crate) struct SendTask {
    send: SendStream,
    control: Receiver<SendControlMessage>,
    task_state: OnceLockState<ConnectionDisconnectReason>,
    outbound_receiver: Receiver<Bytes>,
    send_errors: Sender<Box<dyn Error + Send + Sync>>,
    disconnect_flag: Option<ConnectionDisconnectReason>,
    addr: AddrResult,
    stream_id: StreamId,
    send_buf: Vec<Bytes>,
}

impl SendTask {
    fn new(
        send: SendStream,
        control: Receiver<SendControlMessage>,
        task_state: OnceLockState<ConnectionDisconnectReason>,
        outbound_receiver: Receiver<Bytes>,
        send_errors: Sender<Box<dyn Error + Send + Sync>>,
        stream_id: StreamId,
    ) -> Self {
        let addr = send.connection().local_addr();
        Self {
            send,
            control,
            task_state,
            outbound_receiver,
            send_errors,
            disconnect_flag: None,
            addr,
            stream_id,
            send_buf: Vec::with_capacity(MIN_OUTBOUND_BUF_SIZE),
        }
    }

    pub(crate) fn id(&self) -> StreamId {
        self.stream_id
    }

    pub(crate) fn early_close(&mut self) {
        let _ = self.send.finish();
    }

    #[tracing::instrument(
        name = "quic_send_poll"
        skip(self),
        fields(stream_id = %self.stream_id, remote_address = ?self.addr)
    )]
    pub(crate) async fn poll_once(&mut self) -> &Option<ConnectionDisconnectReason> {
        if self.disconnect_flag.is_some() {
            return &self.disconnect_flag;
        }

        select! {
            count = self.outbound_receiver.recv_many(&mut self.send_buf, MAX_OUTBOUND_BUF_SIZE) => {
                // channel closed
                if count == 0 {
                    warn!(
                        "Outbound send channel was closed by the remote peer."
                    );

                    self.disconnect_flag = Some(ConnectionDisconnectReason::MspcChannelClosed{channel_name: OUTBOUND_CHANNEL_NAME})
                }

                let err_opt = self.send.send_vectored(&mut self.send_buf[..count]).await;
                self.send_buf.clear();

                if let Err(err) = err_opt {
                    match err {
                        s2n_quic::stream::Error::InvalidStream { source, .. }
                        | s2n_quic::stream::Error::SendAfterFinish { source, .. } => {
                            error!(
                                "Send stream is in an invalid state, quitting:\n{}",
                                source
                            );
                            self.disconnect_flag = Some(ConnectionDisconnectReason::InvalidStream)
                        }

                        s2n_quic::stream::Error::StreamReset {
                            error, source: _, ..
                        } => {
                            error!(
                                "Send stream has encountered a stream reset:\n{}",
                                error
                            );
                            self.disconnect_flag = Some(ConnectionDisconnectReason::Reset(error));
                        }

                        _ => {
                            error!(
                                "Send stream error:\n{}",
                                err
                            );
                        }
                    }

                    self.send_errors.try_send(Box::new(err)).handle_err();
                }
            }

            cmd_opt = self.control.recv() => {
                if let Some(cmd) = cmd_opt {
                    match cmd {
                        SendControlMessage::CloseAndQuit => {
                            let res = self.send.close().await;

                            if let Err(e) = res {
                                error!(
                                    "Send stream errored when closing stream:\n{}",
                                    e
                                );

                                self.send_errors.try_send(Box::new(e)).handle_err();
                            }

                            self.disconnect_flag = Some(ConnectionDisconnectReason::UserClosed);
                        }

                        SendControlMessage::Flush => {
                            let res = self.send.flush().await;

                            if let Err(e) = res {
                                error!(
                                    "Send stream errored when flushing stream:\n{}",
                                    e
                                );

                                self.send_errors.try_send(Box::new(e)).handle_err();
                            }
                        }
                    }
                }
                else {
                    // Control channel has been dropped
                    info!(
                        "Control channel has been dropped. Quitting...",
                    );
                    self.disconnect_flag = Some(ConnectionDisconnectReason::MspcChannelClosed{channel_name: CONTROL_CHANNEL_NAME})
                };
            }
        }

        // Disconnecting
        if let Some(disconnect) = &self.disconnect_flag {
            let _ = self.task_state.set(disconnect.clone());
            let _ = self.send.close().await;

            info!("Send stream has been closed");

            let dropped_count = self.outbound_receiver.len();

            if dropped_count > 0 {
                warn!(
                    "Send stream dropped {} messages, this will result in loss of data being sent",
                    dropped_count
                )
            }
        }

        &self.disconnect_flag
    }
}

enum SendControlMessage {
    CloseAndQuit,
    Flush,
}
