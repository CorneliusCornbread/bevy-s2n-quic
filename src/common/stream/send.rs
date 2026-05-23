use bevy::{
    ecs::component::Component,
    log::{
        error, info,
        tracing::{self},
        warn,
    },
};
use bytes::Bytes;
use s2n_quic::stream::SendStream;
use std::error::Error;
use tokio::{
    runtime::Handle,
    select,
    sync::mpsc::{self, Receiver, Sender, error::TrySendError},
};

use crate::common::{
    HandleChannelError, QuicParentId,
    connection::disconnect::ConnectionDisconnectReason,
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

#[derive(Debug, Component)]
pub struct QuicSendStream {
    task_state: OnceLockState<ConnectionDisconnectReason>,
    outbound_data: Sender<Bytes>,
    outbound_control: Sender<SendControlMessage>,
    send_errors: Receiver<Box<dyn Error + Send + Sync>>,
    stream_id: StreamId,
}

impl QuicSendStream {
    pub fn new(runtime: Handle, send: SendStream, parent_id: QuicParentId) -> Self {
        let stream_id = StreamId::new(parent_id, send.id());
        let addr = send.connection().local_addr();

        let (send_error_sender, send_errors) = mpsc::channel(DEBUG_CHANNEL_SIZE);
        let (outbound_control, outbound_control_receiver) =
            mpsc::channel(CONTROL_CHANNEL_SIZE);
        let (outbound_data, outbound_data_receiver) =
            mpsc::channel(OUTBOUND_CHANNEL_SIZE);

        let task_state = OnceLockState::new();

        let task = SendTask {
            send,
            control: outbound_control_receiver,
            outbound_receiver: outbound_data_receiver,
            send_errors: send_error_sender,
            disconnect_flag: None,
            addr,
            stream_id,
        };

        // TODO: change this to use orchestrator
        let send_task = runtime.spawn(task.start());

        Self {
            task_state,
            outbound_data,
            outbound_control,
            send_errors,
            stream_id,
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

pub(crate) struct SendTask {
    send: SendStream,
    control: Receiver<SendControlMessage>,
    outbound_receiver: Receiver<Bytes>,
    send_errors: Sender<Box<dyn Error + Send + Sync>>,
    disconnect_flag: Option<ConnectionDisconnectReason>,
    addr: AddrResult,
    stream_id: StreamId,
}

impl SendTask {
    #[tracing::instrument(
        name = "quic_send_task"
        skip(self),
        fields(stream_id = %self.stream_id, remote_address = ?self.addr)
    )]
    async fn start(mut self) -> ConnectionDisconnectReason {
        info!("Send stream opened.");

        let mut send_buf = Vec::with_capacity(MIN_OUTBOUND_BUF_SIZE);

        'running: loop {
            select! {
                count = self.outbound_receiver.recv_many(&mut send_buf, MAX_OUTBOUND_BUF_SIZE) => {
                    // channel closed
                    if count == 0 {
                        warn!(
                            "Outbound send channel was closed by the remote peer."
                        );

                        self.disconnect_flag = Some(ConnectionDisconnectReason::MspcChannelClosed{channel_name: "Outbound channel".into()})
                    }

                    let err_opt = self.send.send_vectored(&mut send_buf[..count]).await;
                    send_buf.clear();

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
                        self.disconnect_flag = Some(ConnectionDisconnectReason::MspcChannelClosed{channel_name: "Control channel".into()})
                    };
                }
            }

            if self.disconnect_flag.is_some() {
                break 'running;
            }
        }

        info!("Send stream has been closed",);

        let dropped_count = self.outbound_receiver.len();

        if dropped_count > 0 {
            warn!(
                "Send stream dropped {} messages, this will result in loss of data being sent",
                dropped_count
            )
        }

        if let Some(reason) = self.disconnect_flag {
            reason
        }
        // In theory this should never happen
        else {
            ConnectionDisconnectReason::NoReason
        }
    }
}

enum SendControlMessage {
    CloseAndQuit,
    Flush,
}
