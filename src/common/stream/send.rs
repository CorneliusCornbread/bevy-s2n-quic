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
use std::{error::Error, fmt, future::Future};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender, error::TrySendError},
};

use crate::common::{
    HandleChannelError, QuicParentId,
    connection::{disconnect::ConnectionDisconnectReason, id::ConnectionId},
    spawner::{QuicTask, TaskSpawner},
    stream::id::StreamId,
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

#[derive(Debug, Component)]
pub struct QuicSendStream {
    task_state: OnceLockState<ConnectionDisconnectReason>,
    outbound_data: Sender<Bytes>,
    outbound_control: Sender<SendControlMessage>,
    send_errors: Receiver<Box<dyn Error + Send + Sync>>,
    stream_id: StreamId,
}

impl QuicSendStream {
    pub fn new(spawner: TaskSpawner, send: SendStream, conn_id: ConnectionId) -> Self {
        let stream_id = StreamId::new(conn_id, send.id());

        let (send_error_sender, send_errors) = mpsc::channel(DEBUG_CHANNEL_SIZE);
        let (outbound_control, outbound_control_receiver) =
            mpsc::channel(CONTROL_CHANNEL_SIZE);
        let (outbound_data, outbound_data_receiver) =
            mpsc::channel(OUTBOUND_CHANNEL_SIZE);

        let task_state = OnceLockState::new();

        let task = SendTask::new(
            send,
            outbound_control_receiver,
            outbound_data_receiver,
            send_error_sender,
            stream_id,
        );

        spawner.spawn(task, task_state.clone());

        Self {
            task_state,
            outbound_data,
            outbound_control,
            send_errors,
            stream_id,
        }
    }

    /// Queues a close request for the async task without blocking.
    ///
    /// Data queued with [`Self::send`] before the close request is still sent.
    /// Once the task handles the request, further sends are rejected.
    ///
    /// Returns `Some(())` if the request was queued. `None` means either the
    /// control channel is full (the request was dropped, try again later) or
    /// the receiver was dropped, in which case the async task has likely
    /// been shut down, already quit, or crashed. Use [`Self::is_open`] to
    /// tell those apart.
    pub fn close(&mut self) -> Option<()> {
        self.outbound_control
            .try_send(SendControlMessage::CloseAndQuit)
            .ok()
    }

    /// Queues a flush request for the async task without blocking.
    ///
    /// Data queued with [`Self::send`] before the flush request is sent first.
    ///
    /// Returns `Some(())` if the request was queued. `None` means either the
    /// control channel is full (the request was dropped, try again later) or
    /// the receiver was dropped, in which case the async task has likely
    /// been shut down, already quit, or crashed. Use [`Self::is_open`] to
    /// tell those apart.
    pub fn flush(&mut self) -> Option<()> {
        self.outbound_control
            .try_send(SendControlMessage::Flush)
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
        while let Ok(err) = self.send_errors.try_recv() {
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
    outbound_receiver: Receiver<Bytes>,
    send_errors: Sender<Box<dyn Error + Send + Sync>>,
    addr: AddrResult,
    stream_id: StreamId,
    send_buf: Vec<Bytes>,
}

impl fmt::Display for SendTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "send stream {}", self.stream_id)
    }
}

impl QuicTask for SendTask {
    fn run(self) -> impl Future<Output = ConnectionDisconnectReason> + Send + 'static {
        SendTask::run(self)
    }

    fn reject(mut self) {
        let _ = self.send.finish();
    }
}

impl SendTask {
    fn new(
        send: SendStream,
        control: Receiver<SendControlMessage>,
        outbound_receiver: Receiver<Bytes>,
        send_errors: Sender<Box<dyn Error + Send + Sync>>,
        stream_id: StreamId,
    ) -> Self {
        let addr = send.connection().local_addr();
        Self {
            send,
            control,
            outbound_receiver,
            send_errors,
            addr,
            stream_id,
            send_buf: Vec::with_capacity(MIN_OUTBOUND_BUF_SIZE),
        }
    }

    #[tracing::instrument(
        name = "quic_send_task",
        level = "debug",
        skip(self),
        fields(stream_id = %self.stream_id, remote_address = ?self.addr)
    )]
    async fn run(mut self) -> ConnectionDisconnectReason {
        let mut control_open = true;

        let reason = loop {
            select! {
                count = self.outbound_receiver.recv_many(&mut self.send_buf, MAX_OUTBOUND_BUF_SIZE) => {
                    if count == 0 {
                        info!("Outbound send channel has been closed. Quitting...");
                        break ConnectionDisconnectReason::MspcChannelClosed {
                            channel_name: OUTBOUND_CHANNEL_NAME,
                        };
                    }

                    if let Some(reason) = self.send_buffered().await {
                        break reason;
                    }
                }

                cmd_opt = self.control.recv(), if control_open => {
                    let Some(cmd) = cmd_opt else {
                        // The component was dropped. Keep going until the
                        // outbound channel is drained so queued data isn't lost.
                        control_open = false;
                        continue;
                    };

                    if let Some(reason) = self.handle_command(cmd).await {
                        break reason;
                    }
                }
            }

            tokio::task::consume_budget().await;
        };

        if !matches!(reason, ConnectionDisconnectReason::UserClosed)
            && let Err(e) = self.send.close().await
        {
            info!("Send stream errored when closing stream: {e}");
        }

        info!("Send stream has been closed");

        let dropped_count = self.outbound_receiver.len();
        if dropped_count > 0 {
            warn!(
                "Send stream dropped {} messages, this will result in loss of data being sent",
                dropped_count
            )
        }

        reason
    }

    async fn handle_command(
        &mut self,
        cmd: SendControlMessage,
    ) -> Option<ConnectionDisconnectReason> {
        match cmd {
            SendControlMessage::CloseAndQuit => {
                // Send everything queued before the close request, and refuse
                // anything queued after it.
                self.outbound_receiver.close();
                while self
                    .outbound_receiver
                    .recv_many(&mut self.send_buf, MAX_OUTBOUND_BUF_SIZE)
                    .await
                    > 0
                {
                    if let Some(reason) = self.send_buffered().await {
                        return Some(reason);
                    }
                }

                if let Err(e) = self.send.close().await {
                    error!("Send stream errored when closing stream:\n{}", e);
                    self.send_errors.try_send(Box::new(e)).handle_err();
                }

                Some(ConnectionDisconnectReason::UserClosed)
            }

            SendControlMessage::Flush => {
                // Send everything queued before the flush request first.
                let queued = self.outbound_receiver.len();
                if queued > 0 {
                    self.outbound_receiver
                        .recv_many(&mut self.send_buf, queued)
                        .await;

                    if let Some(reason) = self.send_buffered().await {
                        return Some(reason);
                    }
                }

                if let Err(e) = self.send.flush().await {
                    error!("Send stream errored when flushing stream:\n{}", e);
                    self.send_errors.try_send(Box::new(e)).handle_err();
                }

                None
            }
        }
    }

    /// Sends everything in `send_buf`, then clears it. Returns a reason if the
    /// stream can no longer be used.
    async fn send_buffered(&mut self) -> Option<ConnectionDisconnectReason> {
        let res = self.send.send_vectored(&mut self.send_buf).await;
        self.send_buf.clear();

        let Err(err) = res else {
            return None;
        };

        let reason = match err {
            s2n_quic::stream::Error::InvalidStream { source, .. }
            | s2n_quic::stream::Error::SendAfterFinish { source, .. } => {
                error!("Send stream is in an invalid state, quitting:\n{}", source);
                Some(ConnectionDisconnectReason::InvalidStream)
            }

            s2n_quic::stream::Error::StreamReset { error, .. } => {
                error!("Send stream has encountered a stream reset:\n{}", error);
                Some(ConnectionDisconnectReason::Reset(error))
            }

            s2n_quic::stream::Error::ConnectionError { error, .. } => {
                error!("Send stream connection error:\n{}", error);
                Some(ConnectionDisconnectReason::ConnectionError(error))
            }

            _ => {
                error!("Send stream error:\n{}", err);
                None
            }
        };

        self.send_errors.try_send(Box::new(err)).handle_err();

        reason
    }
}

enum SendControlMessage {
    CloseAndQuit,
    Flush,
}
