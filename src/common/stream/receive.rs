use aeronet_io::packet::RecvPacket;
use bevy::{
    ecs::component::Component,
    log::{
        error, info,
        tracing::{self},
        warn,
    },
};
use bytes::Bytes;
use s2n_quic::application::Error as ErrorCode;
use s2n_quic::stream::ReceiveStream;
use std::{error::Error, fmt, future::Future};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::Instant as TokioInstant,
};

use crate::common::{
    HandleChannelError, QuicParentId,
    connection::{disconnect::ConnectionDisconnectReason, id::ConnectionId},
    spawner::{QuicTask, SPAWN_REJECTED_ERROR_CODE, TaskSpawner},
    stream::id::StreamId,
    task_state::{OnceLockState, TaskState},
};

type AddrResult = Result<std::net::SocketAddr, s2n_quic::connection::Error>;

/// How many errors can be sent at a single time without being dropped
const DEBUG_CHANNEL_SIZE: usize = 64;
/// How many commands can be sent to the receive socket without being processed before being dropped
const CONTROL_CHANNEL_SIZE: usize = 32;
/// How many messages can sit between async and bevy before the stream stops
/// reading and applies backpressure to the peer
const INBOUND_CHANNEL_SIZE: usize = 512;

/// How big the receive buffer of Bytes chunks we can receive at once is to be sent to Bevy
const INBOUND_BUFF_SIZE: usize = 128;

#[derive(Debug, Component)]
pub struct QuicReceiveStream {
    task_state: OnceLockState<ConnectionDisconnectReason>,
    inbound_data: Receiver<RecvPacket>,
    inbound_control: Sender<RecControlMessage>,
    receive_errors: Receiver<Box<dyn Error + Send + Sync>>,
    stream_id: StreamId,
}

impl QuicReceiveStream {
    pub fn new(
        spawner: TaskSpawner,
        rec: ReceiveStream,
        parent_id: ConnectionId,
    ) -> Self {
        let stream_id = StreamId::new(parent_id, rec.id());
        let addr = rec.connection().remote_addr();

        let (receive_error_sender, receive_errors) = mpsc::channel(DEBUG_CHANNEL_SIZE);
        let (inbound_control, inbound_control_receiver) =
            mpsc::channel(CONTROL_CHANNEL_SIZE);
        let (inbound_data_sender, inbound_data) = mpsc::channel(INBOUND_CHANNEL_SIZE);

        let task_state = OnceLockState::new();

        let task = RecTask {
            rec,
            control: inbound_control_receiver,
            inbound_sender: inbound_data_sender,
            receive_errors: receive_error_sender,
            disconnect_flag: None,
            addr,
            stream_id,
            read_buf: Box::new(std::array::from_fn(|_| Bytes::new())),
        };

        spawner.spawn(task, task_state.clone());

        Self {
            task_state,
            inbound_data,
            inbound_control,
            receive_errors,
            stream_id,
        }
    }

    /// Receives a single packet from the QUIC stream.
    pub fn recv(&mut self) -> Option<RecvPacket> {
        self.inbound_data.try_recv().ok()
    }

    /// Receive up to `limit` packets that are already waiting and push them
    /// to the given buffer for reading.
    ///
    /// Never blocks. Returns the number of packets pushed, which is `0` when
    /// no data is waiting or the stream is closed and drained. Use
    /// [`Self::is_open`] to tell those apart.
    pub fn recv_many(&mut self, buffer: &mut Vec<RecvPacket>, limit: usize) -> usize {
        buffer.reserve(limit.min(self.inbound_data.len()));

        let mut count = 0;
        while count < limit {
            let Ok(packet) = self.inbound_data.try_recv() else {
                break;
            };

            buffer.push(packet);
            count += 1;
        }

        count
    }

    /// Returns `true` if this stream is still open
    pub fn is_open(&self) -> bool {
        !self.task_state.is_finished()
    }

    /// Gets the disconnect reason if the stream has closed.
    /// Returns `None` if the stream is still open.
    pub fn get_disconnect_reason(&mut self) -> Option<ConnectionDisconnectReason> {
        self.task_state.get_disconnect_reason()
    }

    /// Notifies the peer to stop sending data on the stream.
    ///
    /// This requests the peer to finish the stream as soon as possible by issuing a reset with the provided error_code.
    ///
    /// Never blocks. If the control channel is full or closed the request is
    /// dropped and a warning is logged.
    pub fn stop_send(&mut self, err_code: ErrorCode) {
        let Err(e) = self
            .inbound_control
            .try_send(RecControlMessage::StopSend(err_code))
        else {
            return;
        };

        match e {
            mpsc::error::TrySendError::Full(_) => warn!(
                "Stop_send() dropped, control channel is full for stream with ID: {}.",
                self.stream_id
            ),
            mpsc::error::TrySendError::Closed(_) => warn!(
                "Stop_send() called on stopped connection with ID: {}.",
                self.stream_id
            ),
        }
    }

    /// Outputs any outstanding errors that have happened on the
    /// async side of this stream.
    pub fn log_outstanding_errors(&mut self) {
        while let Ok(err) = self.receive_errors.try_recv() {
            error!(
                "Receiver ID: {}, encountered error:\n{}",
                self.stream_id, err
            );
        }
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

enum RecControlMessage {
    StopSend(ErrorCode),
}

#[derive(Debug)]
pub(crate) struct RecTask {
    rec: ReceiveStream,
    control: Receiver<RecControlMessage>,
    inbound_sender: Sender<RecvPacket>,
    receive_errors: Sender<Box<dyn Error + Send + Sync>>,
    disconnect_flag: Option<ConnectionDisconnectReason>,
    addr: AddrResult,
    stream_id: StreamId,
    read_buf: Box<[Bytes; INBOUND_BUFF_SIZE]>,
}

impl fmt::Display for RecTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receive stream {}", self.stream_id)
    }
}

impl QuicTask for RecTask {
    fn run(self) -> impl Future<Output = ConnectionDisconnectReason> + Send + 'static {
        RecTask::run(self)
    }

    fn reject(mut self) {
        let _ = self.rec.stop_sending(SPAWN_REJECTED_ERROR_CODE.into());
    }
}

impl RecTask {
    #[tracing::instrument(
        name = "quic_rec_task",
        level = "debug",
        skip(self),
        fields(stream_id = %self.stream_id, remote_address = ?self.addr)
    )]
    async fn run(mut self) -> ConnectionDisconnectReason {
        let reason = loop {
            select! {
                biased;

                result = self.rec.receive_vectored(&mut self.read_buf[..]) => {
                    self.handle_receive_result(result).await;
                }

                cmd_opt = self.control.recv() => {
                    self.handle_control(cmd_opt);
                }
            }

            if let Some(reason) = self.disconnect_flag.take() {
                break reason;
            }

            tokio::task::consume_budget().await;
        };

        self.stop_and_empty(&reason).await;

        info!("Receive stream has been closed");

        reason
    }

    fn handle_control(&mut self, cmd_opt: Option<RecControlMessage>) {
        let Some(cmd) = cmd_opt else {
            info!("Receive control channel is closed, closing receive stream.");
            self.disconnect_flag = Some(ConnectionDisconnectReason::MspcChannelClosed {
                channel_name: "Control channel",
            });
            return;
        };

        match cmd {
            RecControlMessage::StopSend(error_code) => {
                self.disconnect_flag = Some(ConnectionDisconnectReason::UserClosed);

                if let Err(stream_err) = self.rec.stop_sending(error_code) {
                    warn!("Stream error on receive stop_send():\n{stream_err}");
                }
            }
        }
    }

    async fn handle_receive_result(
        &mut self,
        result: Result<(usize, bool), s2n_quic::stream::Error>,
    ) {
        match result {
            Ok((size, is_open)) => {
                let recv_at = TokioInstant::now().into_std();

                for i in 0..size {
                    let payload = std::mem::take(&mut self.read_buf[i]);

                    if !self.deliver(RecvPacket { recv_at, payload }).await {
                        // Stopping, the rest of the data is discarded.
                        for data in &mut self.read_buf[i + 1..size] {
                            *data = Bytes::new();
                        }
                        return;
                    }
                }

                if !is_open {
                    self.disconnect_flag = Some(ConnectionDisconnectReason::PeerClosed);
                }
            }
            Err(e) => {
                match e {
                    s2n_quic::stream::Error::ConnectionError { error, .. } => {
                        error!("Receive stream connection error: {error}");
                        self.disconnect_flag =
                            Some(ConnectionDisconnectReason::ConnectionError(error));
                    }

                    s2n_quic::stream::Error::InvalidStream { source, .. } => {
                        error!("Invalid receive stream: {source}");
                        self.disconnect_flag =
                            Some(ConnectionDisconnectReason::InvalidStream);
                    }

                    s2n_quic::stream::Error::StreamReset { error, source, .. } => {
                        error!("Receive stream reset: {error}, Source: {source}");
                        self.disconnect_flag =
                            Some(ConnectionDisconnectReason::Reset(error));
                    }

                    _ => {
                        error!("Error when reading from receive stream: {}", e);
                    }
                }

                self.receive_errors.try_send(Box::new(e)).handle_err();
            }
        }
    }

    /// Hands a packet to Bevy, waiting for room in the inbound channel. While
    /// waiting, no more data is read from the stream, so QUIC flow control
    /// pushes back on the peer.
    ///
    /// Returns `false` if the task is stopping and the packet was dropped.
    async fn deliver(&mut self, packet: RecvPacket) -> bool {
        let packet = match self.inbound_sender.try_send(packet) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Full(packet)) => packet,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.inbound_closed();
                return false;
            }
        };

        select! {
            biased;

            res = self.inbound_sender.send(packet) => {
                if res.is_err() {
                    self.inbound_closed();
                    return false;
                }
                true
            }

            cmd_opt = self.control.recv() => {
                self.handle_control(cmd_opt);
                false
            }
        }
    }

    fn inbound_closed(&mut self) {
        warn!(
            "The inbound receive channel is closed. The message received will be dropped and the stream will be closed."
        );

        self.disconnect_flag = Some(ConnectionDisconnectReason::MspcChannelClosed {
            channel_name: "Inbound receive channel",
        });
    }

    /// Stops the peer from sending and hands any data still buffered in the
    /// stream to Bevy without waiting for room in the inbound channel.
    async fn stop_and_empty(&mut self, reason: &ConnectionDisconnectReason) {
        if matches!(reason, ConnectionDisconnectReason::PeerClosed) {
            // The peer finished the stream and all of its data was delivered.
            return;
        }

        let _send_res = self.rec.stop_sending(ErrorCode::UNKNOWN);
        let recv_at = TokioInstant::now().into_std();

        while let Ok(Some(payload)) = self.rec.receive().await {
            if self
                .inbound_sender
                .try_send(RecvPacket { recv_at, payload })
                .is_err()
            {
                break;
            }
        }
    }
}
