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
use std::error::Error;
use tokio::{
    runtime::Handle,
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::Instant as TokioInstant,
};

use crate::common::{
    HandleChannelError, QuicParentId,
    stream::{disconnect::StreamDisconnectReason, id::StreamId},
    task_state::{OnceLockState, TaskState},
};

type AddrResult = Result<std::net::SocketAddr, s2n_quic::connection::Error>;

/// How many errors can be sent at a single time without being dropped
const DEBUG_CHANNEL_SIZE: usize = 64;
/// How many commands can be sent to the receive socket without being processed before being dropped
const CONTROL_CHANNEL_SIZE: usize = 32;
/// How many messages can sit between async and bevy before being dropped
const INBOUND_CHANNEL_SIZE: usize = 512;

/// How big the receive buffer of Bytes chunks we can receive at once is to be sent to Bevy
const INBOUND_BUFF_SIZE: usize = 128;

#[derive(Debug, Component)]
pub struct QuicReceiveStream {
    task_state: OnceLockState<StreamDisconnectReason>,
    inbound_data: Receiver<RecvPacket>,
    inbound_control: Sender<RecControlMessage>,
    receive_errors: Receiver<Box<dyn Error + Send + Sync>>,
    stream_id: StreamId,
}

impl QuicReceiveStream {
    pub fn new(runtime: Handle, rec: ReceiveStream, parent_id: QuicParentId) -> Self {
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
            task_state: task_state.clone(),
            addr,
            stream_id,
        };

        // TODO: change this to use orchestrator
        let rec_task = runtime.spawn(task.start());

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

    /// Receive multiple packets of data and push them to the given
    /// buffer for reading.
    pub fn recv_many(&mut self, buffer: &mut Vec<RecvPacket>, limit: usize) -> usize {
        self.inbound_data.blocking_recv_many(buffer, limit)
    }

    /// Returns `true` if this stream is still open
    pub fn is_open(&self) -> bool {
        !self.task_state.is_finished()
    }

    /// Gets the disconnect reason if the stream has closed.
    /// Returns `None` if the stream is still open.
    pub fn get_disconnect_reason(&mut self) -> Option<StreamDisconnectReason> {
        self.task_state.get_disconnect_reason()
    }

    /// Notifies the peer to stop sending data on the stream.
    ///
    /// This requests the peer to finish the stream as soon as possible by issuing a reset with the provided error_code.
    pub fn stop_send(&mut self, err_code: ErrorCode) {
        let Err(_e) = self
            .inbound_control
            .blocking_send(RecControlMessage::StopSend(err_code))
        else {
            return;
        };

        warn!(
            "Stop_send() called on stopped connection with ID: {}.",
            self.stream_id
        );
    }

    /// Outputs any outstanding errors that have happened on the
    /// async side of this stream.
    pub fn log_outstanding_errors(&mut self) {
        while !self.receive_errors.is_empty() {
            let Some(err) = self.receive_errors.blocking_recv() else {
                continue;
            };

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

struct RecTask {
    rec: ReceiveStream,
    control: Receiver<RecControlMessage>,
    inbound_sender: Sender<RecvPacket>,
    receive_errors: Sender<Box<dyn Error + Send + Sync>>,
    disconnect_flag: Option<StreamDisconnectReason>,
    task_state: OnceLockState<StreamDisconnectReason>,
    addr: AddrResult,
    stream_id: StreamId,
}

impl RecTask {
    #[tracing::instrument(
        name = "quic_rec_task"
        skip(self),
        fields(stream_id = %self.stream_id, remote_address = ?self.addr)
    )]
    async fn start(mut self) {
        info!("Receive stream opened.");

        let mut read_buf: [Bytes; INBOUND_BUFF_SIZE] =
            std::array::from_fn(|_| Bytes::new());

        'running: loop {
            select! {
                biased;

                result = self.rec.receive_vectored(&mut read_buf) => {
                    self.handle_receive_result(&mut read_buf, result);
                }

                cmd_opt = self.control.recv() => {
                    if let Some(cmd) = cmd_opt {
                        match cmd {
                            RecControlMessage::StopSend(error_code) => {
                                self.disconnect_flag = Some(StreamDisconnectReason::UserClosed);

                                if let Err(stream_err) = self.rec.stop_sending(error_code) {
                                    warn!("Stream error on receive stop_send():\n{stream_err}");
                                }
                            }
                        }
                    }
                    else {
                        info!("Receive control channel is closed, closing receive stream.");
                        self.disconnect_flag = Some(StreamDisconnectReason::MspcChannelClosed {
                            channel_name: "Control channel"
                        })
                    };
                }
            }

            if self.disconnect_flag.is_some() {
                break 'running;
            }
        }

        self.stop_and_empty().await;

        info!("Receive stream has been closed");

        let _res = self.task_state.set(
            self.disconnect_flag
                .unwrap_or(StreamDisconnectReason::NoReason),
        );
    }

    fn handle_receive_result(
        &mut self,
        read_buf: &mut [Bytes; INBOUND_BUFF_SIZE],
        result: Result<(usize, bool), s2n_quic::stream::Error>,
    ) {
        match result {
            Ok((size, is_open)) => {
                let instant = TokioInstant::now();

                for data in &mut read_buf[..size] {
                    let payload = std::mem::take(data);

                    let packet = RecvPacket {
                        recv_at: instant.into_std(),
                        payload,
                    };

                    self.transfer_payload_data(packet);
                }

                if !is_open {
                    self.disconnect_flag = Some(StreamDisconnectReason::PeerClosed);
                }
            }
            Err(e) => {
                match e {
                    s2n_quic::stream::Error::ConnectionError { error, .. } => {
                        error!("Receive stream connection error: {error}");
                        self.disconnect_flag =
                            Some(StreamDisconnectReason::ConnectionError(error));
                    }

                    s2n_quic::stream::Error::InvalidStream { source, .. } => {
                        error!("Invalid receive stream: {source}");
                        self.disconnect_flag =
                            Some(StreamDisconnectReason::InvalidStream);
                    }

                    s2n_quic::stream::Error::StreamReset { error, source, .. } => {
                        error!("Receive stream reset: {error}, Source: {source}");
                        self.disconnect_flag = Some(StreamDisconnectReason::Reset(error));
                    }

                    _ => {
                        error!("Error when reading from receive stream: {}", e);
                    }
                }

                self.receive_errors.try_send(Box::new(e)).handle_err();
            }
        }
    }

    fn transfer_payload_data(&mut self, packet: RecvPacket) {
        let Err(inbound_err) = self.inbound_sender.try_send(packet) else {
            return;
        };

        match inbound_err {
            mpsc::error::TrySendError::Full(_) => {
                error!(
                    "The inbound receive channel, is full. The message received will be dropped."
                );
            }
            mpsc::error::TrySendError::Closed(_) => {
                warn!(
                    "The inbound receive channel, is closed. The message received will be dropped and the stream will be closed."
                );

                self.disconnect_flag = Some(StreamDisconnectReason::MspcChannelClosed {
                    channel_name: "Inbound receive channel".into(),
                });
            }
        }
    }

    async fn stop_and_empty(&mut self) {
        let _send_res = self.rec.stop_sending(ErrorCode::UNKNOWN);
        let instant = TokioInstant::now();

        // Empty out receiver
        while let Ok(Some(payload)) = self.rec.receive().await {
            let packet = RecvPacket {
                recv_at: instant.into_std(),
                payload,
            };

            self.transfer_payload_data(packet);
        }
    }
}
