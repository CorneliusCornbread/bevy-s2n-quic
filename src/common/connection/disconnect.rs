use std::{error::Error, sync::Arc};

use aeronet_io::{anyhow::anyhow, connection::DisconnectReason};
use s2n_quic::connection::Error as ConnectionError;

#[derive(Clone, Debug)]
pub enum ConnectionDisconnectReason {
    UserClosed,
    PeerClosed,
    Reset(s2n_quic::application::Error),
    InvalidStream,
    ConnectionError(s2n_quic::connection::Error),
    MspcChannelClosed { channel_name: &'static str },
    OrchestratorError,
    InternalError(Arc<dyn Error + Send + Sync>),
}

impl From<Arc<dyn Error + Send + Sync>> for ConnectionDisconnectReason {
    fn from(error: Arc<dyn Error + Send + Sync>) -> Self {
        ConnectionDisconnectReason::InternalError(error)
    }
}

impl From<ConnectionDisconnectReason> for DisconnectReason {
    fn from(val: ConnectionDisconnectReason) -> Self {
        match val {
            ConnectionDisconnectReason::UserClosed => {
                DisconnectReason::ByUser("Send stream stopped by self.".to_owned())
            }
            ConnectionDisconnectReason::PeerClosed => {
                DisconnectReason::ByPeer("Connection closed by peer.".to_owned())
            }
            ConnectionDisconnectReason::Reset(error) => DisconnectReason::ByError(
                anyhow!("Connection closed by reset with code: {error}"),
            ),
            ConnectionDisconnectReason::InvalidStream => {
                DisconnectReason::ByError(anyhow!("Stream is no longer valid"))
            }
            ConnectionDisconnectReason::ConnectionError(error) => {
                DisconnectReason::ByError(anyhow!(
                    "Connection has been closed due to a connection error: {error}"
                ))
            }
            ConnectionDisconnectReason::MspcChannelClosed { channel_name } => {
                DisconnectReason::ByError(anyhow!(
                    "Connection was closed due to IPC channel \"{channel_name}\" being closed"
                ))
            }
            ConnectionDisconnectReason::OrchestratorError => DisconnectReason::ByError(
                anyhow!("Orchestrator is unable to handle the connection"),
            ),
            ConnectionDisconnectReason::InternalError(error) => {
                DisconnectReason::ByError(anyhow!(
                    "Connection was closed due to an internal error: {error}"
                ))
            }
        }
    }
}

pub trait ConnectionErrorDisconnected {
    /// Returns `true` if this error represents a closed or unrecoverable connection.
    fn is_closed(&self) -> bool;
}

impl ConnectionErrorDisconnected for ConnectionError {
    fn is_closed(&self) -> bool {
        matches!(
            self,
            ConnectionError::Closed { .. }
                | ConnectionError::Transport { .. }
                | ConnectionError::Application { .. }
                | ConnectionError::EndpointClosing { .. }
                | ConnectionError::IdleTimerExpired { .. }
                | ConnectionError::NoValidPath { .. }
        )
    }
}
