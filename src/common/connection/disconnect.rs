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
    MspcChannelClosed {
        channel_name: &'static str,
    },
    InternalError(Arc<dyn Error + Send + Sync>),
    /// This should in theory never happen
    NoReason,
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
                DisconnectReason::ByPeer("Stream closed by peer.".to_owned())
            }
            ConnectionDisconnectReason::Reset(error) => DisconnectReason::ByError(
                anyhow!("Stream closed by reset with code: {error}"),
            ),
            ConnectionDisconnectReason::InvalidStream => {
                DisconnectReason::ByError(anyhow!("Stream is no longer valid"))
            }
            ConnectionDisconnectReason::ConnectionError(error) => {
                DisconnectReason::ByError(anyhow!(
                    "Stream has been closed due to a connection error: {error}"
                ))
            }

            ConnectionDisconnectReason::MspcChannelClosed { channel_name } => {
                DisconnectReason::ByError(anyhow!(
                    "Stream was closed due to an IPC channel \"{channel_name}\" being closed"
                ))
            }
            ConnectionDisconnectReason::InternalError(error) => {
                DisconnectReason::ByError(anyhow!(
                    "Stream was closed due to an internal error: {error}"
                ))
            }
            ConnectionDisconnectReason::NoReason => DisconnectReason::ByError(anyhow!(
                "Stream was closed without reason, this is a bug :("
            )),
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
