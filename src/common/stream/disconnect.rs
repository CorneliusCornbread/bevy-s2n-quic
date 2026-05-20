use std::{error::Error, sync::Arc};

use aeronet_io::{anyhow::anyhow, connection::DisconnectReason};

#[derive(Clone, Debug)]
pub enum StreamDisconnectReason {
    UserClosed,
    PeerClosed,
    Reset(s2n_quic::application::Error),
    InvalidStream,
    ConnectionError(s2n_quic::connection::Error),
    ResourceError,
    MspcChannelClosed {
        channel_name: &'static str,
    },
    InternalError(Arc<dyn Error + Send + Sync>),
    /// This should in theory never happen
    NoReason,
}

impl From<Arc<dyn Error + Send + Sync>> for StreamDisconnectReason {
    fn from(error: Arc<dyn Error + Send + Sync>) -> Self {
        StreamDisconnectReason::InternalError(error)
    }
}

impl From<StreamDisconnectReason> for DisconnectReason {
    fn from(val: StreamDisconnectReason) -> Self {
        match val {
            StreamDisconnectReason::UserClosed => {
                DisconnectReason::ByUser("Send stream stopped by self.".to_owned())
            }
            StreamDisconnectReason::PeerClosed => {
                DisconnectReason::ByPeer("Stream closed by peer.".to_owned())
            }
            StreamDisconnectReason::Reset(error) => DisconnectReason::ByError(anyhow!(
                "Stream closed by reset with code: {error}"
            )),
            StreamDisconnectReason::InvalidStream => {
                DisconnectReason::ByError(anyhow!("Stream is no longer valid"))
            }
            StreamDisconnectReason::ConnectionError(error) => DisconnectReason::ByError(
                anyhow!("Stream has been closed due to a connection error: {error}"),
            ),
            StreamDisconnectReason::ResourceError => DisconnectReason::ByError(anyhow!(
                "Stream was closed due to a resource error"
            )),
            StreamDisconnectReason::MspcChannelClosed { channel_name } => {
                DisconnectReason::ByError(anyhow!(
                    "Stream was closed due to an IPC channel \"{channel_name}\" being closed"
                ))
            }
            StreamDisconnectReason::InternalError(error) => DisconnectReason::ByError(
                anyhow!("Stream was closed due to an internal error: {error}"),
            ),
            StreamDisconnectReason::NoReason => DisconnectReason::ByError(anyhow!(
                "Stream was closed without reason, this is a bug :("
            )),
        }
    }
}
