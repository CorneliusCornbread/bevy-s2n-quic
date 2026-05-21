use bevy::log::error;
use std::fmt;
use tokio::sync::mpsc::error::TrySendError;

use crate::common::id::IdGenerator;

pub mod attempt;
pub mod connection;
pub(crate) mod id;
pub mod orchestrator;
pub mod plugin;
pub mod runtime;
pub mod status_code;
pub mod stream;
pub(crate) mod task_state;

/// Enum determining the type (server or client) of the parent
/// which is responsible for any given QUIC network resource.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum QuicParentType {
    /// This resource was created by a [QuicServer][crate::server::QuicServer]
    Server,
    /// This resource was created by a [QuicClient][crate::client::QuicClient]
    Client,
}

impl fmt::Display for QuicParentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuicParentType::Client => write!(f, "Client"),
            QuicParentType::Server => write!(f, "Server"),
        }
    }
}

/// An ID which uniquely identifies the [QuicClient][crate::client::QuicClient] or
/// [QuicServer][crate::server::QuicServer] that is responsible for the given
/// QUIC network resource.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct QuicParentId {
    parent_type: QuicParentType,
    parent_id: u64,
}

impl QuicParentId {
    pub fn new(parent_id: u64, parent_type: QuicParentType) -> Self {
        Self {
            parent_type,
            parent_id,
        }
    }

    pub fn generate_unique(parent_type: QuicParentType) -> Self {
        Self {
            parent_type,
            parent_id: IdGenerator::generate_unique(),
        }
    }

    /// Gets the unique ID for the parent structure. This is unique across all
    /// [QuicClient][crate::client::QuicClient] and [QuicServer][crate::server::QuicServer]
    /// instances.
    pub fn parent_id(&self) -> u64 {
        self.parent_id
    }

    /// Gets the type of connection created this network resource.
    pub fn connection_type(&self) -> QuicParentType {
        self.parent_type
    }
}

impl fmt::Display for QuicParentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} Id: {}", self.parent_type, self.parent_id)
    }
}

pub(crate) trait HandleChannelError {
    fn handle_err(&self);
}

impl<T> HandleChannelError for Result<(), TrySendError<T>> {
    fn handle_err(&self) {
        if let Err(send_err) = self {
            error!(
                "Error buffer for async task is full or closed, the following error will be dropped: {send_err}"
            );
        }
    }
}
