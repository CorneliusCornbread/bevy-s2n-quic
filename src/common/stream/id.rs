use std::fmt::Display;

use crate::common::{QuicParentId, connection::id::ConnectionId};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct StreamId {
    connection_id: ConnectionId,
    id: u64,
}

impl Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StreamId(Id: {0}, Connection: {1}, Parent: {2})",
            self.id,
            self.connection_id.id(),
            self.connection_id.parent_id()
        )
    }
}

impl StreamId {
    /// Creates a new StreamId with the relevant parent connection
    /// and the ID for this connection.
    pub fn new(connection_id: ConnectionId, id: u64) -> Self {
        Self { connection_id, id }
    }

    /// Gets the ID for the [QuicServer][crate::server::QuicServer] or
    /// [QuicClient][crate::client::QuicClient] that is the parent
    /// for this stream.
    pub fn parent_id(&self) -> QuicParentId {
        self.connection_id.parent_id()
    }

    /// Gets the ID for the relevant [QuicConnection][crate::common::connection::QuicConnection]
    /// that owns this stream.
    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    /// Get the ID for this specific stream, the ID is relative to its parent
    /// [QuicConnection][crate::common::connection::QuicConnection].
    pub fn id(&self) -> u64 {
        self.id
    }
}
