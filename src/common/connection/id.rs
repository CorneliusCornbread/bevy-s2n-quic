use std::fmt;

use crate::common::QuicParentId;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct ConnectionId {
    parent_id: QuicParentId,
    id: u64,
}

impl ConnectionId {
    pub(crate) fn new(id: u64, parent_id: QuicParentId) -> Self {
        Self { parent_id, id }
    }

    pub fn parent_id(&self) -> QuicParentId {
        self.parent_id
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamId(Id: {0}, Parent: {1}, Type: {2:?})",
            self.id,
            self.parent_id.parent_id(),
            self.parent_id.connection_type()
        )
    }
}
