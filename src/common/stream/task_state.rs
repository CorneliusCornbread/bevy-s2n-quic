use crate::common::{
    connection::disconnect::ConnectionDisconnectReason, task_state::JoinHandleState,
};

pub(in crate::common::stream) type StreamTaskState =
    JoinHandleState<ConnectionDisconnectReason>;
