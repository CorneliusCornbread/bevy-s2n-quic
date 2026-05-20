use crate::common::{
    stream::disconnect::StreamDisconnectReason, task_state::JoinHandleState,
};

pub(in crate::common::stream) type StreamTaskState =
    JoinHandleState<StreamDisconnectReason>;
