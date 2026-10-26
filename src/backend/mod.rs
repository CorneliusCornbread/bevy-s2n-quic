//! Escape hatches into the QUIC backend. Nothing here is covered by semver.

use crate::common::runtime::QuicRuntime;

pub(crate) mod s2n;

/// The Tokio runtime behind [QuicRuntime], for reading runtime metrics.
#[doc(hidden)]
pub fn tokio_handle(runtime: &QuicRuntime) -> &tokio::runtime::Handle {
    runtime.handle()
}
