use std::time::Duration;

/// Statistics for a [QuicConnection][super::QuicConnection].
///
/// Mirrored from the network thread at most every 50 ms while the connection
/// is active, so they may be slightly out of date.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionStats {
    /// The current estimate of the round-trip time.
    pub rtt: Duration,
    /// The congestion window: how many bytes may be in flight at once.
    pub congestion_window: u64,
    /// Packets sent, including retransmissions.
    pub sent_packets: u64,
    /// Packets declared lost.
    pub lost_packets: u64,
    /// The largest UDP payload the path currently supports.
    pub current_mtu: u16,
    /// Bytes sent in UDP datagrams, including QUIC overhead.
    pub bytes_sent: u64,
    /// Bytes received in UDP datagrams, including QUIC overhead.
    pub bytes_received: u64,
}
