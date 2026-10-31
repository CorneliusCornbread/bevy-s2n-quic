//! Builds quinn-proto configurations from the crate's own, and converts
//! between the crate's types and quinn-proto's.

use std::{sync::Arc, time::Duration};

use quinn_proto::{
    ClientConfig, EndpointConfig, IdleTimeout, ServerConfig, TransportConfig, VarInt,
    crypto::rustls::QuicClientConfig as RustlsClientConfig,
};
use rustls::crypto::CryptoProvider;
use rustls_platform_verifier::Verifier;

use crate::{
    common::{
        connection::stats::ConnectionStats,
        error::{AppErrorCode, ConnectionError},
    },
    config::{ConfigError, QuicClientConfig, QuicServerConfig},
};

/// The largest number of connections a server may be handshaking at once,
/// whatever its connection limit.
const MAX_INCOMING: usize = 1 << 16;

/// Per-endpoint settings that aren't part of the TLS configuration.
#[derive(Debug, Clone)]
pub(crate) struct EndpointOptions {
    /// Closes a connection after this long without hearing from the peer.
    /// `None` disables the timeout.
    pub idle_timeout: Option<Duration>,
    /// Sends a keep-alive this often when there's nothing else to send, so an
    /// idle connection isn't closed. Should be shorter than `idle_timeout`.
    pub keep_alive_interval: Option<Duration>,
    /// For servers, the most connections open at once. Connections past it
    /// are refused.
    pub max_connections: usize,
    /// Bytes each send stream may have queued that quinn hasn't accepted yet.
    pub send_budget: usize,
    /// Bytes each receive stream may have delivered that the owner hasn't
    /// consumed yet.
    pub recv_budget: usize,
    /// Bytes of datagrams each connection may have queued to send. Past it,
    /// the oldest queued datagrams are dropped.
    pub datagram_send_buffer: usize,
    /// Bytes of received datagrams each connection buffers before the
    /// oldest are dropped. Also limits the size of one datagram from the
    /// peer.
    pub datagram_receive_buffer: usize,
    /// For servers, hands each accepted connection to the least busy driver
    /// in the endpoint's group, instead of keeping it on the endpoint's.
    pub spread_connections: bool,
    /// Seeds quinn's random number generator, for reproducible tests.
    pub rng_seed: Option<[u8; 32]>,
}

impl Default for EndpointOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(30)),
            keep_alive_interval: Some(Duration::from_secs(10)),
            max_connections: usize::MAX,
            send_budget: 1 << 20,
            recv_budget: 1 << 20,
            datagram_send_buffer: 256 << 10,
            datagram_receive_buffer: 1 << 20,
            spread_connections: true,
            rng_seed: None,
        }
    }
}

/// Everything a driver needs to run an endpoint, built ahead of time so the
/// driver thread never parses certificates. Cheap to clone.
#[derive(Clone)]
pub(crate) struct EndpointSetup {
    pub(super) role: Role,
    pub(super) endpoint_config: Arc<EndpointConfig>,
    pub(super) max_connections: usize,
    pub(super) send_budget: usize,
    pub(super) recv_budget: usize,
    pub(super) datagram_send_buffer: usize,
    pub(super) spread_connections: bool,
}

#[derive(Clone)]
pub(super) enum Role {
    Server(Arc<ServerConfig>),
    Client(ClientConfig),
}

impl EndpointSetup {
    pub fn server(
        config: &QuicServerConfig,
        options: &EndpointOptions,
    ) -> Result<Self, ConfigError> {
        let mut server = ServerConfig::with_single_cert(
            config.cert_chain().to_vec(),
            config.key().clone_key(),
        )
        .map_err(|e| ConfigError::Tls(e.to_string()))?;
        server
            .transport_config(transport_config(options))
            .max_incoming(options.max_connections.clamp(1, MAX_INCOMING));

        Ok(Self::new(Role::Server(Arc::new(server)), options))
    }

    pub fn client(
        config: &QuicClientConfig,
        options: &EndpointOptions,
    ) -> Result<Self, ConfigError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier =
            Verifier::new_with_extra_roots(config.roots().to_vec(), provider.clone())
                .map_err(|e| ConfigError::Tls(e.to_string()))?;
        let mut tls = rustls_client_builder(provider)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        tls.enable_early_data = true;

        let crypto = RustlsClientConfig::try_from(tls)
            .map_err(|e| ConfigError::Tls(e.to_string()))?;
        let mut client = ClientConfig::new(Arc::new(crypto));
        client.transport_config(transport_config(options));

        Ok(Self::new(Role::Client(client), options))
    }

    fn new(role: Role, options: &EndpointOptions) -> Self {
        let mut endpoint_config = EndpointConfig::default();
        endpoint_config.rng_seed(options.rng_seed);

        Self {
            role,
            endpoint_config: Arc::new(endpoint_config),
            max_connections: options.max_connections,
            send_budget: options.send_budget,
            recv_budget: options.recv_budget,
            datagram_send_buffer: options.datagram_send_buffer,
            spread_connections: options.spread_connections,
        }
    }
}

fn rustls_client_builder(
    provider: Arc<CryptoProvider>,
) -> Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>, ConfigError>
{
    rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| ConfigError::Tls(e.to_string()))
}

fn transport_config(options: &EndpointOptions) -> Arc<TransportConfig> {
    // Timeouts too long for QUIC to encode are clamped to the longest it can.
    let idle_timeout = options
        .idle_timeout
        .map(|timeout| IdleTimeout::try_from(timeout).unwrap_or(VarInt::MAX.into()));

    let mut transport = TransportConfig::default();
    transport
        .max_idle_timeout(idle_timeout)
        .keep_alive_interval(options.keep_alive_interval)
        .datagram_send_buffer_size(options.datagram_send_buffer)
        .datagram_receive_buffer_size(Some(options.datagram_receive_buffer));

    Arc::new(transport)
}

pub(super) fn to_varint(code: AppErrorCode) -> VarInt {
    VarInt::from_u64(code.value()).expect("AppErrorCode is always a valid QUIC varint")
}

pub(super) fn from_varint(code: VarInt) -> AppErrorCode {
    AppErrorCode::new(code.into_inner()).expect("a VarInt is always a valid AppErrorCode")
}

pub(super) fn connection_error(err: quinn_proto::ConnectionError) -> ConnectionError {
    use quinn_proto::ConnectionError as Quinn;

    match err {
        Quinn::VersionMismatch => ConnectionError::VersionMismatch,
        Quinn::TransportError(e) => ConnectionError::Transport(e.to_string()),
        Quinn::ConnectionClosed(close) => ConnectionError::ConnectionClosed {
            code: close.error_code.into(),
            reason: close.reason,
        },
        Quinn::ApplicationClosed(close) => ConnectionError::ApplicationClosed {
            code: from_varint(close.error_code),
            reason: close.reason,
        },
        Quinn::Reset => ConnectionError::Reset,
        Quinn::TimedOut => ConnectionError::TimedOut,
        Quinn::LocallyClosed => ConnectionError::LocallyClosed,
        other @ Quinn::CidsExhausted => ConnectionError::Transport(other.to_string()),
    }
}

pub(super) fn connection_stats(
    conn: &quinn_proto::Connection,
    datagrams_dropped: u64,
) -> ConnectionStats {
    let stats = conn.stats();
    ConnectionStats {
        rtt: stats.path.rtt,
        congestion_window: stats.path.cwnd,
        sent_packets: stats.path.sent_packets,
        lost_packets: stats.path.lost_packets,
        current_mtu: stats.path.current_mtu,
        bytes_sent: stats.udp_tx.bytes,
        bytes_received: stats.udp_rx.bytes,
        datagrams_dropped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_codes_round_trip() {
        for value in [0, 1, 12345, AppErrorCode::MAX] {
            let code = AppErrorCode::new(value).unwrap();
            assert_eq!(from_varint(to_varint(code)), code);
        }
    }
}
