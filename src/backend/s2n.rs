//! Conversions between the crate's types and s2n-quic's.

use bytes::Bytes;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use s2n_quic::{application, connection::Error as S2nConnectionError};

use crate::{
    client::Connect,
    common::error::{AppErrorCode, ConnectionError},
    config::{ConfigError, QuicClientConfig, QuicServerConfig},
};

pub(crate) fn to_s2n_code(code: AppErrorCode) -> application::Error {
    application::Error::new(code.value())
        .expect("AppErrorCode is always a valid QUIC varint")
}

pub(crate) fn from_s2n_code(code: application::Error) -> AppErrorCode {
    AppErrorCode::new(u64::from(code)).expect("s2n error codes are QUIC varints")
}

pub(crate) fn to_s2n_connect(connect: &Connect) -> s2n_quic::client::Connect {
    let s2n = s2n_quic::client::Connect::new(connect.addr());
    match connect.server_name() {
        Some(name) => s2n.with_server_name(name),
        None => s2n,
    }
}

pub(crate) fn from_s2n_connection_error(err: S2nConnectionError) -> ConnectionError {
    match err {
        S2nConnectionError::Closed { initiator, .. } if initiator.is_remote() => {
            ConnectionError::ConnectionClosed {
                code: 0,
                reason: Bytes::new(),
            }
        }
        S2nConnectionError::Transport {
            code,
            reason,
            initiator,
            ..
        } if initiator.is_remote() => ConnectionError::ConnectionClosed {
            code: code.as_u64(),
            reason: Bytes::from_static(reason.as_bytes()),
        },
        S2nConnectionError::Application {
            error, initiator, ..
        } if initiator.is_remote() => ConnectionError::ApplicationClosed {
            code: from_s2n_code(error),
            reason: Bytes::new(),
        },
        S2nConnectionError::Closed { .. }
        | S2nConnectionError::Application { .. }
        | S2nConnectionError::EndpointClosing { .. } => ConnectionError::LocallyClosed,
        S2nConnectionError::StatelessReset { .. } => ConnectionError::Reset,
        S2nConnectionError::IdleTimerExpired { .. }
        | S2nConnectionError::MaxHandshakeDurationExceeded { .. } => {
            ConnectionError::TimedOut
        }
        other => ConnectionError::Transport(other.to_string()),
    }
}

/// Returns `true` if the error means the connection can no longer be used.
pub(crate) fn is_closed(err: &S2nConnectionError) -> bool {
    matches!(
        err,
        S2nConnectionError::Closed { .. }
            | S2nConnectionError::Transport { .. }
            | S2nConnectionError::Application { .. }
            | S2nConnectionError::EndpointClosing { .. }
            | S2nConnectionError::IdleTimerExpired { .. }
            | S2nConnectionError::NoValidPath { .. }
    )
}

pub(crate) fn server_tls(
    config: &QuicServerConfig,
) -> Result<s2n_quic_tls::Server, ConfigError> {
    let chain: String = config.cert_chain().iter().map(certificate_pem).collect();
    let key = private_key_pem(config.key());

    s2n_quic_tls::Server::builder()
        .with_certificate(chain, key)
        .and_then(|builder| builder.build())
        .map_err(|e| ConfigError::Tls(e.to_string()))
}

pub(crate) fn client_tls(
    config: &QuicClientConfig,
) -> Result<s2n_quic_tls::Client, ConfigError> {
    let mut builder = s2n_quic_tls::Client::builder();
    for root in config.roots() {
        builder = builder
            .with_certificate(certificate_pem(root))
            .map_err(|e| ConfigError::Tls(e.to_string()))?;
    }

    builder.build().map_err(|e| ConfigError::Tls(e.to_string()))
}

// s2n-tls only loads PEM, so DER is converted back.

fn certificate_pem(cert: &CertificateDer<'_>) -> String {
    pem_encode("CERTIFICATE", cert)
}

fn private_key_pem(key: &PrivateKeyDer<'_>) -> String {
    let label = match key {
        PrivateKeyDer::Pkcs1(_) => "RSA PRIVATE KEY",
        PrivateKeyDer::Sec1(_) => "EC PRIVATE KEY",
        _ => "PRIVATE KEY",
    };
    pem_encode(label, key.secret_der())
}

fn pem_encode(label: &str, der: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    // PEM wraps base64 at 64 characters, which is 48 input bytes.
    const LINE_BYTES: usize = 48;

    let mut out = format!("-----BEGIN {label}-----\n");
    for line in der.chunks(LINE_BYTES) {
        for group in line.chunks(3) {
            let b = [
                group[0],
                group.get(1).copied().unwrap_or(0),
                group.get(2).copied().unwrap_or(0),
            ];
            let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
            for i in 0..4 {
                if i <= group.len() {
                    out.push(ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls_pki_types::pem::PemObject;

    #[test]
    fn pem_round_trips() {
        for len in [0, 1, 2, 3, 47, 48, 49, 100] {
            let der: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
            let cert = CertificateDer::from(der.clone());
            let pem = certificate_pem(&cert);
            let decoded = CertificateDer::from_pem_slice(pem.as_bytes()).unwrap();
            assert_eq!(decoded.as_ref(), der.as_slice(), "length {len}");
        }
    }

    #[test]
    fn example_key_round_trips() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/certs/key.pem");
        let key = PrivateKeyDer::from_pem_file(path).unwrap();
        let decoded =
            PrivateKeyDer::from_pem_slice(private_key_pem(&key).as_bytes()).unwrap();
        assert_eq!(decoded.secret_der(), key.secret_der());
    }

    #[test]
    fn app_codes_round_trip() {
        for value in [0, 1, 12345, AppErrorCode::MAX] {
            let code = AppErrorCode::new(value).unwrap();
            assert_eq!(from_s2n_code(to_s2n_code(code)), code);
        }
        assert!(AppErrorCode::new(AppErrorCode::MAX + 1).is_none());
    }
}
