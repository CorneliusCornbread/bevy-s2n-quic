//! TLS and socket configuration for [QuicServer][crate::server::QuicServer]
//! and [QuicClient][crate::client::QuicClient].

use std::{
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

use rustls_pki_types::pem::{self, PemObject};
use thiserror::Error;

pub use rustls_pki_types::{self as pki_types, CertificateDer, PrivateKeyDer};

/// Configuration for a [QuicServer][crate::server::QuicServer]: the
/// certificate chain it presents and the matching private key.
///
/// ```no_run
/// use bevy_s2n_quic::config::QuicServerConfig;
///
/// let config = QuicServerConfig::from_pem_files("cert.pem", "key.pem")?;
/// # Ok::<_, bevy_s2n_quic::config::ConfigError>(())
/// ```
#[derive(Debug)]
pub struct QuicServerConfig {
    cert_chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl QuicServerConfig {
    /// Creates a configuration from a certificate chain, leaf first, and its
    /// private key.
    pub fn new(
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self, ConfigError> {
        if cert_chain.is_empty() {
            return Err(ConfigError::EmptyCertificateChain);
        }

        Ok(Self { cert_chain, key })
    }

    /// Loads the certificate chain and private key from PEM files. The
    /// certificate file may hold several certificates, leaf first. The key
    /// may be PKCS#8, PKCS#1 (RSA) or SEC1 (EC).
    pub fn from_pem_files(
        cert_chain: impl AsRef<Path>,
        key: impl AsRef<Path>,
    ) -> Result<Self, ConfigError> {
        let cert_chain = read_certificates(cert_chain.as_ref())?;
        let key = key.as_ref();
        let key = PrivateKeyDer::from_pem_file(key).map_err(|e| pem_error(key, e))?;

        Ok(Self { cert_chain, key })
    }

    pub(crate) fn cert_chain(&self) -> &[CertificateDer<'static>] {
        &self.cert_chain
    }

    pub(crate) fn key(&self) -> &PrivateKeyDer<'static> {
        &self.key
    }
}

impl Clone for QuicServerConfig {
    fn clone(&self) -> Self {
        Self {
            cert_chain: self.cert_chain.clone(),
            key: self.key.clone_key(),
        }
    }
}

/// Configuration for a [QuicClient][crate::client::QuicClient].
///
/// By default the client trusts the platform's root certificates and binds an
/// ephemeral port on `0.0.0.0`. To connect to a server with a self-signed
/// certificate, add it as a root:
///
/// ```no_run
/// use bevy_s2n_quic::config::QuicClientConfig;
///
/// let config = QuicClientConfig::new().with_root_pem_file("cert.pem")?;
/// # Ok::<_, bevy_s2n_quic::config::ConfigError>(())
/// ```
#[derive(Debug, Clone)]
pub struct QuicClientConfig {
    roots: Vec<CertificateDer<'static>>,
    bind_addr: SocketAddr,
}

impl Default for QuicClientConfig {
    fn default() -> Self {
        Self {
            roots: Vec::new(),
            bind_addr: (Ipv4Addr::UNSPECIFIED, 0).into(),
        }
    }
}

impl QuicClientConfig {
    /// Creates the default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Trusts `root` in addition to the platform's root certificates.
    pub fn with_root(mut self, root: CertificateDer<'static>) -> Self {
        self.roots.push(root);
        self
    }

    /// Trusts every certificate in the PEM file at `path`, in addition to the
    /// platform's root certificates.
    pub fn with_root_pem_file(
        mut self,
        path: impl AsRef<Path>,
    ) -> Result<Self, ConfigError> {
        self.roots.extend(read_certificates(path.as_ref())?);
        Ok(self)
    }

    /// Sets the local address the client's socket binds to.
    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    pub(crate) fn roots(&self) -> &[CertificateDer<'static>] {
        &self.roots
    }

    pub(crate) fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}

/// Errors from loading a configuration or starting an endpoint with it.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A PEM file couldn't be read or parsed.
    #[error("can't load {}: {source}", path.display())]
    Pem {
        path: PathBuf,
        #[source]
        source: pem::Error,
    },
    /// A PEM file that should hold certificates has none.
    #[error("no certificates found in {}", .0.display())]
    NoCertificates(PathBuf),
    /// [QuicServerConfig::new] was given no certificates.
    #[error("the certificate chain is empty")]
    EmptyCertificateChain,
    /// The TLS provider rejected the certificates or key.
    #[error("invalid TLS configuration: {0}")]
    Tls(String),
    /// The endpoint couldn't be started, for example because the address is
    /// already in use.
    #[error("can't start the QUIC endpoint on {addr}: {reason}")]
    Endpoint { addr: SocketAddr, reason: String },
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, ConfigError> {
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|iter| iter.collect::<Result<Vec<_>, _>>())
        .map_err(|e| pem_error(path, e))?;

    if certs.is_empty() {
        return Err(ConfigError::NoCertificates(path.to_owned()));
    }

    Ok(certs)
}

fn pem_error(path: &Path, source: pem::Error) -> ConfigError {
    ConfigError::Pem {
        path: path.to_owned(),
        source,
    }
}
