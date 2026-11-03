use std::net::SocketAddr;

/// Where a [QuicClient][crate::client::QuicClient] should connect to.
///
/// ```
/// use bevy_s2n_quic::client::Connect;
///
/// let connect = Connect::new("127.0.0.1:7777".parse().unwrap()).with_server_name("localhost");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    addr: SocketAddr,
    server_name: Option<String>,
}

impl Connect {
    /// Connects to the server at `addr`.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            server_name: None,
        }
    }

    /// Sets the name the server's certificate must be valid for. It's also
    /// sent to the server as the TLS server name (SNI). Certificates are
    /// usually issued for a host name, so most connections need this.
    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self
    }

    /// The server's address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The name the server's certificate must be valid for, if set.
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }
}
