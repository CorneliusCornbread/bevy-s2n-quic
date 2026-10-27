use bevy::ecs::component::Component;

use crate::{
    client::marker::QuicClientMarker,
    common::{
        QuicParentId, QuicParentType,
        connection::{QuicConnection, QuicConnectionAttempt, id::ConnectionId},
        router::Notice,
        runtime::{Link, QuicRuntime},
    },
    config::{ConfigError, QuicClientConfig},
    driver::{Command, EndpointId, EndpointSetup},
};

pub mod acceptor;
mod connect;
pub mod marker;

pub use connect::Connect;

/// The component which represents a client.
///
/// Dropping the client stops it making connections. Connections it already
/// made stay open.
#[derive(Component)]
#[require(QuicClientMarker)]
pub struct QuicClient {
    link: Link,
    driver: usize,
    endpoint: EndpointId,
    id: QuicParentId,
}

impl QuicClient {
    /// Creates a client with the given configuration.
    ///
    /// The default [QuicClientConfig] only trusts the platform's root
    /// certificates, so it can't connect to servers with self-signed
    /// certificates. Add those with [QuicClientConfig::with_root_pem_file].
    pub fn new(
        runtime: &QuicRuntime,
        config: &QuicClientConfig,
    ) -> Result<Self, ConfigError> {
        let setup = EndpointSetup::client(config, runtime.endpoint_options())?;
        let driver = runtime.pick_driver();
        let link = runtime.link(driver);
        let endpoint = link.driver.ids().endpoint();
        link.driver
            .open_endpoint(endpoint, config.bind_addr(), setup)?;

        Ok(Self {
            link,
            driver,
            endpoint,
            id: QuicParentId::generate_unique(QuicParentType::Client),
        })
    }

    /// The unique ID for this QUIC session.
    pub fn id(&self) -> QuicParentId {
        self.id
    }

    /// Opens a new connection to the given `connect` target.
    /// Returns an attempt and an ID assigned to the connection.
    pub fn open_connection(
        &mut self,
        connect: Connect,
    ) -> (QuicConnectionAttempt, QuicClientMarker) {
        let conn = self.link.driver.ids().conn();
        let connection_id = ConnectionId::new(conn.0, self.id);
        let remote_addr = connect.addr();

        // Registered before connecting, so the router knows the connection
        // before any of its events arrive.
        self.link.notify(Notice::Conn {
            conn,
            id: connection_id,
            driver: self.driver,
        });
        self.link.send(Command::Connect {
            endpoint: self.endpoint,
            conn,
            connect,
        });

        let connection =
            QuicConnection::new(self.link.clone(), conn, connection_id, remote_addr);
        (
            QuicConnectionAttempt::new(connection, self.id),
            QuicClientMarker,
        )
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        self.link.send(Command::ReleaseEndpoint {
            endpoint: self.endpoint,
        });
    }
}
