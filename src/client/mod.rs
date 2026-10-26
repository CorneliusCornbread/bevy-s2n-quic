use std::net::SocketAddr;

use bevy::ecs::component::Component;
use s2n_quic::Client;
use tokio::runtime::Handle;

use crate::{
    backend::s2n::{client_tls, to_s2n_connect},
    client::marker::QuicClientMarker,
    common::{
        QuicParentId, QuicParentType,
        connection::{QuicConnection, QuicConnectionAttempt},
        runtime::QuicRuntime,
        spawner::TaskSpawner,
    },
    config::{ConfigError, QuicClientConfig},
};

pub mod acceptor;
mod connect;
pub mod marker;

pub use connect::Connect;

/// The component which represents a client.
#[derive(Component)]
#[require(QuicClientMarker)]
pub struct QuicClient {
    runtime: Handle,
    spawner: TaskSpawner,
    client: Client,
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
        let handle = runtime.handle().clone();
        let tls = client_tls(config)?;
        let client = handle.block_on(build(config.bind_addr(), tls))?;

        Ok(Self {
            runtime: handle,
            spawner: runtime.spawner().clone(),
            client,
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
        let attempt = self.client.connect(to_s2n_connect(&connect));
        let (runtime, spawner, id) =
            (self.runtime.clone(), self.spawner.clone(), self.id);

        let conn_task = self.runtime.spawn(async move {
            let connection = attempt.await?;
            Ok(QuicConnection::new(runtime, spawner, connection, id))
        });

        (
            QuicConnectionAttempt::new(self.runtime.clone(), conn_task, self.id),
            QuicClientMarker,
        )
    }
}

async fn build(
    addr: SocketAddr,
    tls: s2n_quic_tls::Client,
) -> Result<Client, ConfigError> {
    let endpoint_error = |reason: String| ConfigError::Endpoint { addr, reason };

    Client::builder()
        .with_tls(tls)
        .map_err(|e| ConfigError::Tls(e.to_string()))?
        .with_io(addr)
        .map_err(|e| endpoint_error(e.to_string()))?
        .start()
        .map_err(|e| endpoint_error(e.to_string()))
}
