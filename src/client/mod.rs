use bevy::ecs::component::Component;
use s2n_quic::{
    Client, Connection,
    client::{Connect, ConnectionAttempt},
};
use s2n_quic_tls::{certificate::IntoCertificate, error::Error as TlsError};
use tokio::runtime::Handle;

use crate::{
    client::marker::QuicClientMarker,
    common::{
        QuicParentId, QuicParentType, attempt::TaskError,
        connection::QuicConnectionAttempt, runtime::TokioRuntime,
    },
};

pub mod acceptor;
pub mod marker;

/// The component which represents a client.
#[derive(Component)]
#[require(QuicClientMarker)]
pub struct QuicClient {
    runtime: Handle,
    client: Client,
    id: QuicParentId,
}

impl QuicClient {
    /// Construct a client with default TLS settings. This will not allow you to connect to
    /// servers with self-signed certs.
    pub fn new(tokio_runtime: &TokioRuntime) -> Self {
        let runtime = tokio_runtime.handle().clone();
        let client = runtime.block_on(build());

        Self {
            runtime,
            client,
            id: QuicParentId::generate_unique(QuicParentType::Client),
        }
    }

    /// Construct a client with custom TLS settings. This is commonly used for development purposes
    /// to allow custom certs.
    pub fn new_with_tls<C: IntoCertificate>(
        tokio_runtime: &TokioRuntime,
        certificate: C,
    ) -> Result<Self, TlsError> {
        let runtime = tokio_runtime.handle().clone();
        let client = runtime.block_on(build_tls(certificate))?;

        let ret = Self {
            runtime,
            client,
            id: QuicParentId::generate_unique(QuicParentType::Client),
        };

        Ok(ret)
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
        let client = &self.client;
        let attempt = client.connect(connect);

        let conn_task = self.runtime.spawn(create_connection(attempt));

        (
            QuicConnectionAttempt::new(self.runtime.clone(), conn_task, self.id),
            QuicClientMarker,
        )
    }
}

async fn create_connection(attempt: ConnectionAttempt) -> Result<Connection, TaskError> {
    attempt.await.map_err(TaskError::ConnectionFailed)
}

async fn build() -> Client {
    Client::builder()
        .with_io("0.0.0.0:0")
        .expect("Unable to build client... are we... out of sockets??")
        .start()
        .expect("Unable to start client")
}

async fn build_tls<C: IntoCertificate>(certificate: C) -> Result<Client, TlsError> {
    let tls = s2n_quic_tls::Client::builder()
        .with_certificate(certificate)?
        .build()?;

    let client = Client::builder()
        .with_io("0.0.0.0:0")
        .expect("Unable to build client... are we... out of sockets??")
        .with_tls(tls)
        .expect("Invalid TLS")
        .start()
        .expect("Unable to start client");

    Ok(client)
}
