use std::{io, net::SocketAddr, sync::Arc};

use bevy::ecs::component::Component;
use s2n_quic::Server;
use tokio::runtime::Handle;

use crate::{
    backend::s2n::server_tls,
    common::{
        QuicParentId, QuicParentType, connection::QuicConnection, runtime::QuicRuntime,
        spawner::TaskSpawner,
    },
    config::{ConfigError, QuicServerConfig},
    server::marker::QuicServerMarker,
};

pub mod acceptor;
pub mod marker;

/// The component which manages an instance of a QuicServer.
///
/// It is recommended you parent any [QuicConnection] to their related QuicServer entity.
#[derive(Component)]
#[require(QuicServerMarker)]
pub struct QuicServer {
    runtime: Handle,
    server: Server,
    id: QuicParentId,
    spawner: TaskSpawner,
}

impl QuicServer {
    /// Creates a new QuicServer and binds it to the given address, presenting
    /// the certificate from `config`.
    ///
    /// Bind to port `0` to let the OS pick a free port, then read it with
    /// [Self::local_addr].
    pub fn bind(
        runtime: &QuicRuntime,
        bind_addr: SocketAddr,
        config: &QuicServerConfig,
    ) -> Result<Self, ConfigError> {
        let handle = runtime.handle().clone();
        let tls = server_tls(config)?;
        let server = handle.block_on(build_server(bind_addr, tls))?;
        let spawner = runtime.spawner().clone();

        Ok(Self {
            runtime: handle,
            server,
            id: QuicParentId::generate_unique(QuicParentType::Server),
            spawner,
        })
    }

    /// Polls to receive any new pending connections
    pub fn accept_connection(&mut self) -> ConnectionPoll {
        let waker = Arc::new(futures::task::noop_waker_ref());
        let mut cx = std::task::Context::from_waker(&waker);

        let poll = self.server.poll_accept(&mut cx);

        match poll {
            std::task::Poll::Ready(conn_opt) => {
                if let Some(conn) = conn_opt {
                    ConnectionPoll::NewConnection(QuicConnection::new(
                        self.runtime.clone(),
                        self.spawner.clone(),
                        conn,
                        self.id,
                    ))
                } else {
                    bevy::log::info!(
                        "Server connection poll returned none, is our server not running?"
                    );
                    ConnectionPoll::ServerClosed
                }
            }
            std::task::Poll::Pending => ConnectionPoll::None,
        }
    }

    /// The local address the server is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.server.local_addr()
    }

    pub fn id(&self) -> QuicParentId {
        self.id
    }
}

#[derive(Debug)]
pub enum ConnectionPoll {
    None,
    ServerClosed,
    NewConnection(QuicConnection),
}

async fn build_server(
    addr: SocketAddr,
    tls: s2n_quic_tls::Server,
) -> Result<Server, ConfigError> {
    let endpoint_error = |reason: String| ConfigError::Endpoint { addr, reason };

    Server::builder()
        .with_tls(tls)
        .map_err(|e| ConfigError::Tls(e.to_string()))?
        .with_io(addr)
        .map_err(|e| endpoint_error(e.to_string()))?
        .start()
        .map_err(|e| endpoint_error(e.to_string()))
}

pub enum QuitReason {
    ServerClosed,
    BrokenSender,
}
