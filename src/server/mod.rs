use std::{error::Error, net::SocketAddr, sync::Arc};

use bevy::ecs::component::Component;
use s2n_quic::Server;
use s2n_quic_tls::certificate::{IntoCertificate, IntoPrivateKey};
use tokio::{runtime::Handle, task::JoinError};

use crate::{
    common::{
        QuicParentId, QuicParentType, connection::QuicConnection,
        orchestrator::handle::OrchestratorHandle, runtime::TokioRuntime,
    },
    server::marker::QuicServerMarker,
};

pub mod acceptor;
pub mod marker;

/// The component which manages an instance of a QuicServer.
///
/// It is recommended you parent any [QuicServerConnection] to their related QuicServer entity.
#[derive(Component)]
#[require(QuicServerMarker)]
pub struct QuicServer {
    runtime: Handle,
    server: Server,
    id: QuicParentId,
    orchestrator: OrchestratorHandle,
}

impl QuicServer {
    /// Creates a new QuicServer and binds it to the given address with the given certificates.
    ///
    /// QUIC requires some form of TLS certificate, this function accepts the same kinds of certs the regular s2n-quic
    /// `bind()` function does.
    pub fn bind<C: IntoCertificate, PK: IntoPrivateKey>(
        runtime: &TokioRuntime,
        bind_ip: SocketAddr,
        certificate: C,
        private_key: PK,
    ) -> Result<Self, Box<dyn Error>> {
        let handle = runtime.handle().clone();
        let server = handle.block_on(build_server(bind_ip, certificate, private_key))?;
        let orchestrator = runtime.orchestrator().clone();

        Ok(Self {
            runtime: handle,
            server,
            id: QuicParentId::generate_unique(QuicParentType::Server),
            orchestrator,
        })
    }

    /// Polls to receive any new pending connections
    pub fn accept_connection(&mut self) -> Result<ConnectionPoll, JoinError> {
        let waker = Arc::new(futures::task::noop_waker_ref());
        let mut cx = std::task::Context::from_waker(&waker);

        let poll = self.server.poll_accept(&mut cx);

        match poll {
            std::task::Poll::Ready(conn_opt) => {
                if let Some(conn) = conn_opt {
                    let ret = ConnectionPoll::NewConnection(QuicConnection::new(
                        self.runtime.clone(),
                        self.orchestrator.clone(),
                        conn,
                        self.id,
                    ));

                    Ok(ret)
                } else {
                    bevy::log::info!(
                        "Server connection poll returned none, is our server not running?"
                    );
                    Ok(ConnectionPoll::ServerClosed)
                }
            }
            std::task::Poll::Pending => Ok(ConnectionPoll::None),
        }
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

async fn build_server<C: IntoCertificate, PK: IntoPrivateKey>(
    ip: SocketAddr,
    certificate: C,
    private_key: PK,
) -> Result<Server, Box<dyn Error>> {
    let tls = s2n_quic_tls::Server::builder()
        .with_certificate(certificate, private_key)?
        .build()?;

    let server = Server::builder().with_tls(tls)?.with_io(ip)?.start()?;
    Ok(server)
}

pub enum QuitReason {
    ServerClosed,
    BrokenSender,
}
