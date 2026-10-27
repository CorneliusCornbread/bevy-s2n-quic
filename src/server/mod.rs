use std::{collections::VecDeque, io, net::SocketAddr};

use bevy::ecs::component::Component;

use crate::{
    common::{
        QuicParentId, QuicParentType,
        connection::QuicConnection,
        router::Notice,
        runtime::{Link, QuicRuntime},
    },
    config::{ConfigError, QuicServerConfig},
    driver::{Command, EndpointId, EndpointSetup},
    server::marker::QuicServerMarker,
};

pub mod acceptor;
pub mod marker;

/// The component which manages an instance of a QuicServer.
///
/// It is recommended you parent any [QuicConnection] to their related QuicServer entity.
///
/// Dropping the server stops it accepting connections. Connections it
/// already accepted stay open.
#[derive(Component)]
#[require(QuicServerMarker)]
pub struct QuicServer {
    link: Link,
    endpoint: EndpointId,
    local_addr: SocketAddr,
    id: QuicParentId,
    accepted: VecDeque<QuicConnection>,
    closed: bool,
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
        let setup = EndpointSetup::server(config, runtime.endpoint_options())?;
        let link = runtime.link(runtime.pick_driver());
        let id = QuicParentId::generate_unique(QuicParentType::Server);

        // Registered first, so the router knows the server before any
        // connection arrives.
        let endpoint = link.driver.ids().endpoint();
        link.notify(Notice::Endpoint {
            endpoint,
            parent: id,
        });
        let local_addr = match link.driver.open_endpoint(endpoint, bind_addr, setup) {
            Ok(addr) => addr,
            Err(e) => {
                link.notify(Notice::EndpointReleased { endpoint });
                return Err(e);
            }
        };

        Ok(Self {
            link,
            endpoint,
            local_addr,
            id,
            accepted: VecDeque::new(),
            closed: false,
        })
    }

    /// Takes the oldest connection that finished its handshake, if any.
    pub fn accept_connection(&mut self) -> ConnectionPoll {
        match self.accepted.pop_front() {
            Some(connection) => ConnectionPoll::NewConnection(connection),
            None if self.closed => ConnectionPoll::ServerClosed,
            None => ConnectionPoll::None,
        }
    }

    /// The local address the server is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    pub fn id(&self) -> QuicParentId {
        self.id
    }

    pub(crate) fn endpoint(&self) -> EndpointId {
        self.endpoint
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn receive(&mut self, accepted: Vec<QuicConnection>, closed: bool) {
        self.accepted.extend(accepted);
        self.closed = closed;
    }
}

impl Drop for QuicServer {
    fn drop(&mut self) {
        self.link.send(Command::ReleaseEndpoint {
            endpoint: self.endpoint,
        });
        self.link.notify(Notice::EndpointReleased {
            endpoint: self.endpoint,
        });
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ConnectionPoll {
    None,
    ServerClosed,
    NewConnection(QuicConnection),
}

pub enum QuitReason {
    ServerClosed,
    BrokenSender,
}
