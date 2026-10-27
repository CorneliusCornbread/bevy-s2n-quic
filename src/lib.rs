//! # bevy-s2n-quic
//!
//! An Aeronet compatible Bevy plugin providing network IO with QUIC based on
//! [quinn-proto](https://github.com/quinn-rs/quinn).
//!
//! ## Getting Started
//!
//! Add the default plugins to your app:
//!
//! ```no_run
//! use bevy::prelude::*;
//! use bevy_s2n_quic::QuicDefaultPlugins;
//!
//! App::new()
//!     .add_plugins(QuicDefaultPlugins);
//! ```
//!
//! See the simple_net_system example for a basic setup of connecting a server and client
//!
//! ## Feature Flags
//!
//! | Flag | Description |
//! |------|-------------|
//! | `connection-errors` | Adds error components when connections fail |
//! | `stream-errors` | Adds error components when streams fail |
//! | `performance-warns` | Warns when buffers fill faster than they drain (default) |

pub mod backend;
pub mod client;
pub mod common;
pub mod config;
// Datagrams, stream resets and a few accessors aren't exposed yet.
#[allow(dead_code)]
pub(crate) mod driver;
pub mod driver_plugin;
pub mod server;

use bevy::app::{PluginGroup, PluginGroupBuilder};

use crate::{
    client::acceptor::SimpleClientAcceptorPlugin,
    common::{
        connection::plugin::ConnectionAttemptPlugin,
        plugin::DisconnectHandlerPlugin,
        stream::{
            plugin::StreamAttemptPlugin,
            session::{QuicAeronetEventPlugin, QuicAeronetPacketPlugin},
        },
    },
    driver_plugin::QuicDriverPlugin,
    server::acceptor::SimpleServerAcceptorPlugin,
};

/// The default set of plugins needed to make the Bevy Quic components
/// function and handle errors and connection attempts.
pub struct QuicDefaultPlugins;

impl PluginGroup for QuicDefaultPlugins {
    fn build(self) -> bevy::app::PluginGroupBuilder {
        PluginGroupBuilder::start::<Self>()
            .add(QuicDriverPlugin::default())
            .add(ConnectionAttemptPlugin)
            .add(StreamAttemptPlugin)
            .add(SimpleServerAcceptorPlugin)
            .add(SimpleClientAcceptorPlugin)
            .add(DisconnectHandlerPlugin)
    }
}

/// The plugin group for Aeronet event handling.
pub struct QuicAeronetPlugins;

impl PluginGroup for QuicAeronetPlugins {
    fn build(self) -> bevy::app::PluginGroupBuilder {
        PluginGroupBuilder::start::<Self>()
            .add(QuicAeronetPacketPlugin)
            .add(QuicAeronetEventPlugin)
    }
}
