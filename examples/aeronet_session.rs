//! Sends packets through aeronet sessions over QUIC, with a server and a
//! client in one headless app.
//!
//! The client sends numbered pings through its session, and the server
//! echoes each one back. Once the pongs are in, the client disconnects with
//! a reason, which the server receives, and the app exits.
//!
//! By default each connection is a session and packets are QUIC datagrams.
//! Pass `--streams` to make the client open a stream and use it as the
//! session instead:
//!
//! ```text
//! cargo run --example aeronet_session
//! cargo run --example aeronet_session -- --streams
//! ```
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use aeronet_io::{
    Session,
    connection::{Disconnect, DisconnectReason, Disconnected},
};
use bevy::{
    MinimalPlugins,
    app::{App, AppExit, PluginGroup, ScheduleRunnerPlugin, Startup, Update},
    ecs::{
        component::Component,
        entity::Entity,
        message::MessageWriter,
        observer::On,
        query::{With, Without},
        resource::Resource,
        system::{Commands, Query, Res, ResMut},
    },
    log::{LogPlugin, error, info},
};
use bevy_s2n_quic::{
    QuicAeronetPlugins, QuicDefaultPlugins,
    client::{Connect, QuicClient, marker::QuicClientMarker},
    common::{
        connection::QuicConnection,
        runtime::QuicRuntime,
        stream::session::{QuicAeronetPacketPlugin, QuicSessionMode},
    },
    config::{QuicClientConfig, QuicServerConfig},
    server::{QuicServer, marker::QuicServerMarker},
};
use bytes::Bytes;

const ADDR: &str = "127.0.0.1:7778";
const PINGS: u32 = 100;
/// How long to wait for pongs after the last ping.
const PONG_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Resource)]
struct Demo {
    mode: QuicSessionMode,
    sent: u32,
    pongs: u32,
    last_ping: Option<Instant>,
    disconnected: bool,
}

fn main() -> AppExit {
    let mode = if std::env::args().any(|arg| arg == "--streams") {
        QuicSessionMode::Streams
    } else {
        QuicSessionMode::Datagrams
    };

    App::new()
        .add_plugins((
            MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                1.0 / 60.0,
            ))),
            LogPlugin::default(),
            QuicDefaultPlugins,
            QuicAeronetPlugins.set(QuicAeronetPacketPlugin::new(mode)),
        ))
        .insert_resource(Demo {
            mode,
            sent: 0,
            pongs: 0,
            last_ping: None,
            disconnected: false,
        })
        .add_observer(on_disconnected)
        .add_systems(Startup, setup)
        .add_systems(
            Update,
            (open_stream, client_ping, server_echo, client_count_pongs),
        )
        .run()
}

fn setup(mut commands: Commands, runtime: Res<QuicRuntime>, demo: Res<Demo>) {
    let cert = format!("{}/examples/certs/cert.pem", env!("CARGO_MANIFEST_DIR"));
    let key = format!("{}/examples/certs/key.pem", env!("CARGO_MANIFEST_DIR"));
    let addr: SocketAddr = ADDR.parse().unwrap();

    let server_config = QuicServerConfig::from_pem_files(&cert, &key).expect("Invalid cert");
    commands.spawn(QuicServer::bind(&runtime, addr, &server_config).expect("Can't bind"));

    let client_config = QuicClientConfig::new()
        .with_root_pem_file(&cert)
        .expect("Invalid cert");
    let mut client = QuicClient::new(&runtime, &client_config).expect("Can't start client");
    let attempt = client.open_connection(Connect::new(addr).with_server_name("localhost"));
    commands.spawn(client).with_children(|parent| {
        parent.spawn(attempt);
    });

    info!("Sending {PINGS} pings over {:?}", demo.mode);
}

#[derive(Component)]
struct StreamRequested;

/// In stream mode, the client opens the stream its session runs on.
fn open_stream(
    mut commands: Commands,
    demo: Res<Demo>,
    connections: Query<
        (Entity, &mut QuicConnection),
        (With<QuicClientMarker>, Without<StreamRequested>),
    >,
) {
    if demo.mode != QuicSessionMode::Streams {
        return;
    }
    for (entity, mut connection) in connections {
        let attempt = connection
            .open_bidrectional_stream()
            .expect("Can't open a stream");
        commands.entity(entity).insert(StreamRequested);
        commands.spawn((attempt, QuicClientMarker));
    }
}

/// Sends one ping per frame. Sessions are on the connection entity in
/// datagram mode, and on the stream entity in stream mode; this works with
/// either.
fn client_ping(
    mut demo: ResMut<Demo>,
    mut sessions: Query<&mut Session, With<QuicClientMarker>>,
) {
    if demo.sent >= PINGS {
        return;
    }
    for mut session in &mut sessions {
        let ping = format!("ping {}", demo.sent);
        session.send.push(Bytes::from(ping));
        demo.sent += 1;
        demo.last_ping = Some(Instant::now());
    }
}

fn server_echo(mut sessions: Query<&mut Session, With<QuicServerMarker>>) {
    for mut session in &mut sessions {
        let session = &mut *session;
        for packet in session.recv.drain(..) {
            let pong = String::from_utf8_lossy(&packet.payload).replace("ping", "pong");
            session.send.push(Bytes::from(pong));
        }
    }
}

fn client_count_pongs(
    mut commands: Commands,
    mut demo: ResMut<Demo>,
    mut sessions: Query<(Entity, &mut Session), With<QuicClientMarker>>,
    connections: Query<&QuicConnection, With<QuicClientMarker>>,
) {
    for (entity, mut session) in &mut sessions {
        demo.pongs += session.recv.drain(..).count() as u32;

        let timed_out = demo
            .last_ping
            .is_some_and(|at| demo.sent == PINGS && at.elapsed() > PONG_TIMEOUT);
        if demo.disconnected || (demo.pongs < PINGS && !timed_out) {
            continue;
        }

        for connection in &connections {
            info!(
                "Session MTU {} (min {}), RTT {:?}, datagrams dropped {}",
                session.mtu(),
                session.min_mtu(),
                connection.rtt(),
                connection.stats().datagrams_dropped
            );
        }
        info!("Received {} of {} pongs, disconnecting", demo.pongs, demo.sent);
        demo.disconnected = true;
        commands.trigger(Disconnect::new(entity, "all pongs received"));
    }
}

fn on_disconnected(
    disconnected: On<Disconnected>,
    servers: Query<(), With<QuicServerMarker>>,
    demo: Res<Demo>,
    mut exit: MessageWriter<AppExit>,
) {
    let side = if servers.contains(disconnected.entity) {
        "Server"
    } else {
        "Client"
    };
    match &disconnected.reason {
        DisconnectReason::ByUser(reason) => info!("{side} disconnected: {reason}"),
        DisconnectReason::ByPeer(reason) => info!("{side} disconnected by peer: {reason}"),
        DisconnectReason::ByError(e) => error!("{side} disconnected with an error: {e:#}"),
    }

    if side == "Server" {
        // Datagrams may be lost; stream packets may not.
        let ok = demo.pongs > 0
            && (demo.mode == QuicSessionMode::Datagrams || demo.pongs == demo.sent);
        exit.write(if ok { AppExit::Success } else { AppExit::error() });
    }
}
