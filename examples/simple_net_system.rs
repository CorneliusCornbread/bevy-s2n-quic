/// This is a simple example showing how to utilize both the client and server components
/// to connect and send data between endpoints. This example puts both the client and server
/// in one ECS world but this would function all the same between two different instances of
/// Bevy.
use bevy::{
    DefaultPlugins,
    app::{App, PostUpdate, Startup, Update},
    ecs::{
        component::Component,
        entity::Entity,
        hierarchy::{ChildOf, Children},
        query::{With, Without},
        schedule::IntoScheduleConfigs,
        system::{Commands, Query, Res},
    },
    input::{common_conditions::input_just_pressed, keyboard::KeyCode},
    log::{error, info},
    prelude::PluginGroup,
    remote::{RemotePlugin, http::RemoteHttpPlugin},
    render::{
        RenderPlugin,
        settings::{PowerPreference, WgpuSettings},
    },
};
use bevy_s2n_quic::{
    QuicDefaultPlugins,
    client::{Connect, QuicClient, marker::QuicClientMarker},
    common::{
        connection::QuicConnection,
        runtime::QuicRuntime,
        stream::{receive::QuicReceiveStream, send::QuicSendStream},
    },
    config::{QuicClientConfig, QuicServerConfig},
    server::{QuicServer, marker::QuicServerMarker},
};
use std::net::SocketAddr;

fn main() {
    let _app = App::new()
        .add_plugins(
            // Set the default render settings to low power,
            // works nicer on dGPU systems with the dGPU disabled.
            // We're not rendering anything anyways.
            DefaultPlugins.set(RenderPlugin {
                render_creation: WgpuSettings {
                    power_preference: PowerPreference::LowPower,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }),
        )
        // These are the default plugins, they do not include the Aeronet functionality
        .add_plugins(QuicDefaultPlugins)
        // These plugins can be skipped however they make for easy debugging and viewing of state of the ECS world
        .add_plugins(RemotePlugin::default())
        .add_plugins(RemoteHttpPlugin::default())
        .add_systems(Startup, setup)
        .add_systems(PostUpdate, (client_open_stream, debug_receive, debug_send))
        .add_systems(
            PostUpdate,
            client_send.run_if(input_just_pressed(KeyCode::Space)),
        )
        .add_systems(
            PostUpdate,
            stop_receive.run_if(input_just_pressed(KeyCode::Digit0)),
        )
        .add_systems(
            PostUpdate,
            close_send.run_if(input_just_pressed(KeyCode::Digit9)),
        )
        .add_systems(Update, add_debug)
        .run();
}

/// Server address we want to connect to
const IP: &str = "127.0.0.1:7777";

fn setup(mut commands: Commands, runtime: Res<QuicRuntime>) {
    // We need to load our certs and keys to make a proper connection.
    // QUIC requires TLS to function, so we use self-signed test certs
    // and have the client trust them.

    // REMEMBER TO USE DIFFERENT CERTS IN A PRODUCTION ENVIRONMENT!!
    let cert_path = format!("{}/examples/certs/cert.pem", env!("CARGO_MANIFEST_DIR"));
    let key_path = format!("{}/examples/certs/key.pem", env!("CARGO_MANIFEST_DIR"));

    let ip: SocketAddr = IP.parse().unwrap();
    info!("IP set to: {}", ip);
    // Spawn a new server with our given cert and private key
    let server_config =
        QuicServerConfig::from_pem_files(&cert_path, &key_path).expect("Invalid cert");
    let server_comp = QuicServer::bind(&runtime, ip, &server_config)
        .expect("Unable to bind to server address");

    commands.spawn(server_comp);

    // Spawn a new client which trusts our cert
    let client_config = QuicClientConfig::new()
        .with_root_pem_file(&cert_path)
        .expect("Invalid cert");
    let mut client_comp =
        QuicClient::new(&runtime, &client_config).expect("Unable to start client");
    let connect = Connect::new(ip).with_server_name("localhost");
    let conn_bundle = client_comp.open_connection(connect);

    // Spawn client with connection attempt as child
    commands.spawn(client_comp).with_children(|parent| {
        parent.spawn(conn_bundle);
    });
}

#[derive(Component)]
struct DebugCount(u64);

#[allow(clippy::type_complexity)]
fn client_open_stream(
    mut commands: Commands,
    connection_query: Query<
        (Entity, &mut QuicConnection),
        (Without<Children>, With<QuicClientMarker>),
    >,
) {
    // As soon as we see a connection without any children,
    // attempt to spawn a bidirectional stream on the client end.
    // Both servers and clients may open streams.
    for (entity, mut connection) in connection_query {
        let stream_bundle = connection
            .open_bidrectional_stream()
            .expect("Unable to open new client stream");
        commands.spawn((
            stream_bundle,
            DebugCount(0),
            ChildOf(entity),
            QuicClientMarker,
        ));
    }
}

fn client_send(
    client_streams: Query<(&mut QuicSendStream, &mut DebugCount), With<QuicClientMarker>>,
) {
    // As soon as we see an open client stream send a message when this system is run
    // (only when spacebar is pressed)
    for (mut send_stream, mut debug_count) in client_streams {
        let res = send_stream.send("Yippieee".into());
        if let Err(e) = res {
            error!("Error sending data: {}", e);
            continue;
        }

        debug_count.0 += 1;

        info!("Message send count: {}", debug_count.0);
    }
}

/// Keeps track of the number of messages sent and received
#[allow(clippy::type_complexity)]
fn add_debug(
    mut commands: Commands,
    query: Query<
        (Entity, &QuicReceiveStream),
        (Without<DebugCount>, With<QuicServerMarker>),
    >,
) {
    for (entity, _stream) in &query {
        commands.entity(entity).insert(DebugCount(0));
    }
}

// Output our current message count with the data of the message itself.
// Also log any outstanding errors from our receive stream.
#[allow(clippy::type_complexity)]
fn debug_receive(
    server_receivers: Query<
        (&mut QuicReceiveStream, &mut DebugCount),
        (With<QuicServerMarker>, Without<QuicClientMarker>),
    >,
    client_receivers: Query<
        &mut QuicReceiveStream,
        (With<QuicClientMarker>, Without<QuicServerMarker>),
    >,
) {
    for (mut stream, mut debug) in server_receivers {
        stream.log_outstanding_errors();

        if !stream.is_open() {
            continue;
        }

        if let Some(data) = stream.recv() {
            let bytes = data.payload;
            let string = String::from_utf8_lossy(&bytes);
            info!("Received message: '{}'", string);
            debug.0 += 1;
            let count = debug.0;

            info!("Message rec count: {}", count);
        }
    }

    for mut stream in client_receivers {
        stream.log_outstanding_errors();
    }
}

// Log any outstanding errors from our streams
fn debug_send(
    server_receivers: Query<
        &mut QuicSendStream,
        (With<QuicServerMarker>, Without<QuicClientMarker>),
    >,
    client_receivers: Query<
        &mut QuicSendStream,
        (With<QuicClientMarker>, Without<QuicServerMarker>),
    >,
) {
    for mut stream in server_receivers {
        stream.log_outstanding_errors();
    }

    for mut stream in client_receivers {
        stream.log_outstanding_errors();
    }
}

fn stop_receive(receivers: Query<&mut QuicReceiveStream, With<QuicServerMarker>>) {
    for mut stream in receivers {
        stream.stop_send(0u8.into());
    }
}

fn close_send(senders: Query<&mut QuicSendStream, With<QuicClientMarker>>) {
    for mut stream in senders {
        stream.close();
    }
}
