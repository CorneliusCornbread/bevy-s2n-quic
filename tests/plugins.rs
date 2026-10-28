//! Bevy-level tests of the plugins: stream attempts, what happens when
//! streams and connections close, datagrams, aeronet sessions in both
//! modes, the connection limit, and several network threads.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use aeronet_io::{
    Session, SessionEndpoint,
    connection::{Disconnect, DisconnectReason, Disconnected, PeerAddr},
};
use bevy::{
    MinimalPlugins,
    app::{App, PluginGroup, PluginGroupBuilder},
    ecs::{
        entity::Entity,
        observer::On,
        query::{QueryFilter, With},
        resource::Resource,
        system::{Query, ResMut},
        world::World,
    },
};
use bevy_s2n_quic::{
    QuicAeronetPlugins, QuicDefaultPlugins, backend,
    client::{Connect, QuicClient, marker::QuicClientMarker},
    common::{
        connection::{
            QuicConnection, QuicConnectionAttempt, disconnect::ConnectionDisconnectReason,
        },
        error::{AppErrorCode, ConnectionError, DatagramError},
        plugin::DisconnectHandlerPlugin,
        runtime::QuicRuntime,
        stream::{
            receive::QuicReceiveStream,
            send::QuicSendStream,
            session::{QuicAeronetPacketPlugin, QuicSessionMode},
        },
    },
    config::{QuicClientConfig, QuicServerConfig},
    driver_plugin::QuicDriverPlugin,
    server::{QuicServer, marker::QuicServerMarker},
};
use bytes::Bytes;

const TIMEOUT: Duration = Duration::from_secs(30);

fn app(plugins: impl FnOnce(PluginGroupBuilder) -> PluginGroupBuilder) -> App {
    app_with(plugins, |_| {})
}

/// Like [app], running `setup` before the app is finished.
fn app_with(
    plugins: impl FnOnce(PluginGroupBuilder) -> PluginGroupBuilder,
    setup: impl FnOnce(&mut App),
) -> App {
    let mut app = App::new();
    app.add_plugins((MinimalPlugins, plugins(QuicDefaultPlugins.build())));
    setup(&mut app);
    app.finish();
    app.cleanup();
    app
}

fn cert_path() -> String {
    format!("{}/examples/certs/cert.pem", env!("CARGO_MANIFEST_DIR"))
}

fn spawn_server(app: &mut App) -> SocketAddr {
    let key = format!("{}/examples/certs/key.pem", env!("CARGO_MANIFEST_DIR"));
    let config = QuicServerConfig::from_pem_files(cert_path(), key).unwrap();
    let runtime = app.world().resource::<QuicRuntime>();
    let server =
        QuicServer::bind(runtime, "127.0.0.1:0".parse().unwrap(), &config).unwrap();
    let addr = server.local_addr().unwrap();
    app.world_mut().spawn(server);
    addr
}

/// Spawns a client connecting to `addr`. Returns the entity holding the
/// connection attempt, which becomes the connection.
fn spawn_client(app: &mut App, addr: SocketAddr) -> Entity {
    let config = QuicClientConfig::new()
        .with_root_pem_file(cert_path())
        .unwrap();
    let runtime = app.world().resource::<QuicRuntime>();
    let mut client = QuicClient::new(runtime, &config).unwrap();
    let attempt =
        client.open_connection(Connect::new(addr).with_server_name("localhost"));
    let attempt = app.world_mut().spawn(attempt).id();
    app.world_mut().spawn(client).add_child(attempt);
    attempt
}

/// Runs frames until `found` returns `Some`.
fn update_until<T>(
    app: &mut App,
    what: &str,
    mut found: impl FnMut(&mut World) -> Option<T>,
) -> T {
    let start = Instant::now();
    loop {
        assert!(start.elapsed() < TIMEOUT, "timed out while {what}");
        app.update();
        if let Some(value) = found(app.world_mut()) {
            return value;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn find<F: QueryFilter>(world: &mut World) -> Option<Entity> {
    world.query_filtered::<Entity, F>().iter(world).next()
}

/// Connects one client to a new server. Returns the client and server
/// connection entities.
fn connect(app: &mut App) -> (Entity, Entity) {
    let addr = spawn_server(app);
    let client = spawn_client(app, addr);
    update_until(app, "connecting", |world| {
        let server = find::<(With<QuicConnection>, With<QuicServerMarker>)>(world)?;
        world.get::<QuicConnection>(client)?;
        Some((client, server))
    })
}

/// `len` bytes counting up from `start`, wrapping at 251 so chunk boundaries
/// don't line up with the pattern.
fn pattern(start: usize, len: usize) -> Vec<u8> {
    (start..start + len).map(|i| (i % 251) as u8).collect()
}

/// Reads everything waiting on `stream`. Returns `false` once the stream has
/// ended and been drained.
fn read_into(world: &mut World, stream: Entity, received: &mut Vec<u8>) -> bool {
    let mut stream = world.get_mut::<QuicReceiveStream>(stream).unwrap();
    while let Some(packet) = stream.recv() {
        received.extend_from_slice(&packet.payload);
    }
    stream.is_open()
}

#[test]
fn send_stream_opens_and_dropping_it_still_delivers_queued_data() {
    const CHUNK: usize = 16 * 1024;
    const CHUNKS: usize = 32;

    // The test reads the server's stream after it ends, so it mustn't be
    // despawned.
    let mut app = app(|plugins| plugins.disable::<DisconnectHandlerPlugin>());
    let (client, _) = connect(&mut app);

    let attempt = app
        .world_mut()
        .get_mut::<QuicConnection>(client)
        .unwrap()
        .open_send_stream()
        .unwrap();
    let send = app.world_mut().spawn((attempt, QuicClientMarker)).id();
    update_until(&mut app, "opening the send stream", |world| {
        world.get::<QuicSendStream>(send).map(|_| ())
    });

    let mut stream = app.world_mut().get_mut::<QuicSendStream>(send).unwrap();
    for i in 0..CHUNKS {
        let chunk = Bytes::from(pattern(i * CHUNK, CHUNK));
        stream
            .send(chunk)
            .expect("the send buffer should have room");
    }
    // Despawned before the data has even been handed to the network thread.
    app.world_mut().entity_mut(send).despawn();

    let rec = update_until(&mut app, "accepting the stream", |world| {
        find::<(With<QuicReceiveStream>, With<QuicServerMarker>)>(world)
    });
    let mut received = Vec::new();
    update_until(&mut app, "receiving the data", |world| {
        (!read_into(world, rec, &mut received)).then_some(())
    });

    assert_eq!(received, pattern(0, CHUNK * CHUNKS));
    let reason = app
        .world_mut()
        .get_mut::<QuicReceiveStream>(rec)
        .unwrap()
        .get_disconnect_reason();
    assert!(
        matches!(reason, Some(ConnectionDisconnectReason::PeerClosed)),
        "unexpected reason: {reason:?}"
    );
}

#[test]
fn finished_receive_stream_is_kept_until_its_data_is_read() {
    let mut app = app(|plugins| plugins);
    let (client, _) = connect(&mut app);

    let attempt = app
        .world_mut()
        .get_mut::<QuicConnection>(client)
        .unwrap()
        .open_send_stream()
        .unwrap();
    let send = app.world_mut().spawn((attempt, QuicClientMarker)).id();
    update_until(&mut app, "opening the send stream", |world| {
        world.get::<QuicSendStream>(send).map(|_| ())
    });

    let data = pattern(0, 4096);
    let mut stream = app.world_mut().get_mut::<QuicSendStream>(send).unwrap();
    stream.send(Bytes::from(data.clone())).unwrap();
    stream.close().unwrap();

    let rec = update_until(&mut app, "accepting the stream", |world| {
        find::<(With<QuicReceiveStream>, With<QuicServerMarker>)>(world)
    });
    // The client's stream is despawned once the server has acknowledged
    // everything, finish included.
    update_until(&mut app, "finishing the send stream", |world| {
        world.get_entity(send).is_err().then_some(())
    });
    for _ in 0..20 {
        app.update();
        std::thread::sleep(Duration::from_millis(1));
    }

    // The stream has ended, but its data hasn't been read.
    let stream = app
        .world()
        .get::<QuicReceiveStream>(rec)
        .expect("despawned with unread data");
    assert!(stream.is_open());

    let mut received = Vec::new();
    assert!(!read_into(app.world_mut(), rec, &mut received));
    assert_eq!(received, data);
    update_until(&mut app, "despawning the drained stream", |world| {
        world.get_entity(rec).is_err().then_some(())
    });
}

/// Packets of several sizes, for checking they arrive whole.
fn session_packets(large: usize) -> Vec<Bytes> {
    vec![
        Bytes::from_static(b"hello"),
        Bytes::new(),
        Bytes::from(pattern(0, large)),
        Bytes::from_static(b"world"),
    ]
}

/// Runs frames, collecting the packets `entity`'s session receives, until
/// `count` have arrived. Packets must be taken every frame, or aeronet
/// clears them.
fn receive_session_packets(app: &mut App, entity: Entity, count: usize) -> Vec<Bytes> {
    let mut received = Vec::new();
    update_until(app, "receiving the packets", |world| {
        let mut session = world.get_mut::<Session>(entity)?;
        received.extend(session.recv.drain(..).map(|packet| packet.payload));
        (received.len() >= count).then_some(())
    });
    received
}

#[test]
fn datagram_sessions_carry_packets_between_connections() {
    let mut app = app_with(
        |plugins| plugins,
        |app| {
            app.add_plugins(QuicAeronetPlugins);
        },
    );
    let (client, server) = connect(&mut app);
    // Sessions are added when the connections are inserted.
    app.update();

    for entity in [client, server] {
        let session = app.world().get::<Session>(entity).expect("no session");
        assert!(session.min_mtu() > 1000);
        assert!(session.mtu() >= session.min_mtu());
        assert!(app.world().get::<PeerAddr>(entity).is_some());
    }
    let mtu = app.world().get::<Session>(client).unwrap().mtu();

    app.world_mut()
        .get_mut::<Session>(client)
        .unwrap()
        .send
        .extend(session_packets(mtu));
    let mut received = receive_session_packets(&mut app, server, 4);
    // Datagrams may be reordered.
    received.sort();
    let mut sent = session_packets(mtu);
    sent.sort();
    assert_eq!(received, sent);

    let stats = app.world().get::<Session>(server).unwrap().stats;
    assert_eq!(stats.packets_recv.0, 4);
    let stats = app.world().get::<Session>(client).unwrap().stats;
    assert_eq!(stats.packets_sent.0, 4);
    let conn = app.world().get::<QuicConnection>(client).unwrap();
    assert_eq!(conn.stats().datagrams_dropped, 0);
}

#[test]
fn stream_sessions_keep_packet_boundaries() {
    let mut app = app_with(
        |plugins| plugins,
        |app| {
            app.add_plugins(
                QuicAeronetPlugins
                    .set(QuicAeronetPacketPlugin::new(QuicSessionMode::Streams)),
            );
        },
    );
    let (client, server) = connect(&mut app);
    app.update();
    // Connections have no session in stream mode.
    assert!(app.world().get::<Session>(client).is_none());
    assert!(app.world().get::<Session>(server).is_none());

    let attempt = app
        .world_mut()
        .get_mut::<QuicConnection>(client)
        .unwrap()
        .open_bidrectional_stream()
        .unwrap();
    let stream = app.world_mut().spawn((attempt, QuicClientMarker)).id();
    assert!(app.world().get::<SessionEndpoint>(stream).is_some());
    update_until(&mut app, "opening the stream", |world| {
        world.get::<Session>(stream).map(|_| ())
    });

    // Larger than a QUIC packet, so the stream splits it.
    let packets = session_packets(20_000);
    app.world_mut()
        .get_mut::<Session>(stream)
        .unwrap()
        .send
        .extend(packets.clone());

    let server_stream = update_until(&mut app, "accepting the stream", |world| {
        find::<(With<QuicReceiveStream>, With<QuicServerMarker>, With<Session>)>(world)
    });
    assert_eq!(receive_session_packets(&mut app, server_stream, 4), packets);
}

#[test]
fn connection_datagrams_reach_the_peer() {
    let mut app = app(|plugins| plugins);
    let (client, server) = connect(&mut app);

    let conn = app.world().get::<QuicConnection>(client).unwrap();
    let max = conn.max_datagram_size().expect("datagrams unsupported");
    assert_eq!(
        conn.send_datagram(Bytes::from(vec![0; max + 1])),
        Err(DatagramError::TooLarge { size: max + 1, max })
    );
    conn.send_datagram(Bytes::from_static(b"ping")).unwrap();

    let packet = update_until(&mut app, "receiving the datagram", |world| {
        world
            .get_mut::<QuicConnection>(server)
            .unwrap()
            .recv_datagram()
    });
    assert_eq!(&packet.payload[..], b"ping");

    let conn = app.world().get::<QuicConnection>(client).unwrap();
    conn.close(AppErrorCode::UNKNOWN);
    assert_eq!(
        conn.send_datagram(Bytes::from_static(b"late")),
        Err(DatagramError::Closed)
    );
}

#[derive(Resource, Default)]
struct AeronetDisconnects(Vec<(Entity, String)>);

#[test]
fn aeronet_disconnect_reason_reaches_the_peer() {
    let mut app = app_with(
        |plugins| plugins,
        |app| {
            app.add_plugins(QuicAeronetPlugins);
        },
    );
    app.init_resource::<AeronetDisconnects>().add_observer(
        |event: On<Disconnected>, mut disconnects: ResMut<AeronetDisconnects>| {
            let reason = match &event.reason {
                DisconnectReason::ByPeer(reason) => format!("peer: {reason}"),
                DisconnectReason::ByUser(reason) => format!("user: {reason}"),
                DisconnectReason::ByError(e) => format!("error: {e}"),
            };
            disconnects.0.push((event.entity, reason));
        },
    );
    let (client, server) = connect(&mut app);

    app.world_mut()
        .trigger(Disconnect::new(client, "game over"));
    app.update();
    // aeronet despawns the session.
    assert!(app.world().get_entity(client).is_err());

    update_until(&mut app, "disconnecting the server", |world| {
        world.get_entity(server).is_err().then_some(())
    });
    let disconnects = &app.world().resource::<AeronetDisconnects>().0;
    assert!(disconnects.contains(&(client, "user: game over".to_owned())));
    assert!(
        disconnects.contains(&(server, "peer: game over".to_owned())),
        "{disconnects:?}"
    );
}

#[derive(Resource, Default)]
struct Disconnects(Vec<(Entity, Option<ConnectionDisconnectReason>)>);

#[test]
fn close_reaches_the_peer_and_triggers_disconnected() {
    let mut app = app(|plugins| plugins);
    app.init_resource::<Disconnects>().add_observer(
        |event: On<Disconnected>,
         mut conns: Query<&mut QuicConnection>,
         mut disconnects: ResMut<Disconnects>| {
            // Runs before the disconnect handler removes the connection.
            let reason = conns
                .get_mut(event.entity)
                .ok()
                .and_then(|mut conn| conn.get_disconnect_reason());
            disconnects.0.push((event.entity, reason));
        },
    );
    let (client, server) = connect(&mut app);

    let code = AppErrorCode::from(42u32);
    app.world()
        .get::<QuicConnection>(client)
        .unwrap()
        .close(code);
    assert!(!app.world().get::<QuicConnection>(client).unwrap().is_open());

    let reason = update_until(&mut app, "disconnecting the server", |world| {
        let disconnects = world.resource::<Disconnects>();
        disconnects
            .0
            .iter()
            .find(|(entity, _)| *entity == server)
            .map(|(_, reason)| reason.clone())
    });
    match reason {
        Some(ConnectionDisconnectReason::ConnectionError(
            ConnectionError::ApplicationClosed { code: received, .. },
        )) => assert_eq!(received, code),
        other => panic!("unexpected reason: {other:?}"),
    }
    assert!(app.world().get::<QuicConnection>(server).is_none());
}

#[test]
fn server_refuses_connections_past_its_limit() {
    let mut app =
        app(|plugins| plugins.set(QuicDriverPlugin::default().with_max_connections(1)));
    let addr = spawn_server(&mut app);

    let first = spawn_client(&mut app, addr);
    update_until(&mut app, "connecting the first client", |world| {
        world.get::<QuicConnection>(first).map(|_| ())
    });

    let second = spawn_client(&mut app, addr);
    update_until(&mut app, "refusing the second client", |world| {
        world
            .get::<QuicConnectionAttempt>(second)
            .is_none()
            .then_some(())
    });
    assert!(app.world().get::<QuicConnection>(second).is_none());
    assert!(app.world().get::<QuicConnection>(first).unwrap().is_open());
}

#[test]
fn several_threads_serve_every_client_with_one_wake_per_frame() {
    const THREADS: usize = 3;
    const CLIENTS: usize = 5;
    const BYTES: usize = 64 * 1024;

    let mut app =
        app(|plugins| plugins.set(QuicDriverPlugin::default().with_threads(THREADS)));
    let addr = spawn_server(&mut app);
    let clients: Vec<Entity> =
        (0..CLIENTS).map(|_| spawn_client(&mut app, addr)).collect();

    let mut wakes = backend::driver_metrics(app.world().resource::<QuicRuntime>());
    assert_eq!(wakes.len(), THREADS);
    let mut update = |app: &mut App| {
        app.update();
        let now = backend::driver_metrics(app.world().resource::<QuicRuntime>());
        for (before, after) in wakes.iter().zip(&now) {
            assert!(
                after.wakes - before.wakes <= 1,
                "woke a thread more than once in a frame"
            );
        }
        wakes = now;
        std::thread::sleep(Duration::from_millis(1));
    };

    let start = Instant::now();
    while clients
        .iter()
        .any(|&client| app.world().get::<QuicConnection>(client).is_none())
    {
        assert!(start.elapsed() < TIMEOUT, "timed out connecting");
        update(&mut app);
    }

    for &client in &clients {
        let conn = app.world().get::<QuicConnection>(client).unwrap();
        assert!(conn.rtt() > Duration::ZERO, "stats weren't mirrored");
        assert!(conn.current_mtu() >= 1200);

        let attempt = app
            .world_mut()
            .get_mut::<QuicConnection>(client)
            .unwrap()
            .open_send_stream()
            .unwrap();
        app.world_mut()
            .spawn((attempt, QuicClientMarker, ClientStream { sent: false }));
    }

    let mut received = 0;
    while received < CLIENTS * BYTES {
        assert!(start.elapsed() < TIMEOUT, "timed out transferring data");
        update(&mut app);

        let world = app.world_mut();
        let mut senders = world.query::<(&mut QuicSendStream, &mut ClientStream)>();
        for (mut stream, mut client) in senders.iter_mut(world) {
            if !client.sent {
                stream.send(Bytes::from(vec![7; BYTES])).unwrap();
                stream.close().unwrap();
                client.sent = true;
            }
        }

        let mut receivers =
            world.query_filtered::<&mut QuicReceiveStream, With<QuicServerMarker>>();
        for mut stream in receivers.iter_mut(world) {
            while let Some(packet) = stream.recv() {
                received += packet.payload.len();
            }
        }
    }
    assert_eq!(received, CLIENTS * BYTES);
}

#[derive(bevy::ecs::component::Component)]
struct ClientStream {
    sent: bool,
}
