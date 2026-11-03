//! Loopback integration test through the Bevy plugins.
//!
//! Pushes enough data through one stream to hit backpressure end to end and
//! checks that every byte arrives exactly once and in order.

use std::time::{Duration, Instant};

use bevy::{
    MinimalPlugins,
    app::{App, PluginGroup},
    ecs::{
        entity::Entity,
        query::{QueryFilter, With},
        world::{Mut, World},
    },
};
use bevy_s2n_quic::{
    QuicDefaultPlugins,
    client::{Connect, QuicClient, marker::QuicClientMarker},
    common::{
        connection::{QuicConnection, disconnect::ConnectionDisconnectReason},
        error::SendError,
        plugin::DisconnectHandlerPlugin,
        runtime::QuicRuntime,
        stream::{receive::QuicReceiveStream, send::QuicSendStream},
    },
    config::{QuicClientConfig, QuicServerConfig},
    server::{QuicServer, marker::QuicServerMarker},
};
use bytes::Bytes;

/// Size of each chunk handed to `QuicSendStream::send`.
const CHUNK_SIZE: usize = 4096;
/// Total number of chunks sent over the stream (32 MiB).
const CHUNK_COUNT: u64 = 8192;
const WORDS_PER_CHUNK: u64 = (CHUNK_SIZE / 8) as u64;

/// Small, odd `recv_many` limit so reads that stop at the limit are
/// exercised, mixed with single `recv` calls.
const RECV_LIMIT: usize = 7;

const TIMEOUT: Duration = Duration::from_secs(120);
/// How long `send` must keep reporting a full buffer to count as stalled.
const STALL_TIME: Duration = Duration::from_millis(500);

/// Chunk `index` holds the little-endian u64 word indices it covers, so any
/// lost, duplicated or reordered data changes the byte sequence.
fn chunk(index: u64) -> Bytes {
    let mut data = Vec::with_capacity(CHUNK_SIZE);
    for word in index * WORDS_PER_CHUNK..(index + 1) * WORDS_PER_CHUNK {
        data.extend_from_slice(&word.to_le_bytes());
    }
    Bytes::from(data)
}

fn deadline_check(start: Instant, what: &str) {
    assert!(start.elapsed() < TIMEOUT, "timed out while {what}");
}

fn find<F: QueryFilter>(world: &mut World) -> Option<Entity> {
    world.query_filtered::<Entity, F>().iter(world).next()
}

/// Runs frames until `found` returns `Some`.
fn update_until<T>(
    app: &mut App,
    start: Instant,
    what: &str,
    mut found: impl FnMut(&mut World) -> Option<T>,
) -> T {
    loop {
        deadline_check(start, what);
        app.update();
        if let Some(value) = found(app.world_mut()) {
            return value;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn send_stream(app: &mut App, entity: Entity) -> Mut<'_, QuicSendStream> {
    app.world_mut()
        .get_mut(entity)
        .expect("send stream despawned")
}

fn receive_stream(app: &mut App, entity: Entity) -> Mut<'_, QuicReceiveStream> {
    app.world_mut()
        .get_mut(entity)
        .expect("receive stream despawned")
}

#[test]
fn stream_delivers_all_data_in_order_under_backpressure() {
    let mut app = App::new();
    // Disconnected streams would be despawned, but this test reads their
    // disconnect reasons.
    let plugins = QuicDefaultPlugins
        .build()
        .disable::<DisconnectHandlerPlugin>();
    app.add_plugins((MinimalPlugins, plugins));
    app.finish();
    app.cleanup();

    let manifest = env!("CARGO_MANIFEST_DIR");
    let cert = format!("{manifest}/examples/certs/cert.pem");
    let key = format!("{manifest}/examples/certs/key.pem");

    let server_config = QuicServerConfig::from_pem_files(&cert, &key).unwrap();
    let client_config = QuicClientConfig::new().with_root_pem_file(&cert).unwrap();

    let runtime = app.world().resource::<QuicRuntime>();
    let server =
        QuicServer::bind(runtime, "127.0.0.1:0".parse().unwrap(), &server_config)
            .unwrap();
    let addr = server.local_addr().unwrap();
    let mut client = QuicClient::new(runtime, &client_config).unwrap();
    let attempt =
        client.open_connection(Connect::new(addr).with_server_name("localhost"));

    app.world_mut().spawn(server);
    app.world_mut().spawn(client).with_children(|parent| {
        parent.spawn(attempt);
    });

    let start = Instant::now();

    let client_conn = update_until(&mut app, start, "connecting", |world| {
        find::<(With<QuicConnection>, With<QuicClientMarker>)>(world)
    });

    let stream_attempt = app
        .world_mut()
        .get_mut::<QuicConnection>(client_conn)
        .unwrap()
        .open_bidrectional_stream()
        .unwrap();
    app.world_mut().spawn((stream_attempt, QuicClientMarker));

    let send = update_until(&mut app, start, "opening the stream", |world| {
        find::<(With<QuicSendStream>, With<QuicClientMarker>)>(world)
    });

    // The peer only sees the stream once data is sent on it.
    send_stream(&mut app, send).send(chunk(0)).unwrap();

    let rec = update_until(&mut app, start, "accepting the stream", |world| {
        find::<(With<QuicReceiveStream>, With<QuicServerMarker>)>(world)
    });

    // Send without reading until the whole pipeline stalls: the send buffer,
    // QUIC flow control and the receive buffer are all full. If the
    // receiver dropped data instead of pushing back, this would never stall.
    let mut next = 1;
    let mut pending = Some(chunk(next));
    let mut last_progress = Instant::now();

    while let Some(data) = pending.take() {
        deadline_check(start, "filling the pipeline");

        match send_stream(&mut app, send).send(data) {
            Ok(()) => {
                next += 1;
                pending = (next < CHUNK_COUNT).then(|| chunk(next));
                last_progress = Instant::now();
            }
            Err(SendError::Full(data)) => {
                pending = Some(data);
                if last_progress.elapsed() > STALL_TIME {
                    break;
                }
                app.update();
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(SendError::Closed(_)) => panic!("send stream closed early"),
        }
    }

    assert!(
        pending.is_some(),
        "queued all {CHUNK_COUNT} chunks without the receiver pushing back"
    );

    // Now read and send concurrently until everything has arrived.
    let total_bytes = CHUNK_COUNT as usize * CHUNK_SIZE;
    let mut received = Vec::with_capacity(total_bytes);
    let mut packets = Vec::new();
    let mut closing = false;

    loop {
        deadline_check(start, "transferring data");
        app.update();

        let mut sender = send_stream(&mut app, send);
        if let Some(data) = pending.take() {
            match sender.send(data) {
                Ok(()) => {
                    next += 1;
                    pending = (next < CHUNK_COUNT).then(|| chunk(next));
                }
                Err(SendError::Full(data)) => pending = Some(data),
                Err(SendError::Closed(_)) => panic!("send stream closed early"),
            }
        } else if !closing {
            // Close only after everything is queued. Queued data is still sent.
            closing = sender.close().is_some();
        }

        let mut receiver = receive_stream(&mut app, rec);
        let open = receiver.is_open();
        packets.clear();
        let mut count = receiver.recv_many(&mut packets, RECV_LIMIT);
        assert!(
            count <= RECV_LIMIT,
            "recv_many returned more than its limit"
        );
        assert_eq!(count, packets.len(), "recv_many count doesn't match");

        // Mix in single receives, which must continue in the same order.
        if let Some(packet) = receiver.recv() {
            packets.push(packet);
            count += 1;
        }

        for packet in &packets {
            received.extend_from_slice(&packet.payload);
        }

        assert!(
            received.len() <= total_bytes,
            "received more bytes than were sent"
        );

        // A receive stream reports closed only once its data has all been
        // read.
        if !open && count == 0 {
            break;
        }

        if count == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    assert_eq!(next, CHUNK_COUNT, "not every chunk was queued");
    assert!(closing, "close request was never queued");
    assert_eq!(received.len(), total_bytes, "lost data");

    for (i, word) in received.as_chunks::<8>().0.iter().enumerate() {
        let value = u64::from_le_bytes(*word);
        assert_eq!(value, i as u64, "data out of order at byte {}", i * 8);
    }

    // The sender finishes after the peer acknowledges all data.
    update_until(&mut app, start, "closing the send stream", |world| {
        let stream = world.get::<QuicSendStream>(send).unwrap();
        (!stream.is_open()).then_some(())
    });

    assert!(
        matches!(
            send_stream(&mut app, send).get_disconnect_reason(),
            Some(ConnectionDisconnectReason::UserClosed)
        ),
        "unexpected send disconnect reason"
    );
    assert!(
        matches!(
            receive_stream(&mut app, rec).get_disconnect_reason(),
            Some(ConnectionDisconnectReason::PeerClosed)
        ),
        "unexpected receive disconnect reason"
    );
}
