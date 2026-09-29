//! Loopback integration test on a plain Tokio runtime (no Bevy).
//!
//! Pushes enough data through one stream to hit backpressure end to end and
//! checks that every byte arrives exactly once and in order.

use std::{
    path::Path,
    time::{Duration, Instant},
};

use bevy_s2n_quic::common::{
    QuicParentId, QuicParentType,
    connection::{disconnect::ConnectionDisconnectReason, id::ConnectionId},
    spawner::TaskSpawner,
    stream::{receive::QuicReceiveStream, send::QuicSendStream},
};
use bytes::Bytes;
use s2n_quic::{Client, Server, client::Connect};
use tokio::sync::mpsc::error::TrySendError;

/// Size of each chunk handed to `QuicSendStream::send`.
const CHUNK_SIZE: usize = 4096;
/// Total number of chunks sent over the stream (32 MiB).
const CHUNK_COUNT: u64 = 8192;
const WORDS_PER_CHUNK: u64 = (CHUNK_SIZE / 8) as u64;

/// Small, odd `recv_many` limit so reads that stop at the limit are
/// exercised, mixed with single `recv` calls.
const RECV_LIMIT: usize = 7;

const TIMEOUT: Duration = Duration::from_secs(120);
/// How long `send` must keep reporting a full channel to count as stalled.
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

#[test]
fn stream_delivers_all_data_in_order_under_backpressure() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let spawner = TaskSpawner::new(runtime.handle().clone(), None);

    let manifest = env!("CARGO_MANIFEST_DIR");
    let cert = format!("{manifest}/examples/certs/cert.pem");
    let key = format!("{manifest}/examples/certs/key.pem");

    let (mut server, client) = runtime.block_on(async {
        let server_tls = s2n_quic_tls::Server::builder()
            .with_certificate(Path::new(&cert), Path::new(&key))
            .unwrap()
            .build()
            .unwrap();
        let server = Server::builder()
            .with_tls(server_tls)
            .unwrap()
            .with_io("127.0.0.1:0")
            .unwrap()
            .start()
            .unwrap();

        let client_tls = s2n_quic_tls::Client::builder()
            .with_certificate(Path::new(&cert))
            .unwrap()
            .build()
            .unwrap();
        let client = Client::builder()
            .with_tls(client_tls)
            .unwrap()
            .with_io("0.0.0.0:0")
            .unwrap()
            .start()
            .unwrap();

        (server, client)
    });

    let addr = server.local_addr().unwrap();

    // Open a send stream from the client. Both connections must stay alive for
    // the whole test.
    let (client_conn, send) = runtime.block_on(async {
        let mut client_conn = client
            .connect(Connect::new(addr).with_server_name("localhost"))
            .await
            .unwrap();
        let send = client_conn.open_send_stream().await.unwrap();
        (client_conn, send)
    });

    let client_id = ConnectionId::new(
        client_conn.id(),
        QuicParentId::generate_unique(QuicParentType::Client),
    );
    let mut send = QuicSendStream::new(spawner.clone(), send, client_id);

    // The peer only sees the stream once data is sent on it.
    send.send(chunk(0)).unwrap();

    let (server_conn, rec) = runtime.block_on(async {
        let mut server_conn = server.accept().await.unwrap();
        let rec = server_conn.accept_receive_stream().await.unwrap().unwrap();
        (server_conn, rec)
    });

    let server_id = ConnectionId::new(
        server_conn.id(),
        QuicParentId::generate_unique(QuicParentType::Server),
    );
    let mut rec = QuicReceiveStream::new(spawner, rec, server_id);

    let start = Instant::now();

    // Send without reading until the whole pipeline stalls: the outbound
    // channel, QUIC flow control and the inbound channel are all full. If the
    // receiver dropped data instead of pushing back, this would never stall.
    let mut next = 1;
    let mut pending = Some(chunk(next));
    let mut last_progress = Instant::now();

    while let Some(data) = pending.take() {
        deadline_check(start, "filling the pipeline");

        match send.send(data) {
            Ok(()) => {
                next += 1;
                pending = (next < CHUNK_COUNT).then(|| chunk(next));
                last_progress = Instant::now();
            }
            Err(TrySendError::Full(data)) => {
                pending = Some(data);
                if last_progress.elapsed() > STALL_TIME {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(TrySendError::Closed(_)) => panic!("send stream closed early"),
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

        if let Some(data) = pending.take() {
            match send.send(data) {
                Ok(()) => {
                    next += 1;
                    pending = (next < CHUNK_COUNT).then(|| chunk(next));
                }
                Err(TrySendError::Full(data)) => pending = Some(data),
                Err(TrySendError::Closed(_)) => panic!("send stream closed early"),
            }
        } else if !closing {
            // Close only after everything is queued. Queued data is still sent.
            closing = send.close().is_some();
        }

        let open = rec.is_open();
        packets.clear();
        let mut count = rec.recv_many(&mut packets, RECV_LIMIT);
        assert!(count <= RECV_LIMIT, "recv_many returned more than its limit");
        assert_eq!(count, packets.len(), "recv_many count doesn't match");

        // Mix in single receives, which must continue in the same order.
        if let Some(packet) = rec.recv() {
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

        // All data is in the channel before the task reports closed, so a
        // drain after seeing it closed gets everything.
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
    while send.is_open() {
        deadline_check(start, "closing the send stream");
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(
        matches!(
            send.get_disconnect_reason(),
            Some(ConnectionDisconnectReason::UserClosed)
        ),
        "unexpected send disconnect reason"
    );
    assert!(
        matches!(
            rec.get_disconnect_reason(),
            Some(ConnectionDisconnectReason::PeerClosed)
        ),
        "unexpected receive disconnect reason"
    );

    drop((client_conn, server_conn));
}
