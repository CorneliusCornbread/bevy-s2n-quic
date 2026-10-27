# Changelog

## Unreleased

### Added

- `QuicDriverPlugin::with_max_connections(n)` limits how many connections
  each server has open at once. Past the limit, new connections are refused
  and the client's `QuicConnectionAttempt` fails with
  `QuicActionError::ConnectionFailed`. There is no limit by default. Streams
  are no longer limited.
- `QuicDriverPlugin::with_thread_name(name)` and
  `QuicDriverPlugin::on_thread_start(hook)` name the network threads and run
  a hook on each one as it starts (for pinning or priority). The
  `quic_concurrency_stress` example uses the hook to pin and nice the threads.
- `QuicRuntime::default_thread_count()` is public, to help size other thread
  pools around the runtime. `QuicRuntime::thread_count()` returns the number
  of network threads.
- Connection statistics: `QuicConnection::stats()` returns a
  `ConnectionStats` with the round-trip time, congestion window, packets sent
  and lost, current MTU and bytes sent and received. `rtt()` and
  `current_mtu()` are shortcuts. The statistics are updated at most every
  50 ms while the connection is active.
- `QuicConnection::remote_addr()`.
- `QuicServer::local_addr()`, for servers bound to port `0`.
- `config::QuicServerConfig` and `config::QuicClientConfig`. Certificates and
  keys load from PEM files or from rustls-pki-types `CertificateDer` /
  `PrivateKeyDer` values, which are re-exported from `config`.
- `common::error::AppErrorCode`, `common::error::ConnectionError` and
  `common::error::SendError`.

### Removed

- `common::orchestrator` (`OrchestratorHandle`).
- The `ConnectionErrorDisconnected` trait, which was implemented only for
  s2n-quic's error type.
- `MissingErrorData`, which was unused.

### Deprecated

- `QuicSendStream::flush` does nothing. Data is always sent as fast as flow
  and congestion control allow. It still returns `None` once the stream is
  closed.

### Changed

- **The QUIC backend is now quinn-proto instead of s2n-quic, and Tokio is no
  longer used.**
  - One or more network threads each run an event loop over their sockets,
    connections and timers. Every server and client is assigned to one
    thread, and its connections run on that thread too.
  - Once per frame, `poll_driver_events` (in `IoSystems::Poll`, `PreUpdate`)
    applies what the network threads reported to the components. Commands
    such as `send`, `close` and opening streams are queued without waking a
    thread, and handed over in `Last` with at most one wake per thread per
    frame. The benchmark went from about 600 wakes per frame at 600 sending
    clients to 2.
  - Component state (`is_open()`, disconnect reasons, received data, stats)
    is mirrored from the network threads in `IoSystems::Poll`, so it can be
    up to one frame out of date. Systems that read it should run after
    `IoSystems::Poll`; systems in `Update` already do.
  - A stream opened with `open_bidrectional_stream` or `open_send_stream`
    can be used from the frame after it's requested.
- **The default number of network threads is now 1, or 2 on machines with
  at least 8 logical processors.** It used to be 25% of the logical
  processors, and at least 2.
- **Backpressure counts bytes instead of messages.** Each send stream
  accepts up to 1 MiB that hasn't been sent yet. Past that,
  `QuicSendStream::send` returns `SendError::Full`. A single write larger
  than that is accepted when nothing else is queued. Each receive stream
  buffers up to 1 MiB that hasn't been read. Past that, the stream stops
  reading, and QUIC flow control slows the peer down. Data used to be
  dropped with a "channel is full" error. A stream that is never read now
  stalls its peer.
- **`QuicReceiveStream::is_open()` stays `true` until the stream has ended
  and all of its data has been read**, and `get_disconnect_reason()` returns
  `None` until then. `DisconnectHandlerPlugin` therefore no longer despawns
  a receive stream that still has unread data.
- **Dropping a component closes what it holds:**
  - Dropping a `QuicSendStream` finishes the stream. Data already queued is
    still sent.
  - Dropping a `QuicReceiveStream` asks the peer to stop sending.
  - Dropping a `QuicConnection` closes the connection.
  - Dropping a `QuicServer` or `QuicClient` stops it accepting or making
    connections. Connections it already has stay open.
  - Dropping an attempt before it resolves closes the connection or stream
    it was waiting for.
- `QuicSendStream::close` finishes the stream once the data queued before it
  has been sent. Later `send` calls return `SendError::Closed`. The stream
  closes, with `ConnectionDisconnectReason::UserClosed`, once the peer has
  acknowledged everything. `close` returns `None` only if the stream is
  already closed.
- `QuicConnection::close` is sent to the peer, which sees
  `ConnectionError::ApplicationClosed` with the same code. Data not yet sent
  on the connection's streams is lost. `is_open()` returns `false` right
  away.
- `QuicConnection::open_bidrectional_stream` and `open_send_stream` return
  `ConnectionCommandError::Closed` if the connection is closed.
  `ConnectionCommandError::Full` is no longer returned.
- `QuicConnection::accept_stream`, `accept_receive_stream` and
  `accept_bidirectional_stream` return `None` unless the peer has opened a
  stream, and the attempt they return is already resolved.
- `QuicReceiveStream::stop_send` discards data that hasn't been read, and
  never fails.
- `log_outstanding_errors` on both stream types logs the stream's
  disconnect reason once, if it closed because of an error.
- A refused or failed connection reports the transport error instead of
  `ConnectionDisconnectReason::OrchestratorError`.
- **The public API no longer exposes s2n-quic or Tokio types.** Migration:
  - `QuicAsyncPlugin` is now `driver_plugin::QuicDriverPlugin`.
    `QuicAsyncPlugin::new_with_threads(n)` becomes
    `QuicDriverPlugin::default().with_threads(n)`.
  - `TokioRuntime` is now `QuicRuntime`. It no longer exposes `handle()` or
    `spawner()`. `default_worker_count()` is now `default_thread_count()`.
  - `QuicServer::bind(&runtime, addr, cert, key)` is now
    `QuicServer::bind(&runtime, addr, &QuicServerConfig)` and returns
    `ConfigError`.
  - `QuicClient::new(&runtime)` and `QuicClient::new_with_tls(&runtime, cert)`
    are replaced by `QuicClient::new(&runtime, &QuicClientConfig)`, which
    returns a `Result`. The default config trusts the platform's roots. Add
    a self-signed certificate with `QuicClientConfig::with_root_pem_file`.
  - `client::Connect` replaces `s2n_quic::client::Connect` and has the same
    `Connect::new(addr).with_server_name(name)` shape.
  - `QuicConnection::close`, `QuicReceiveStream::stop_send` and
    `ConnectionDisconnectReason::Reset` use `AppErrorCode` instead of
    `s2n_quic::application::Error`. `AppErrorCode` converts from `u8`, `u16`,
    `u32` and `StatusCode`, and `AppErrorCode::new(u64)` checks the 2⁶² − 1
    limit. `AERONET_DISCONNECT_CODE` is now an `AppErrorCode`.
  - `ConnectionDisconnectReason::ConnectionError` and
    `QuicActionError::ConnectionFailed` hold the crate's `ConnectionError`
    instead of `s2n_quic::connection::Error`.
  - `QuicSendStream::send` and `send_many_drain` return `SendError<Bytes>`
    instead of Tokio's `TrySendError<Bytes>`. The variants are the same:
    `Full` and `Closed`.
  - `QuicServer::accept_connection` returns `ConnectionPoll` instead of
    `Result<ConnectionPoll, JoinError>`. It could never fail.
  - `QuicConnectionAttempt` resolves to a `QuicConnection` instead of an s2n
    `Connection`.
  - `StatusCode` converts into `AppErrorCode` instead of the s2n error type.
  - Constructors that took s2n or Tokio types are now crate-private:
    `QuicConnection::new`, `QuicSendStream::new`, `QuicReceiveStream::new`,
    `QuicPeerStream::new`, every `*Attempt::new` and
    `QuicActionAttempt::new`. So are `ConnectionId::new` and `StreamId::new`.
  - `ConnectionCommandError` moved from `common::connection::task` to
    `common::connection`.
- `aeronet_session_recv` now runs in aeronet's `IoSystems::Poll`, after
  `poll_driver_events`, and `aeronet_session_send` runs in `IoSystems::Flush`
  (`PostUpdate`) instead of `PreUpdate`. Packets written to `Session::send`
  go out the same frame.
- Idle connections no longer use CPU: the benchmark went from about 4 busy
  cores at any load to under 0.1 cores with 600 idle connections.
- Network threads are named `bevy-s2n-quic` instead of
  `tokio-runtime-worker`. Override it with `with_thread_name`.
- `QuicReceiveStream::recv_many` **no longer blocks**. It returns only the packets
  already waiting, up to `limit`, and returns `0` when there are none. It used to
  block the calling (Bevy) thread until at least one packet arrived or the stream
  closed. `0` no longer means the stream is closed: check `is_open()`. The
  signature is unchanged.
- `QuicSendStream::close` and `QuicReceiveStream::stop_send` no longer
  block.

### Fixed

- `QuicConnection::open_send_stream` never produced a `QuicSendStream`:
  `StreamAttemptPlugin` didn't resolve `QuicSendStreamAttempt`.
- `DisconnectHandlerPlugin` could despawn a receive stream whose peer had
  finished while data was still unread, losing that data.
- With aeronet's `AeronetIoPlugin`, packets written to `Session::send` were
  cleared at the end of the frame (after `IoSystems::Flush`) before
  `aeronet_session_send` read them in the next `PreUpdate`, so they were never
  sent.
- Data loss on send streams under load. A partially sent batch could be
  dropped, which lost data whenever the network couldn't keep up
  (3.4 GB lost in the benchmark reproducer, now 0).
- Accept requests could fail with a dropped response when the short accept
  poll didn't finish in one orchestrator poll. Receive streams could report
  closed before they finished stopping.
- A send stream now closes on a connection error instead of retrying forever.
