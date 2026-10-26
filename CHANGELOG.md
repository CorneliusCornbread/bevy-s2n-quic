# Changelog

## Unreleased

### Added

- `QuicDriverPlugin::with_max_tasks(n)` limits how many connection and stream
  tasks run at once. Past the limit, new connections and streams are closed
  with `StatusCode::ServiceUnavailable` and report
  `ConnectionDisconnectReason::OrchestratorError`, as before. There is no
  limit by default.
- `QuicDriverPlugin::with_thread_name(name)` and
  `QuicDriverPlugin::on_thread_start(hook)` name the network threads and run
  a hook on each one as it starts (for pinning or priority). The
  `quic_concurrency_stress` example uses the hook to pin and nice the threads.
- `QuicRuntime::default_thread_count()` is public, to help size other thread
  pools around the runtime.
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

### Changed

- **The public API no longer exposes s2n-quic or Tokio types.** The QUIC
  backend can now change without further breaking changes. Migration:
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
    `QuicActionAttempt::new`. So are `ConnectionId::new`, `StreamId::new`,
    `TaskSpawner`, `TaskResult`, `TaskError` and `TaskAborted`.
  - `ConnectionCommandError` moved from `common::connection::task` to
    `common::connection`.

- `aeronet_session_recv` now runs in aeronet's `IoSystems::Poll`, and
  `aeronet_session_send` runs in `IoSystems::Flush` (`PostUpdate`) instead of
  `PreUpdate`. Packets written to `Session::send` go out the same frame.
- The async orchestrator (dispatcher plus spin-polling workers) is replaced by
  one long-lived Tokio task per connection and stream. Idle connections no
  longer use CPU: the benchmark went from ~4 busy cores at any load to
  0.04–0.8 cores. The per-orchestrator channel limits are gone, so a burst of
  new connections no longer fails with `OrchestratorError` unless
  `with_max_tasks` is set.
- `QuicReceiveStream` **applies backpressure instead of dropping data.** When
  the inbound channel is full, the stream stops reading until Bevy drains it,
  and QUIC flow control slows the peer down. Data used to be dropped with a
  "channel is full" error. A stream that is never read now stalls its peer.
- `QuicSendStream::close` sends the data queued before the close request, then
  closes. Later `send` calls return `SendError::Closed`. `flush` sends the
  data queued before the flush request first. When the component is dropped,
  queued data is still sent before the stream is closed.
- Task tracing spans cover the whole task at `debug` level (`quic_send_task`,
  `quic_rec_task`, `quic_connection_task`) instead of one `info` span per poll.
- Network threads are named `bevy-s2n-quic` instead of
  `tokio-runtime-worker`. Override it with `with_thread_name`.

- `QuicReceiveStream::recv_many` **no longer blocks**. It returns only the packets
  already waiting, up to `limit`, and returns `0` when there are none. It used to
  block the calling (Bevy) thread until at least one packet arrived or the stream
  closed. `0` no longer means the stream is closed: check `is_open()`. The
  signature is unchanged.
- `QuicSendStream::close`, `QuicSendStream::flush` and `QuicReceiveStream::stop_send`
  no longer block when their control channel is full. `close` and `flush` still
  return `Option<()>`, but `None` now also means the channel was full and the
  request was dropped. `stop_send` logs a warning in that case.
- `log_outstanding_errors` on both stream types no longer blocks.

### Fixed

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
- A task that panics or is cancelled now reports
  `ConnectionDisconnectReason::InternalError`. Its component used
  to report `is_open() == true` forever.
- A send stream now closes on a connection error instead of retrying forever.
