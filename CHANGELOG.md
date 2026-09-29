# Changelog

## Unreleased

### Added

- `QuicAsyncPlugin::with_max_tasks(n)` limits how many connection and stream
  tasks run at once. Past the limit, new connections and streams are closed
  with `StatusCode::ServiceUnavailable` and report
  `ConnectionDisconnectReason::OrchestratorError`, as before. There is no
  limit by default.
- `TokioRuntime::spawner()` and `TaskSpawner::new`, so connections and streams
  can be created outside Bevy (for example in tests).

### Deprecated

- `common::orchestrator::OrchestratorHandle` (and `common::orchestrator::handle`)
  is now a deprecated alias of `common::spawner::TaskSpawner`. It will be
  removed in the next breaking release.

### Changed

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
  closes. Later `send` calls return `TrySendError::Closed`. `flush` sends the
  data queued before the flush request first. When the component is dropped,
  queued data is still sent before the stream is closed.
- Task tracing spans cover the whole task at `debug` level (`quic_send_task`,
  `quic_rec_task`, `quic_connection_task`) instead of one `info` span per poll.

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

- Data loss on send streams under load. A partially sent batch could be
  dropped, which lost data whenever the network couldn't keep up
  (3.4 GB lost in the benchmark reproducer, now 0).
- Accept requests could fail with a dropped response when the short accept
  poll didn't finish in one orchestrator poll. Receive streams could report
  closed before they finished stopping.
- A task that panics or is cancelled now reports
  `ConnectionDisconnectReason::InternalError(TaskAborted)`. Its component used
  to report `is_open() == true` forever.
- A send stream now closes on a connection error instead of retrying forever.
