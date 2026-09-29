# Changelog

## Unreleased

### Changed

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
