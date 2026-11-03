# Migration plan: s2n-quic → quinn-proto

Plan for replacing the s2n-quic internals with
[quinn-proto](https://docs.rs/quinn-proto) (sans-IO) and
[quinn-udp](https://docs.rs/quinn-udp). Written against the `perf` branch
(Phase 4 + dependency update), quinn-proto 0.11.19 and quinn-udp 0.6.3.

The phases are ordered so the public API is decoupled from s2n-quic first,
while s2n-quic still runs underneath. That keeps the numbers in
[`perf-baseline.md`](perf-baseline.md) comparable until the backend is
actually swapped. Phases use letters (A–G) to avoid clashing with the
numbered phases in the perf baseline.

## Why

- s2n-quic hides connection and stream state behind async handles, so the
  crate needs `OpenFlag`, `StreamFlag`, `OnceLockState`, attempt components,
  oneshots and per-stream channels to mirror state into Bevy. Several state
  queries (for example "is this connection/stream open") don't exist at all.
- With a sans-IO library the connection is plain data the crate owns and can
  query directly (`is_closed`, `is_handshaking`, `rtt`, `stats`, stream and
  datagram APIs), and the crate chooses the threading model.
- Phase 2 of the perf baseline showed the remaining frame-time cost is about
  one remote Tokio wake per sending stream per frame. Owning the I/O loop lets
  that drop to one wake per driver thread per frame.
- quinn-udp already implements GSO, GRO, ECN and batched receive across
  Linux, macOS and Windows, and quinn-proto paces internally (pacing delays
  are reported through `poll_timeout`).

## Target architecture

```
Bevy (main thread)                           Driver thread(s), one mio event loop each
 QuicSendStream::send ──► command channel ──►  sockets (quinn-udp) + quinn_proto::Endpoint
 QuicConnection::close    (no wake per send)    connections + timer heap
 flush system (Last) ─────── waker.wake() ──►   (at most 1 wake per driver per frame)
 poll system (IoSystems::Poll) ◄── event channel ◄── stream data, datagrams, state, stats
```

- **No Tokio.** A small pool of driver threads (configurable, default 1–2),
  each running one mio event loop.
  - Each `QuicServer` or `QuicClient` endpoint is assigned to one driver, and
    its connections live on that same thread, so the endpoint and its
    connections never talk across threads.
  - The benchmark's 600 client endpoints are spread across the drivers.
- **Commands from Bevy go into a channel whose send doesn't wake the
  driver.** A flush system wakes each driver once per frame if it has pending
  commands.
- **Events come back on a channel** that a Bevy system drains each frame and
  applies to entities.
  - Components hold plain state mirrored from those events, so `is_open()`
    and stats are ordinary field reads.
  - This removes `OnceLockState`, `OpenFlag`, `StreamFlag`,
    `JoinHandleState`, `TaskSpawner` and the oneshot replies.
- **Backpressure counts bytes, not messages.** Today a 512-message channel
  of 64 KB chunks can buffer about 32 MB per stream.
  - Send budget: bytes queued by Bevy that quinn hasn't accepted yet.
  - Receive budget: bytes delivered to Bevy that it hasn't drained yet. The
    driver stops reading past it, so QUIC flow control slows the peer.
- **No backend types in the public API.** quinn types are reachable only
  through an explicit escape-hatch module.

## Decisions to settle first

1. **Crate name.** `bevy-s2n-quic` will no longer fit. Recommended: publish
   under a new name and make the last `bevy-s2n-quic` release point to it.
2. **TLS provider.** Recommended: default to `rustls-ring` (no C toolchain
   needed) and offer `aws-lc-rs` as a feature.
3. **aeronet mapping.** Recommended: one aeronet session per connection, with
   packets sent as QUIC datagrams. Stream mode stays available as an option
   (Phase D).
4. **Platforms.** mio and quinn-udp cover Linux, macOS and Windows. Browsers
   aren't supported; aeronet's WebTransport backend already covers those.

## Phase A: make the public API independent of s2n (still running on s2n)

- Replace s2n types in public signatures with crate-owned ones:
  - `QuicServerConfig` and `QuicClientConfig` (certificates from PEM paths or
    rustls `CertificateDer`/`PrivateKeyDer`) replace
    `IntoCertificate`/`IntoPrivateKey`.
  - `Connect { addr, server_name }` replaces `s2n_quic::client::Connect`.
  - `AppErrorCode(u64)` replaces `application::Error` in `close`,
    `stop_send` and `ConnectionDisconnectReason::Reset`.
  - A crate-owned `ConnectionError` enum shaped like quinn's (`TimedOut`,
    `Reset`, `ApplicationClosed { code, reason }`, `ConnectionClosed`,
    `LocallyClosed`, `VersionMismatch`, `Transport(String)`) replaces
    s2n's in `ConnectionDisconnectReason` and `QuicActionError`.
- Make the constructors that take s2n types `pub(crate)`:
  `QuicSendStream::new`, `QuicReceiveStream::new`, `QuicPeerStream::new` and
  `QuicConnection::new`.
- Remove the `s2n_quic` imports from the examples, the benchmark and
  `tests/loopback.rs`.

**Gate:** benchmark numbers match the Phase 4 baseline, and all tests pass.
After this, the backend swap is purely internal.

## Phase B: driver core, without Bevy (`src/driver/`)

The driver owns:

- the mio `Poll` and `Waker`
- a slab of sockets, each holding a mio `UdpSocket`, its `UdpSocketState`, a
  `quinn_proto::Endpoint` and any transmit still waiting to be sent
- a slab of connections, each holding its `quinn_proto::Connection`,
  per-stream state and a dirty flag
- a timer heap of `(Instant, conn, generation)` entries, where stale entries
  are skipped when popped
- a list of dirty connections, and the command and event channels

Each loop iteration:

```text
poll(timeout = earliest timer)
drain commands      → write / finish / reset / stop / open / datagram / close / connect / resume-read
receive (per readable socket, until WouldBlock):
  UdpSocketState::recv → split each GRO buffer by meta.stride → endpoint.handle(now, …)
    ConnectionEvent → conn.handle_event, mark dirty
    NewConnection(incoming) → accept, or refuse past the limit
    Response(transmit) → send
expired timers      → conn.handle_timeout(now), mark dirty
each dirty connection:
  conn.poll() events          → read streams and datagrams within budget, emit events to Bevy
  conn.poll_endpoint_events() → endpoint.handle_event(), feed any reply back to conn; Drained removes the conn
  conn.poll_transmit(now, state.max_gso_segments(), &mut buf) until None → UdpSocketState::send
  push conn.poll_timeout() onto the timer heap
```

### Things that have to be right

- **Endpoint events go both ways.** Forgetting `Drained` leaks connections,
  and new-ID or retire events stall connection-ID rotation.
- **Never drop a transmit.** On `WouldBlock`, keep it, register WRITABLE
  interest on the socket, and stop polling transmits until it drains.
- **Receive buffers are `gro_segments() × max UDP payload`**, read with
  `quinn_udp::BATCH_SIZE` buffers per call.
- **Reads:** call `recv_stream(id).read(true)` and consume chunks only up to
  the stream's receive budget. Always call `chunks.finalize()`, and if it
  returns `ShouldTransmit`, mark the connection dirty. Stopping early is how
  quinn's flow control pushes back on the peer.
- **Writes:** a `write` that returns `Blocked` leaves the rest queued on the
  driver until the `Writable` event. The stream's send budget is released
  only once quinn accepts the bytes.
- **Closing:** `conn.close(...)` keeps driving the connection until it has
  drained, then the endpoint removes it.
- **Incoming connections:** accept up to a configurable limit and refuse past
  it. This replaces `with_max_tasks`. Also set `ServerConfig::max_incoming`.
- **Keep-alive and idle timeout** come from
  `TransportConfig::keep_alive_interval` and `max_idle_timeout`. That
  replaces today's `keep_alive(true)`.
- **Inject the socket and clock** behind a small trait, so tests can wire two
  drivers through an in-memory pipe with a fake clock and injected loss or
  reordering.

**Gate:**

- Deterministic tests pass: handshake, in-order stream delivery under loss,
  datagrams, close and drain, idle timeout, refusal past the limit.
- Single-connection bulk throughput on loopback comes close to quinn's own
  `perf` tool on the same machine. A large gap means the I/O loop is the
  problem, not quinn-proto.

## Phase C: Bevy integration

- **`QuicDriverPlugin` replaces `QuicAsyncPlugin` and `TokioRuntime`.** It
  configures the driver count, thread names and an `on_thread_start` hook, so
  the Phase 4 pinning setup still works.
- **Two systems:**
  - `poll_driver_events` in `IoSystems::Poll` drains the event channels and
    updates components through a `(driver, conn, stream) → Entity` map.
  - `flush_driver_commands` in `IoSystems::Flush` wakes each driver that has
    pending commands. This is the only per-frame work: no pass over every
    stream, which is what made the rejected Phase 3 batching slower.
- **Components keep their names and methods.** Internally they hold IDs, a
  command sender and mirrored state.
  - Opening and accepting streams still produce attempt components, but
    events resolve them instead of polled JoinHandles. That removes
    `TaskResult` and the oneshot machinery.
- **Keep the behaviour recorded in the CHANGELOG:**
  - `recv_many` is non-blocking.
  - A full receive buffer applies backpressure instead of dropping data.
  - `close` sends queued data, then finishes.
  - Dropping a component finishes the stream.
  - `flush` becomes a deprecated no-op, because quinn-proto sends as fast as
    congestion control allows and has no flush.
- **Add connection stats to `QuicConnection`,** mirrored once per frame: RTT,
  congestion window, packets lost and current MTU (`rtt()`, `stats()`,
  `current_mtu()`).
- **Tests:** port `tests/loopback.rs`, and add a Bevy-level test with
  `MinimalPlugins`.

**Gate:** all tests pass, and the benchmark reports 0 lost bytes.

## Phase D: aeronet over datagrams

- **Put `Session` on the connection entity.**
  - Each `Session::send` packet goes out with `datagrams().send(bytes, true)`,
    where `true` drops the oldest queued datagram when the buffer is full.
    Count those drops and expose the count.
  - `datagrams().recv()` fills `Session::recv`.
- **MTU:** set the session's `min_mtu` from `datagrams().max_size()` at the
  QUIC minimum path MTU. Call `Session::set_mtu` as path MTU discovery raises
  it.
- **Configuration:** enable datagrams in `TransportConfig` with
  `datagram_receive_buffer_size` and `datagram_send_buffer_size`.
- **Stream mode:** keep it as an option, with length-prefixed framing. That
  fixes the current problem where receive chunk boundaries needn't match the
  packets that were sent.

**Gate:** the aeronet example works, and the benchmark's datagram scenario
runs.

## Phase E: benchmark and decide

- **Port `quic_concurrency_stress`** with the same flags, and add:
  - a server-broadcast scenario
  - one-way latency p50/p99, using a timestamp in the payload
  - a datagram scenario
  - a per-driver wake counter (replaces `remote_schedule_count`)
- **Investigate the bulk throughput gap found in Phase C.** The 64 KB,
  600-client run (`--payload 65536 --warmup 1 --duration 2`) received
  425 MB/s, against s2n's 626 MB/s (3,130 MB in 5 s). It lost 0 bytes, but
  `send full` was 65,284 and driver busy was 78% across 2 drivers. This was
  a single run with a 2 s window, so first repeat it with s2n's 5 s window.
  Then check, in order:
  - **Driver saturation:** the server endpoint and all its connections share
    one driver. Record per-driver busy time. If the server's driver is near
    100%, this is the Phase G trigger.
  - **Byte budgets:** the send and receive budgets are 1 MiB per stream.
    The high `send full` count suggests senders are waiting on them. Rerun
    with larger budgets, and consider exposing them on `QuicDriverPlugin`.
  - **The I/O loop:** run the ignored `bulk_throughput` driver test against
    quinn's `perf` tool. This Phase B gate was never run. A large gap there
    points at GSO, GRO, pacing or the per-turn send and receive limits
    (`MAX_TRANSMIT_DATAGRAMS`, `MAX_RECV_DATAGRAMS`).
  - **Flow control:** quinn's default stream and connection receive windows,
    compared with s2n's.

**Gate** (same machine as [`perf-baseline.md`](perf-baseline.md), with the
`perf` branch's s2n backend run alongside for comparison):

- The data-loss reproducer still loses 0 bytes.
- At 600 active, frame avg and CPU are at or below Phase 2. Frame avg is
  expected to drop, since wakes go from about 600 per frame to a handful.
- Bulk received in the 64 KB, 600-client run is at least s2n's (3,130 MB in
  5 s), or the gap is recorded.
- Broadcast and latency numbers are recorded for both backends.

**Outcome** (details in [`perf-baseline.md`](perf-baseline.md#phase-e-quinn-proto-driver)):

- The benchmark has the four additions, plus per-driver busy % and
  `--stream-budget`. The byte budgets are exposed as
  `QuicDriverPlugin::with_stream_budgets`.
- Three fixes: GSO sends are capped at 10 segments and 65,507 bytes (larger
  ones were silently lost, which cut one connection to 44 MiB/s, against
  757 MiB/s now and 785–812 for quinn's `bulk`). `MAX_RECV_DATAGRAMS` is now
  4096. The acceptor plugins now take every waiting connection and stream
  each frame, not one.
- Gate: the reproducer loses 0 bytes, and broadcast and latency are recorded.
  At 600 active, CPU is below Phase 2 (0.50 against 0.78 cores). Frame avg
  (0.49–0.51 ms) is at or below s2n's in the same session, but not below
  Phase 2's recorded 0.31 ms.
- Bulk is **not** at s2n's level: 2,535 MB in 5 s with 2 threads and
  3,697 MB with 4, against 5,145 MB for s2n. Budgets, flow-control windows,
  ACK frequency and the crypto provider made no difference. The server's
  driver is at 100% in every bulk run, so **Phase G is triggered**.

### Single-stream throughput

Added after Phase E. The bulk run above is a total over 600 streams, so it
says nothing about one large download. One stream moves at most about one
send budget and one receive budget per frame, because the app hands data
over once per frame. Its throughput is then roughly `budget × frame rate`,
until the network thread becomes the limit.

How to run it:

```sh
# Through the Bevy API: one connection, one stream, filled every tick.
# `download` is server → client, `upload` is client → server.
cargo run --release --example quic_concurrency_stress -- \
    --clients 1 --scenarios download,upload --payload 65536 --duration 5 \
    --stream-budget 4194304 [--hz 240] [--out results.md]

# The driver alone, without Bevy: 2 GiB on one stream.
cargo test --release --lib driver::tests::bulk_throughput -- --ignored --nocapture

# quinn's own tool, for a ceiling on the same machine, from a quinn
# checkout at the `quinn-proto-0.11.19` tag:
cargo run --release -p bench --bin bulk -- --upload-size 2G --download-size 0
```

`--stream-budget` sets both the send and receive budget, and the default is
1 MiB. `--hz` sets the frame rate and the fixed tick together (headless
only). The `rx/s` column is the throughput.

Results are in
[`perf-baseline.md`](perf-baseline.md#single-stream-through-the-bevy-api).
The default 1 MiB budget gives 60 MB/s at 60 Hz. 16 MiB at 60 Hz, or 4 MiB at
240 Hz, reaches 740–765 MB/s, which is the driver's own single-connection
speed. Phase G doesn't change this: quinn-proto runs a connection on one
thread, so spreading connections over cores only helps many-client totals.
The s2n backend wasn't measured, because its benchmark port is gone.

## Phase F: remove s2n and release

- **Dependencies:** remove `s2n-quic`, `s2n-quic-tls`, `tokio` and
  `futures`, and add a feature for the TLS provider.
- **Release:** write a migration guide, and update the CHANGELOG and README
  under the new crate name.
- **CI:** add `cargo test` (CI only runs clippy today) and short benchmark
  smoke runs that fail on lost bytes: `--clients 50 --duration 3`, and
  `--clients 1 --scenarios download,upload --payload 65536 --duration 3`.

## Phase G: multi-core server (only if Phase E shows one server thread saturating)

- **Option 1:** the endpoint stays on one driver and its connections are
  spread across the others, exchanging connection and endpoint events over
  channels. This is what quinn's own async crate does.
- **Option 2:** use `SO_REUSEPORT` with several sockets, each with its own
  endpoint on its own driver. Simpler, but connection migration breaks it,
  and its behaviour is Linux-specific.

**Outcome:** Option 1, compared with the others in
[`phase-g-multicore.md`](phase-g-multicore.md). The gate is met (details in
[`perf-baseline.md`](perf-baseline.md#phase-g-multi-core-server)): 600 × 64 KB
bulk receives 1,187 MB/s with 4 threads in one process (664 without
spreading, 1,029 for s2n), and 1,979 MB/s with the server in its own process.
The reproducer loses 0 bytes, and 600-active CPU and frame avg are slightly
below Phase E's. The server's Bevy main thread, which handles every received
chunk, is now the limit in the separate-process run.

## Risks

- **Bulk throughput may trail s2n** until GSO, GRO and pacing are right. The
  Phase B comparison with quinn's `perf` tool catches this early.
- **Mirrored state is up to one frame stale.** `is_open()` reflects the last
  event poll; document this on the components.
- **quinn-proto is pre-1.0.** Pin it, and keep all quinn calls inside
  `src/driver/` so a 0.12 upgrade touches one module.
