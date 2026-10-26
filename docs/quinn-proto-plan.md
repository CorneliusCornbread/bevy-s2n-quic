Target architecture
Bevy (main thread) Driver thread(s), one mio event loop each
QuicSendStream::send ──► command channel ──► sockets (quinn-udp) + quinn_proto::Endpoint
QuicConnection::close (no wake per send) connections + timer heap
flush system (Last) ─────── waker.wake() ──► (at most 1 wake per driver per frame)
poll system (IoSystems::Poll) ◄── event channel ◄── stream data, datagrams, state, stats
No Tokio. Run a small pool of driver threads (configurable, default 1–2), each with one mio event loop.
Each QuicServer or QuicClient endpoint is assigned to one driver, and its connections live on that same thread. This avoids cross-thread traffic between the endpoint and its connections.
The benchmark’s 600 client endpoints get spread across the drivers.
Commands from Bevy go into a channel that doesn’t wake the driver. A flush system wakes each driver once per frame if it has pending commands. Phase 2 found the remaining cost was about one remote wake per sending stream per frame; this design brings that down to the number of drivers.
Events come back on a channel that a Bevy system drains each frame and applies to entities.
Components hold plain state mirrored from those events, so is_open() and stats are ordinary field reads.
That removes OnceLockState, OpenFlag, StreamFlag, JoinHandleState, TaskSpawner and the oneshot replies.
Backpressure counts bytes, not messages. That fixes the case where a 512-message channel of 64 KB chunks could buffer about 32 MB per stream.
No backend types in the public API. quinn types are only reachable through an explicit escape-hatch module.
Decisions to settle first
Crate name. bevy-s2n-quic will no longer fit. I’d publish a new name and make the last bevy-s2n-quic release point to it.
TLS provider. I’d default to rustls-ring, which needs no C toolchain, and offer aws-lc-rs as a feature.
aeronet mapping. I’d make the aeronet session one per connection, with packets sent as QUIC datagrams, and keep stream mode as an option (Phase D).
Platforms. mio and quinn-udp cover Linux, macOS and Windows. Browsers aren’t supported; aeronet’s WebTransport backend already covers those.
Phase A: make the public API independent of s2n (still running on s2n)
Replace s2n types in public signatures with the crate’s own:
QuicServerConfig and QuicClientConfig (certificates from PEM paths or rustls CertificateDer/PrivateKeyDer) replace IntoCertificate/IntoPrivateKey.
A Connect { addr, server_name } type replaces s2n_quic::client::Connect.
AppErrorCode(u64) replaces application::Error in close, stop_send and Reset.
A crate-owned ConnectionError enum shaped like quinn’s (TimedOut, Reset, ApplicationClosed { code, reason }, ConnectionClosed, LocallyClosed, VersionMismatch, Transport(String)) replaces s2n’s in ConnectionDisconnectReason and QuicActionError.
Make the constructors that take s2n types pub(crate). That’s QuicSendStream::new, QuicReceiveStream::new, QuicPeerStream::new and QuicConnection::new.
Remove the s2n_quic imports from the examples, the benchmark and tests/loopback.rs.
Gate: benchmark numbers match the Phase 4 baseline, and all tests pass. After this, the swap is purely internal.
Phase B: driver core, without Bevy (src/driver/)

The driver owns:

the mio Poll and Waker
a slab of sockets, each holding a mio UdpSocket, its UdpSocketState, a quinn_proto::Endpoint and any transmit still waiting to be sent
a slab of connections, each holding its quinn_proto::Connection, per-stream state and a dirty flag
a timer heap of (Instant, conn, generation) entries, where stale entries are skipped when popped
a list of dirty connections, and the command and event channels

Each loop iteration:

text
poll(timeout = earliest timer)
drain commands → write / finish / reset / stop / open / datagram / close / connect / resume-read
receive (per readable socket, until WouldBlock):
UdpSocketState::recv → split each GRO buffer by meta.stride → endpoint.handle(now, …)
ConnectionEvent → conn.handle_event, mark dirty
NewConnection(incoming) → accept, or refuse past the limit
Response(transmit) → send
expired timers → conn.handle_timeout(now), mark dirty
each dirty connection:
conn.poll() events → read streams and datagrams within budget, emit events to Bevy
conn.poll_endpoint_events() → endpoint.handle_event(), feed any reply back to conn; Drained removes the conn
conn.poll_transmit(now, state.max_gso_segments(), &mut buf) until None → UdpSocketState::send
push conn.poll_timeout() onto the timer heap

Things that have to be right:

Endpoint events go both ways. Forgetting Drained leaks connections, and new-ID or retire events stall connection-ID rotation.
Never drop a transmit. On WouldBlock, keep it, register WRITABLE interest on the socket, and stop polling transmits until it drains.
Receive buffers are gro_segments() × max UDP payload, read with quinn_udp::BATCH_SIZE buffers per call.
Reads: call recv_stream(id).read(true) and consume chunks only up to the stream’s receive budget. Always call chunks.finalize(), and if it returns ShouldTransmit, mark the connection dirty. Stopping early is how quinn’s flow control pushes back on the peer.
Writes: a write that returns Blocked leaves the rest queued on the driver until the Writable event. The stream’s send budget is released only once quinn accepts the bytes.
Closing: conn.close(...) keeps driving the connection until it has drained, then the endpoint removes it.
Incoming connections: accept up to a configurable limit and refuse past it. This replaces with_max_tasks. Also set ServerConfig::max_incoming.
Keep-alive and idle timeout come from TransportConfig::keep_alive_interval and max_idle_timeout. That replaces today’s keep_alive(true).
Inject the socket and clock behind a small trait, so tests can wire two drivers through an in-memory pipe with a fake clock and injected loss or reordering. Being able to test this deterministically is one of the main advantages of a sans-IO library.

Gate:

Deterministic tests pass: handshake, in-order stream delivery under loss, datagrams, close and drain, idle timeout, refusal past the limit.
Single-connection bulk throughput on loopback comes close to quinn’s own perf tool on the same machine. A large gap means the I/O loop is the problem, not quinn-proto.
Phase C: Bevy integration
QuicDriverPlugin replaces QuicAsyncPlugin and TokioRuntime. It configures the driver count, thread names and an on_thread_start hook, so the Phase 4 pinning setup still works.
Two systems:
poll_driver_events in IoSystems::Poll drains the event channels and updates components through a (driver, conn, stream) → Entity map.
flush_driver_commands in IoSystems::Flush wakes each driver that has pending commands. This is the only per-frame work: no pass over every stream, which is what made Phase 3 slower.
Components keep their names and methods. Internally they hold IDs, a command sender and mirrored state.
Opening and accepting streams still produce attempt components, but events resolve them instead of polled JoinHandles. That lets you delete TaskResult and the oneshot machinery.
Keep the behaviour from your CHANGELOG:
recv_many is non-blocking.
A full receive buffer applies backpressure instead of dropping data.
close sends queued data, then finishes.
Dropping a component finishes the stream.
flush becomes a deprecated no-op, because quinn-proto sends as fast as congestion control allows and has no flush.
Add connection stats to QuicConnection, mirrored once per frame: RTT, congestion window, packets lost and current MTU. These come from rtt(), stats() and current_mtu(), and they’re the introspection that motivated the switch.
Tests: port tests/loopback.rs, and add a Bevy-level test with MinimalPlugins.
Gate: all tests pass, and the benchmark reports 0 lost bytes.
Phase D: aeronet over datagrams
Put Session on the connection entity.
Each Session::send packet goes out with datagrams().send(bytes, true), where true drops the oldest queued datagram when the buffer is full. Count those drops and expose the count.
datagrams().recv() fills Session::recv.
MTU: set the session’s min_mtu from datagrams().max_size() at the QUIC minimum path MTU. Call set_mtu as path MTU discovery raises it.
Configuration: enable datagrams in TransportConfig with datagram_receive_buffer_size and datagram_send_buffer_size.
Stream mode: keep it as an option, with length-prefixed framing. That fixes today’s problem where receive chunk boundaries needn’t match the packets that were sent.
Gate: the aeronet example works, and the benchmark’s datagram scenario runs.
Phase E: benchmark and decide
Port quic_concurrency_stress with the same flags, and add:
a server-broadcast scenario
one-way latency p50/p99, using a timestamp in the payload
a datagram scenario
a per-driver wake counter (it replaces remote_schedule_count)
Gates:
Run on the same machine as perf-baseline.md, with the perf branch’s s2n backend run alongside for comparison.
The data-loss reproducer still loses 0 bytes.
At 600 active, frame avg and CPU are at or below Phase 2. I’d expect frame avg to drop, since wakes go from about 600 per frame to a handful.
Bulk received in the 64 KB, 600-client run is at least s2n’s (3,130 MB in 5 s), or the gap is written down.
Broadcast and latency numbers are recorded for both backends.
Phase F: remove s2n and release
Dependencies: remove s2n-quic, s2n-quic-tls, tokio and futures, and add a feature for the TLS provider.
Release: write a migration guide, and update the CHANGELOG and README under the new crate name.
CI: add cargo test, since CI only runs clippy today. Add a short benchmark smoke run (--clients 50 --duration 3) that fails on lost bytes.
Phase G (only if Phase E shows one server thread saturating)
Option 1: the endpoint stays on one driver and its connections are spread across the others, exchanging connection and endpoint events over channels. This is what quinn’s own async crate does.
Option 2: use SO_REUSEPORT with several sockets, each with its own endpoint on its own driver. It’s simpler, but connection migration breaks it, and its behaviour is Linux-specific.
Risks
Bulk throughput may trail s2n until GSO, GRO and pacing are right. The Phase B comparison with quinn’s perf tool catches this early.
Mirrored state is up to one frame stale. is_open() reflects the last event poll, so document that.
quinn-proto is pre-1.0. Pin it, and keep all quinn calls inside src/driver/ so a 0.12 upgrade touches one module.
