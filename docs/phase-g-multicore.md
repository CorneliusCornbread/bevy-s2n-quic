# Phase G: multi-core server

Design notes for Phase G of the [quinn-proto migration](quinn-proto-migration.md).
Phase E triggered it: the server's driver thread is at 100% in every
64 KB × 600 bulk run (see
[`perf-baseline.md`](perf-baseline.md#phase-e-quinn-proto-driver)).

**Decision:** Option 1, the endpoint stays on one driver and its connections
are spread over all of them. It's implemented, and the gate is met (see
[Outcome](#outcome)).

## What's being fixed

One thread runs the server's socket, its `quinn_proto::Endpoint` and all 600
connections. The rest of the runtime is idle: with 4 threads, the other
drivers sit at about 40%. s2n spread the work over all its workers and got
5,145 MB in 5 s. quinn-proto got 3,697 MB with 4 threads.

The Phase E instrumentation shows where the server thread's time goes:

- about 4.3 s of 9.8 s busy in `Endpoint::handle` plus
  `Connection::handle_event`
- 2.7 s in `poll_transmit` and sends

In quinn-proto, `Endpoint::handle` only decodes part of the header and looks
up the connection ID. Decryption and frame handling happen in
`Connection::handle_event`, and encryption in `poll_transmit`. So most of the
cost is per-connection work, which can move to other threads. Only the routing
has to stay with the endpoint. That's inferred from quinn-proto's design, not
measured, so the first implementation step measures it.

## Options

### Option 1: endpoint on one driver, connections spread over all of them

This is how quinn's async crate works.

- The endpoint's home driver keeps the socket and receives on it.
- It calls `endpoint.handle()` and `endpoint.accept()`, then hands each new
  `Connection` to a worker driver. `Connection` is `Send`.
- **Home to worker:** routed datagrams, batched into one command per worker
  per receive turn.
- **Worker to home:** endpoint events (`NewIdentifiers`,
  `RetireConnectionId`, `Drained`).
- **Sending:** workers send directly on a shared `Arc` of the socket.
  `UdpSocketState::send` only needs `&self`.

### Option 2: `SO_REUSEPORT`

N sockets on the same port, each with its own endpoint on its own driver, and
the kernel hashes the 4-tuple to pick a socket.

- Nothing crosses threads per packet, and receiving scales too.
- **NAT rebinding and connection migration break it.** A client whose port
  changes hashes to another socket. That endpoint doesn't know the connection
  ID, so the connection dies. Consumer NATs do this routinely, and QUIC is
  meant to survive it.
- The fix is to steer packets by connection ID with a classic BPF program
  (`SO_ATTACH_REUSEPORT_CBPF`) and a custom `ConnectionIdGenerator` that puts
  the socket index in the ID. That works, but it's Linux-only and uses raw
  `setsockopt`.
- **macOS and Windows don't load-balance unicast UDP over reuseport
  sockets,** so those platforms stay single-threaded.

### Option 3: fork-join turns

One `Core` still owns everything.

- Receiving, routing, commands and timers stay on one thread.
- When a turn has a lot of dirty connections, a helper pool drives them in
  parallel: `handle_event`, reads and `poll_transmit`. The turn waits for all
  of them before continuing.
- Below a threshold the turn runs on one thread as today, so light load costs
  no extra wakes.
- Ownership stays simple, and `conns.iter_mut()` already hands out separate
  `&mut Conn`s, so no protocol between threads is needed.

### Option 0: defer

About 500–740 MB/s per server is already 4–6 Gbit/s, and a game server rarely
sustains that. Not chosen, but it means Phase G has to prove it helps rather
than be assumed to.

## Comparison

| | Option 1 (endpoint + spread) | Option 2 (reuseport) | Option 3 (fork-join) |
|---|---|---|---|
| Migration / NAT rebinding | ✅ | ❌, or Linux-only with CID steering | ✅ |
| Linux / macOS / Windows | ✅ / ✅ / ✅ | ✅ / ❌ / ❌ | ✅ / ✅ / ✅ |
| Work that stays on one thread | Receive + routing, running in parallel with the workers | none | Receive + routing + timers, with workers waiting on them each turn |
| Load balancing | Chosen at accept (fewest connections) | Kernel hash, can't rebalance | Re-split every turn |
| Extra wakes under light load | ⚠️ one wake per sleeping worker per receive burst | none | none (threshold) |
| Driver complexity | High: adopt, endpoint-event round trip, close fan-out, shared socket blocking | Low | Medium: pool, a barrier each turn, results collected afterwards |
| Bevy-layer changes | ~none (see below) | One server ↔ N endpoints, shared connection limit, close fan-out | none |
| Threads | Reuses the existing drivers | Reuses the existing drivers | Needs helper threads that fit awkwardly beside the drivers |

## Why Option 1

1. **It keeps QUIC working correctly everywhere.** Routing by connection ID
   is what quinn-proto's `Endpoint` was built for. Option 2 would break
   migration only for users running more than one thread, and silently. On
   macOS and Windows it would give no speedup at all, so a game developed on
   Windows would behave differently on a Linux server.

2. **It splits the work where the cost actually is.** Decryption, frame
   handling and transmit generation move to workers. The home thread keeps
   receiving, header decoding and routing, and it keeps receiving while the
   workers drive connections. Option 3's barrier means those two steps take
   turns. As a rough estimate, with 4 threads Option 3 gets about 70% of
   Option 1's ceiling.

3. **The codebase is already set up for it.**
   - `IdAllocator` is shared by all drivers, so `ConnId` is unique everywhere.
   - `Command::Close { conn }` is keyed by `ConnId`, so close fan-out uses
     commands that already exist.
   - The router attaches a connection to whichever driver's channel delivered
     `ConnectionAccepted`. If the adopting worker emits that event, the Bevy
     layer needs no changes. Option 2, the "simple" one, is the one that
     changes the Bevy layer.

4. **The tricky protocol is already proven.** quinn's async crate does the
   same split, including the endpoint-event round trip, so the edge cases are
   known.

5. **It can be extended later.** If one receiving socket ever becomes the
   limit, reuseport with CID steering can go on top: each socket becomes a
   home, and its connections still spread over the drivers.

**The main weakness is extra wakes under light load.** A packet already wakes
the server driver through socket readiness. With Option 1, a sleeping worker
is also woken by the home driver. With real clients, whose packets aren't
synchronised like the benchmark's, that's roughly twice the wakeups.

- **Mitigation:** the home driver batches each worker's datagrams into one
  command per receive turn, so a worker is woken at most once per turn.
- **Check:** the 600-active CPU numbers (0.49–0.50 cores) are part of the
  gate. If they get noticeably worse, Option 3's threshold idea can be added
  to Option 1: keep connections on the home driver until it gets busy.

## Implementation steps

**G0. Measure before building.** Time `Endpoint::handle` separately from
`Connection::handle_event`. That split sets the home thread's ceiling. If
`Endpoint::handle` is most of the cost, reconsider.

Result: about 0.1 µs per datagram in `Endpoint::handle` against 1.05 µs in
`Connection::handle_event`, so routing is under 10% of the cost.

**G1. Driver protocol (`src/driver/core.rs`, `protocol.rs`)**

- An endpoint maps each connection handle to a local connection or to a
  remote one on another driver.
- A connection's endpoint is either local or remote (home driver, endpoint,
  handle, shared socket).
- New driver-to-driver messages: adopt a connection, routed connection
  events, endpoint events back to the home, and socket-writable.
- Each driver gets the other drivers' handles once they've all spawned.
- Messages from the home to a worker go through one ordered channel, so
  `Adopt` always comes before that connection's first events.
- On a worker, `Drained` is sent to the home and the connection is removed
  locally. The home sends `EndpointClosed` once every handle has drained.
- Connections placed on their own endpoint's driver keep the direct code
  path. Clients and 1-thread runtimes are unchanged.

**G2. Shared socket and blocking**

- Workers send on a shared socket. As built, each socket has a second
  handle (`try_clone`) for sending, shared by every driver that has one of
  its connections. It keeps the socket open until the last of them has
  drained.
- A worker that hits `WouldBlock` keeps the transmit, stops sending on that
  socket, and asks the home to tell it when the socket is writable
  (`WatchWritable`). The home turns on writable interest, and on the next
  writable event sends `Writable` to every driver watching. Transmits are
  never dropped, and the socket is only registered with the home's poll,
  which avoids registering it in every worker's, shaky on Windows.

**G3. Placement**

- Each new connection goes to the driver with the fewest connections,
  counting client connections and ones handed over but not yet taken.
- With 3 or more drivers the home driver is left out; with 2 it's included.
- `QuicDriverPlugin::with_connection_spreading(false)` turns it off, and the
  benchmark's `--no-spread` uses that for comparisons.

**G4. Tests and gate**

- Sim tests with a server spread over several drivers: handshake,
  connection-ID rotation, close and drain, endpoint close with remote
  connections, refusal past the limit, a blocked socket while a worker is
  sending, and migration (the client's port changes mid-transfer).
- Gate:
  - the reproducer loses 0 bytes
  - bulk receives at least s2n's 1,029 MB/s with 4 threads
  - 600-active CPU and frame avg are no worse than Phase E

## Risks

- **Handshake crypto stays on the home driver,** because `Endpoint::accept`
  processes the ClientHello. A burst of new connections serialises there, as
  it does in quinn.
- **Events for one connection come from a different driver than its
  server's.** The router already handles events from several channels, but
  tests should cover `EndpointClosed` and `ConnectionAccepted` arriving in
  the same frame.
- **One busy connection still caps at about 757 MiB/s.** No option changes
  that.

## Outcome

Numbers are in
[`perf-baseline.md`](perf-baseline.md#phase-g-multi-core-server). Each was
measured with and without spreading in the same session.

- 600 × 64 KB bulk, 4 threads, one process: 1,187 MB/s, against 664
  without spreading and s2n's 1,029. Every driver is at 100%.
- The same with the server in its own process on 4 physical cores:
  1,979 MB/s against 692. The server's Bevy main thread is now the limit:
  it handles every received chunk, and its frames stretch to 119 ms.
- 600 active clients (default 2 threads): 0.44 cores and 0.44 ms frame avg,
  against 0.47 and 0.45 without spreading. The extra wakes didn't show up.
- Reproducer: 0 bytes lost. One connection: 754.5 MiB/s, unchanged.

Tests, all in `src/driver/tests.rs` on the simulated network:

- placement with 2 and 3 drivers, and with spreading off
- data on spread connections under loss and reordering
- the connection-ID round trip through the home's endpoint
- closing the endpoint with remote connections, including removing the
  workers' stand-ins
- the connection limit counting remote connections, and freeing up once one
  drains
- a blocked socket while a worker sends
- the client's NAT rebinding mid-transfer

Not done:

- A test with `EndpointClosed` and `ConnectionAccepted` arriving in the same
  frame.
- Weighting the home's routing work in placement with 2 drivers. With 2,
  both drivers ran at about 100% in bulk, so nothing showed it's needed.
