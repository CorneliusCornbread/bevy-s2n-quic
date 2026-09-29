# Orchestrator performance baseline

Baseline of the current (spin-polling) async orchestrator, measured with
`examples/quic_concurrency_stress.rs`. Later orchestrator changes should be
compared against these numbers on the same machine.

## How to reproduce

```sh
cargo run --release --example quic_concurrency_stress -- \
    --clients 100,300,600 --duration 10 --out results.md
```

Defaults: headless (`MinimalPlugins`, 60 Hz `ScheduleRunnerPlugin`), scenarios
`active,idle`, 2 s warmup, 10 s measure, ≤5 s drain, idle fraction 0.1, and
Tokio workers at the library default (25% of logical CPUs, minimum 2). See
`--help` for all options. The process exits non-zero if any phase loses data
or doesn't get all of its streams open, so CI can use it.

### Columns

| Column                | Meaning                                                                               |
| --------------------- | ------------------------------------------------------------------------------------- |
| ready                 | Client streams open at the start of the phase (should equal clients)                  |
| frame avg / p99 / max | Main schedule work time,`First` → `Last` (excludes the runner's sleep)           |
| interval p99          | Wall-clock frame interval (`Time::delta`)                                           |
| cpu cores             | Process CPU time (user + sys,`getrusage`) ÷ wall time                              |
| tokio busy (worker-s) | Sum of`worker_total_busy_duration` over all workers during the window               |
| tokio busy %          | Busy time ÷ (workers × wall time)                                                   |
| max gq                | Max`global_queue_depth`, sampled once per frame                                     |
| sent / received       | Bytes sent/received**during the measure window** (can differ by in-flight data) |
| send full             | `QuicSendStream::send` calls rejected because the channel was full                  |
| lost B                | Cumulative bytes sent − received after the drain. Must be 0                          |

## Baseline: commit `cb9a574` (main)

- Machine: AMD Ryzen 7 5700X3D (8C/16T), Linux 7.2.7 (Fedora 44), rustc 1.98.1, `--release`
- Headless, 4 Tokio workers, payload 36 B (default) per sender per fixed tick (64 Hz)
- Lockfile pinned to `s2n-quic 1.83.0` (see "Known issues")
- 3 runs. The table shows run 2; runs 1 and 3 are within ±0.02 ms frame avg and ±0.01 CPU cores.

| clients | scenario | senders | ready | frames | frame avg ms | frame p99 ms | frame max ms | interval p99 ms | cpu cores | tokio busy (worker-s) | tokio busy % | max gq |     sent | received | send full | lost B | ok |
| ------: | -------- | ------: | ----: | -----: | -----------: | -----------: | -----------: | --------------: | --------: | --------------------: | -----------: | -----: | -------: | -------: | --------: | -----: | -- |
|     100 | active   |     100 |   100 |    598 |         0.12 |         0.19 |         0.57 |           16.74 |      3.97 |                 39.18 |           98 |      0 |  2.20 MB |  2.20 MB |         0 |      0 | ✓ |
|     100 | idle     |      10 |   100 |    598 |         0.11 |         0.21 |         0.93 |           16.73 |      3.98 |                 39.20 |           98 |      0 | 225.0 KB | 225.0 KB |         0 |      0 | ✓ |
|     300 | active   |     300 |   300 |    598 |         0.18 |         0.31 |         1.08 |           16.73 |      3.93 |                 39.59 |           99 |      2 |  6.59 MB |  6.92 MB |         0 |      0 | ✓ |
|     300 | idle     |      30 |   300 |    598 |         0.14 |         0.28 |         0.73 |           16.74 |      3.98 |                 39.66 |           99 |      0 | 675.0 KB | 675.0 KB |         0 |      0 | ✓ |
|     600 | active   |     600 |   600 |    598 |         0.24 |         0.50 |         0.82 |           16.74 |      3.86 |                 39.63 |           99 |      2 | 13.18 MB | 13.94 MB |         0 |      0 | ✓ |
|     600 | idle     |      60 |   600 |    598 |         0.16 |         0.27 |         0.75 |           16.74 |      3.98 |                 39.79 |           99 |      0 |  1.32 MB |  1.32 MB |         0 |      0 | ✓ |

### Observations

- **Spin cost dominates.** Every Tokio worker is ~98–99% busy and the
  process uses ~4 cores, whatever the load: the same at 100 clients as at 600,
  and the same with 10% of streams sending as with all of them. Even a smoke
  run with 10 clients showed 87% busy and 3.99 cores. CPU doesn't scale
  with work, which is the behavior the idle scenario is meant to expose.
- **Bevy frame cost is small.** Frame work stays under 0.5 ms p99 at 600
  clients, so frame time alone would make this baseline look fine.
- **No byte loss** after the drain in any of the 3 runs at the default
  36 B payload. The transport never pushes back at this rate, so these runs
  don't exercise the loss path (see "Backpressure and data loss" below).
- **Latency backlog.** In the active 300/600 phases the window receives
  ~0.35/0.77 MB more than it sends. That's warmup data still in flight
  at window start, about 0.5 s of end-to-end backlog at these rates.
- **Intermittent ramp failures.** In 1 of 3 runs, 7 of 600 connections were
  closed by the server with `application::Error(503)` (the orchestrator's
  "service unavailable" code) while the clients were ramping up. That run
  reported `ready 593/600` and failed. The likely cause is the orchestrator
  input channel (128 slots) filling during the connection burst while
  every worker is spinning.

## Phase 1: non-blocking stream API

This is the reference baseline for orchestrator changes.

Phase 1 removed every `blocking_*` call from the Bevy-facing stream API
(`recv_many`, `close`, `flush`, `stop_send`, `log_outstanding_errors`). The
benchmark's `server_recv` now uses `recv_many` again, which is also the path
the aeronet session system (`aeronet_session_recv`) takes.

- Same machine and settings as above, default command
- `pre` = `07152f9` (s2n-quic 1.89, no Phase 1). `post` = Phase 1 on top of it
- 3 runs of each, run alternately (pre, post, pre, …). The table shows post run 2.

| clients | scenario | senders | ready | frames | frame avg ms | frame p99 ms | frame max ms | interval p99 ms | cpu cores | tokio busy (worker-s) | tokio busy % | max gq |     sent | received | send full | lost B | ok |
| ------: | -------- | ------: | ----: | -----: | -----------: | -----------: | -----------: | --------------: | --------: | --------------------: | -----------: | -----: | -------: | -------: | --------: | -----: | -- |
|     100 | active   |     100 |   100 |    598 |         0.14 |         0.39 |         0.62 |           16.74 |      3.95 |                 39.20 |           98 |      0 |  2.20 MB |  2.20 MB |         0 |      0 | ✓ |
|     100 | idle     |      10 |   100 |    598 |         0.12 |         0.37 |         0.98 |           16.74 |      3.97 |                 39.22 |           98 |      0 | 225.0 KB | 225.4 KB |         0 |      0 | ✓ |
|     300 | active   |     300 |   300 |    598 |         0.19 |         0.34 |         1.03 |           16.75 |      3.94 |                 39.61 |           99 |      2 |  6.59 MB |  6.92 MB |         0 |      0 | ✓ |
|     300 | idle     |      30 |   300 |    598 |         0.15 |         0.41 |         1.53 |           16.74 |      3.97 |                 39.67 |           99 |      0 | 675.0 KB | 675.0 KB |         0 |      0 | ✓ |
|     600 | active   |     600 |   600 |    598 |         0.26 |         0.48 |         1.48 |           16.75 |      3.86 |                 39.63 |           99 |      2 | 13.18 MB | 13.93 MB |         0 |      0 | ✓ |
|     600 | idle     |      60 |   600 |    598 |         0.17 |         0.35 |         0.60 |           16.76 |      3.98 |                 39.80 |          100 |      0 |  1.32 MB |  1.32 MB |         0 |      0 | ✓ |

Frame avg in ms, min–max over 3 runs:

| clients | scenario | pre        | post       |
| ------: | -------- | ---------- | ---------- |
|     100 | active   | 0.13       | 0.14–0.15 |
|     100 | idle     | 0.11–0.12 | 0.12       |
|     300 | active   | 0.18–0.19 | 0.19       |
|     300 | idle     | 0.14–0.15 | 0.14–0.15 |
|     600 | active   | 0.24–0.25 | 0.25–0.26 |
|     600 | idle     | 0.17       | 0.17       |

### Observations

- **No measurable change**, as expected. The Phase 0 benchmark already
  avoided the blocking call by using `recv()`, so its frame never waited on a
  stream. The gain goes to real users of `recv_many`, most importantly
  `aeronet_session_recv`, which used to stall the frame on the quietest session.
- Active phases are ~0.01 ms slower on frame avg. That's within noise of the
  `recv` → `recv_many` + drain switch in the benchmark itself.
- CPU (~3.85–3.99 cores) and Tokio busy (98–100%) match `cb9a574`/`07152f9`.
  The spin cost is unchanged and is still the orchestrator's to fix. The
  s2n-quic 1.83 → 1.89 bump in `07152f9` made no difference either.
- 0 lost bytes and every stream ready in all 6 runs.

## Backpressure and data loss

`--payload <BYTES>` raises the offered load until s2n-quic can't keep up.
Same machine and commit, `--scenarios active`:

| command                                                              |   offered | sent (API) | received |      lost B after drain | send full |
| -------------------------------------------------------------------- | --------: | ---------: | -------: | ----------------------: | --------: |
| `--clients 100 --payload 16384 --duration 5`                       | ~100 MB/s |     500 MB |   500 MB |                       0 |         0 |
| `--clients 100 --payload 65536 --duration 5`                       | ~400 MB/s |    2000 MB |  2003 MB |                       0 |         0 |
| `--clients 600 --payload 65536 --duration 5` (5 s drain)           | ~2.4 GB/s |   12000 MB |  3130 MB |          10 949 150 478 |         0 |
| `--clients 600 --payload 65536 --warmup 1 --duration 2 --drain 90` | ~2.4 GB/s |    4800 MB |   572 MB | **3 427 729 408** |         0 |

The last row is the reproducer. The drain ran the full 90 s with sending
stopped, yet 3.4 GB never arrived. Peak RSS during the run was 2.1 GB, so the
missing bytes can't all be buffered in the process: they were dropped.
`send full` stays 0 throughout, so `QuicSendStream::send` never reports
backpressure; the outbound channel keeps being emptied faster than the
network can send.

This matches the `SendTask` send_buf bug. The worker polls each task with
`now_or_never()`. When `send_vectored` returns `Pending`, the future is
dropped after `recv_many` has already moved chunks out of the channel into
`send_buf`. The next poll sends only `send_buf[..count]` (the new chunks)
and then clears the buffer. I inferred this from the code; it hasn't been
instrumented.

Use the reproducer as the correctness gate for orchestrator changes:
`lost B` must be 0 (the drain may need to be longer), and `send full`
should become non-zero once backpressure reaches the caller.

## Known issues found while benchmarking

- ~~`QuicReceiveStream::recv_many` uses `blocking_recv_many` and **blocks the
  Bevy thread** until data arrives on that stream.~~ Fixed in Phase 1:
  `recv_many` is now a non-blocking `try_recv` loop.
