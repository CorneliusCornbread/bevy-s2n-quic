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

## Phase 2: long-lived tasks

The orchestrator is replaced by one Tokio task per connection and stream
(`TaskSpawner`, no task limit). Each task is a `run()` loop that is never
dropped mid-await.

- Same machine and settings as above, default command
- Compared against the Phase 1 `post` runs
- 3 runs. The table shows run 2.

| clients | scenario | senders | ready | frames | frame avg ms | frame p99 ms | frame max ms | interval p99 ms | cpu cores | tokio busy (worker-s) | tokio busy % | max gq | sent | received | send full | lost B | ok |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 100 | active | 100 | 100 | 599 | 0.15 | 0.35 | 0.53 | 16.74 | 0.15 | 1.47 | 4 | 49 | 2.20 MB | 2.20 MB | 0 | 0 | ✓ |
| 100 | idle | 10 | 100 | 598 | 0.12 | 0.22 | 0.31 | 16.74 | 0.04 | 0.26 | 1 | 7 | 225.4 KB | 225.0 KB | 0 | 0 | ✓ |
| 300 | active | 300 | 300 | 598 | 0.21 | 0.40 | 0.79 | 16.74 | 0.39 | 3.94 | 10 | 200 | 6.59 MB | 6.92 MB | 0 | 0 | ✓ |
| 300 | idle | 30 | 300 | 599 | 0.14 | 0.24 | 0.68 | 16.73 | 0.07 | 0.54 | 1 | 11 | 676.1 KB | 676.1 KB | 0 | 0 | ✓ |
| 600 | active | 600 | 600 | 598 | 0.31 | 0.65 | 1.18 | 16.74 | 0.78 | 8.26 | 21 | 499 | 13.18 MB | 13.95 MB | 0 | 0 | ✓ |
| 600 | idle | 60 | 600 | 598 | 0.18 | 0.36 | 0.56 | 16.73 | 0.11 | 0.94 | 2 | 21 | 1.32 MB | 1.32 MB | 0 | 0 | ✓ |

Min–max over 3 runs. Phase 1 frame avg is the `post` range above; Phase 1 CPU is `post` run 2.

| clients | scenario | frame avg ms (P1) | frame avg ms (P2) | frame p99 ms (P2) | cpu cores (P1) | cpu cores (P2) | tokio busy % (P2) |
| ------: | -------- | ----------------- | ----------------- | ----------------- | -------------: | -------------: | ----------------: |
|     100 | active   | 0.14–0.15         | 0.14–0.15         | 0.35–0.42         |           3.95 |           0.15 |                 4 |
|     100 | idle     | 0.12              | 0.12              | 0.19–0.22         |           3.97 |           0.04 |                 1 |
|     300 | active   | 0.19              | 0.21–0.22         | 0.40–0.52         |           3.94 |           0.39 |                10 |
|     300 | idle     | 0.14–0.15         | 0.14–0.15         | 0.24–0.27         |           3.97 |           0.07 |                 1 |
|     600 | active   | 0.25–0.26         | 0.31–0.32         | 0.65–0.95         |           3.86 |           0.78 |             20–21 |
|     600 | idle     | 0.17              | 0.18              | 0.33–0.36         |           3.98 |           0.11 |                 2 |

### Remote wakes

Built with `RUSTFLAGS="--cfg tokio_unstable"` (the benchmark then also
reports Tokio's `remote_schedule_count` during the measure window). One run:

| clients | scenario | remote schedules | per frame |
| ------: | -------- | ---------------: | --------: |
|     100 | active   |           60 225 |       101 |
|     100 | idle     |            6 079 |        10 |
|     300 | active   |          180 653 |       302 |
|     300 | idle     |           18 109 |        30 |
|     600 | active   |          361 855 |       604 |
|     600 | idle     |           36 184 |        61 |

```sh
RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=target/unstable \
    cargo run --release --example quic_concurrency_stress -- --clients 100,300,600
```

### Observations

- **CPU is fixed.** 5–100× less CPU, and it now scales with work: 0.04
  cores for 100 mostly idle connections, 0.78 cores for 600 busy ones.
- **Idle frame time is unchanged**, within ±0.01 ms.
- **Busy frame time is worse at scale:** +0.02–0.03 ms at 300 active
  senders and +0.06 ms (~23%) at 600. p99 went from ~0.5 ms to 0.65–0.95 ms
  at 600. It's small in absolute terms but consistent across runs.
- **The cost is remote wakes.** Remote schedules are exactly one per sending
  stream per frame. The Bevy thread's first `QuicSendStream::send` each frame
  wakes that stream's parked send task, which goes through Tokio's global
  inject queue and may unpark a worker. The extra frame time scales the same
  way (~100 ns per wake), and max global queue depth went from ~2 to ~500 at
  600 senders. The old workers never parked, so their wakes were almost free.
  This is inferred from the correlation. It hasn't been profiled.
- **Gate result:** the busy-scenario regression is remote wakes, so per the
  plan this goes to Phase 3 before deciding on sharding.
- All streams were ready in all 4 runs (including the `tokio_unstable` run),
  with 0 lost bytes. With no task limit, the ramp-up `503` failures seen
  on `cb9a574` can't occur.

### Data-loss reproducer

`--clients 600 --payload 65536 --warmup 1 --duration 2 --drain 90 --scenarios active`:

| build      | sent (API) | received in window | lost B after drain | send full | peak RSS |
| ---------- | ---------: | -----------------: | -----------------: | --------: | -------: |
| `cb9a574`  |    4800 MB |             572 MB |  **3 427 729 408** |         0 |   2.1 GB |
| Phase 2    |    4838 MB |             770 MB |              **0** |         0 |   2.4 GB |

The drain finished in well under the 90 s. `send full` stays 0 because each
sender only queues ~128 chunks in the 2 s window, below the 512-slot
outbound channel. That backlog is the extra RSS. `tests/loopback.rs` covers
the case where backpressure reaches `send` (it fails if the receiver drops
data instead of pushing back).

## Phase 3: per-frame batching (rejected, not merged)

Experiment: `QuicSendStream::send` queued on the component, and a
`flush_outbound` system (in `Last`) handed each stream's batch to its task
as one channel message. The receive task handed packets to Bevy as one batch
per read. It gave no gains (see the gate result below), so it was reverted
and the code stays as in Phase 2. The numbers are kept as a record.

- Same machine and settings as above
- Phase 2 (`46517f9`) and Phase 3 were run alternately with the same benchmark
  binary source (the benchmark gained `--burst <N>`, messages per sender
  per fixed tick). 3 runs each at `--burst 1`, 2 runs each at `--burst 8`.
- 0 lost bytes and all streams ready in every run of both builds.

Min–max over the runs, `--burst 1` (the default command):

| clients | scenario | frame avg ms (P2) | frame avg ms (P3) | frame p99 ms (P2) | frame p99 ms (P3) | cpu cores (P2) | cpu cores (P3) | max gq (P2) | max gq (P3) |
| ------: | -------- | ----------------- | ----------------- | ----------------- | ----------------- | -------------- | -------------- | ----------: | ----------: |
|     100 | active   | 0.14              | 0.15              | 0.22–0.42         | 0.21–0.23         | 0.15           | 0.15           |       45–48 |      99–100 |
|     100 | idle     | 0.11–0.12         | 0.12              | 0.17–0.20         | 0.16–0.18         | 0.04           | 0.04           |         2–3 |          10 |
|     300 | active   | 0.20–0.21         | 0.24              | 0.32–0.38         | 0.33–0.35         | 0.38–0.39      | 0.38           |     205–232 |     264–300 |
|     300 | idle     | 0.14              | 0.15              | 0.21–0.28         | 0.20–0.25         | 0.06–0.07      | 0.06–0.07      |       10–11 |          30 |
|     600 | active   | 0.29–0.30         | 0.36–0.37         | 0.45–0.60         | 0.55–0.58         | 0.75–0.77      | 0.77–0.78      |     500–516 |     546–562 |
|     600 | idle     | 0.17–0.18         | 0.19              | 0.32–0.39         | 0.27–0.35         | 0.11           | 0.10–0.11      |       21–24 |       59–60 |

`--burst 8`, 600 clients active: frame avg 0.41 ms (P2) vs 0.43–0.44 ms (P3),
CPU 0.81–0.82 vs 0.82–0.83 cores. Other rows match within the same margins.

Remote schedules (`tokio_unstable`, one run each):

| clients | scenario | P2 burst 1 | P3 burst 1 | P2 burst 8 | P3 burst 8 |
| ------: | -------- | ---------: | ---------: | ---------: | ---------: |
|     100 | active   |     60 057 |     59 800 |     60 337 |     59 800 |
|     300 | active   |    180 863 |    179 923 |    181 335 |    179 623 |
|     600 | active   |    361 302 |    359 315 |    362 902 |    359 316 |
|     600 | idle     |     36 229 |     35 880 |     36 371 |     35 940 |

### Data-loss reproducer

Same command as in Phase 2:

| build   | sent (API) | received in window | lost B after drain | send full | peak RSS |
| ------- | ---------: | -----------------: | -----------------: | --------: | -------: |
| Phase 2 |    4838 MB |             770 MB |                  0 |         0 |   2.4 GB |
| Phase 3 |    2416 MB |             780 MB |                  0 |    38 145 |   2.4 GB |

### Observations

- **Batching didn't reduce wakes.** Phase 2 already made exactly one remote
  wake per sending stream per frame, even at 8 sends per frame. Once a task
  is scheduled, more sends before it runs don't schedule it again, so Tokio
  was already coalescing them. Phase 3 removes only the ~0.5% of wakes where
  a task ran between two sends in the same frame.
- **Busy frame time got worse**, not better: +0.03 ms at 300 active and
  +0.06–0.07 ms (~22%) at 600, consistent across runs. Idle rows are +0.01 ms.
  Likely causes are the extra `flush_outbound` pass over every send stream
  and one `Vec` allocation per batch. This hasn't been profiled.
- **CPU is unchanged** within noise.
- Max global queue depth now equals the number of senders. All wakes happen
  in one burst in `flush_outbound` instead of spread across `Update`. It
  doesn't show up in CPU or frame p99.
- **Backpressure now reaches the caller.** With the channel limited to 8
  batches, `send` returns `Full` under overload (`send full` 38 145 in the
  reproducer) and only half as much data is accepted, with the same
  throughput. Peak RSS is unchanged, so the backlog seems to sit in s2n-quic
  rather than in the channel.
- **Gate result:** per-frame batching doesn't remove the remote-wake cost,
  and the batching pass adds its own, so it was reverted. The wake cost is
  inherent to waking Tokio tasks from a non-Tokio thread, so it needs
  sharding (fewer tasks to wake) or a different handoff, not batching. The
  backpressure gain came from the smaller outbound channel, not from
  batching, and can be revisited on its own.

## Phase 4: runtime configuration and thread placement

`QuicAsyncPlugin::with_runtime_builder` gives access to the Tokio `Builder`
(worker count, `thread_name`, `on_thread_start`, ...). The benchmark now:

- sizes Bevy's pools and Tokio's workers to fit the CPUs it may use: main +
  1 IO + 1 async compute + up to 4 compute threads, plus the Tokio workers.
  It used to create 4 IO + 4 async compute + 4 compute threads plus main and
  4 workers (17 threads on 16 CPUs), though the IO and async pools sit idle.
- with `--pin`, pins the Tokio workers (through `on_thread_start`) and the
  Bevy threads (through `on_thread_spawn` and the main thread) to separate
  physical cores. Tokio gets whole cores from the end of the allowed set,
  so on this machine `6-7,14-15` and Bevy `0-5,8-13`.
- with `--tokio-nice <N>`, sets the Tokio workers' nice value. That isn't
  benchmarked below, because there's no contention to arbitrate at these loads.
- with `--role server|clients`, runs the two ends as separate processes,
  linked by a TCP control connection. The clients drive the phases, and the
  server reports its received byte count back, so `lost B` still works.

Same machine as above, s2n-quic 1.90, default command unless noted, 0 lost
bytes and all streams ready in every run. `old` is the pre-Phase-4 benchmark
(`5d67ba9`) built from the same lockfile. Runs were interleaved (old, new,
pin, split, ...).

### Single process: sizing and pinning

Min–max over 3 runs each. `p99 spread` is the max − min of frame p99 over
the runs.

| clients | scenario | frame avg ms (old / new / pin) | frame p99 ms (old / new / pin) | p99 spread ms (old / new / pin) | cpu cores (old / new / pin) |
| ------: | -------- | ------------------------------ | ------------------------------ | ------------------------------- | --------------------------- |
|     100 | active   | 0.14–0.16 / 0.14–0.17 / 0.13–0.14 | 0.26–0.46 / 0.24–0.57 / 0.20–0.26 | 0.20 / 0.33 / **0.06** | 0.15 / 0.15 / 0.19 |
|     100 | idle     | 0.11–0.14 / 0.11–0.12 / 0.11–0.12 | 0.20–0.29 / 0.18–0.24 / 0.17–0.35 | 0.09 / 0.06 / 0.18 | 0.04 / 0.04 / 0.04 |
|     300 | active   | 0.21–0.24 / 0.21–0.25 / 0.19–0.21 | 0.32–0.78 / 0.31–0.98 / 0.30–0.53 | 0.46 / 0.67 / **0.23** | 0.38–0.42 / 0.39–0.42 / 0.50–0.51 |
|     300 | idle     | 0.14–0.18 / 0.14–0.15 / 0.13–0.14 | 0.26–0.56 / 0.24–0.27 / 0.20–0.41 | 0.30 / 0.03 / 0.21 | 0.07–0.08 / 0.07 / 0.07 |
|     600 | active   | 0.30–0.32 / 0.30–0.34 / 0.28–0.30 | 0.44–0.93 / 0.50–1.06 / 0.46–0.55 | 0.49 / 0.56 / **0.09** | 0.76–0.80 / 0.76–0.81 / 1.03–1.04 |
|     600 | idle     | 0.17–0.19 / 0.17–0.19 / 0.16–0.17 | 0.32–0.38 / 0.29–0.43 / 0.30–0.31 | 0.06 / 0.14 / 0.01 | 0.11 / 0.10–0.11 / 0.12–0.13 |

```sh
cargo run --release --example quic_concurrency_stress -- --pin
```

### Separate server and clients processes

The server (`taskset -c 0-3,8-11`) and the clients (`taskset -c 4-7,12-15`)
run on disjoint physical cores, with 4 Tokio workers each (`--tokio-threads 4`,
so the server's runtime matches the single-process runs). Each process then
sizes itself to its 8 CPUs: Tokio 4 + Bevy 4 (compute 1). Server-process
numbers:

| clients | scenario | frame avg ms (split ×3) | frame p99 ms (split ×3) | cpu cores (split ×3) | frame avg ms (hog ×2) | frame p99 ms (hog ×2) | cpu cores (hog ×2) | frame avg ms (shared ×2) | frame p99 ms (shared ×2) |
| ------: | -------- | ----------------------- | ----------------------- | -------------------- | --------------------- | --------------------- | ------------------ | ------------------------ | ------------------------ |
|     100 | active   | 0.11                    | 0.16–0.20               | 0.09                 | 0.11–0.13             | 0.20–0.49             | 0.11–0.12          | 0.11                     | 0.15–0.28                |
|     100 | idle     | 0.10                    | 0.14                    | 0.02                 | 0.11                  | 0.17–0.22             | 0.02               | 0.10                     | 0.14–0.18                |
|     300 | active   | 0.14–0.15               | 0.21–0.25               | 0.25                 | 0.14–0.15             | 0.25–0.58             | 0.29–0.31          | 0.14–0.15                | 0.19–0.24                |
|     300 | idle     | 0.11–0.12               | 0.16–0.18               | 0.03                 | 0.12–0.13             | 0.22–0.29             | 0.04               | 0.12                     | 0.17–0.18                |
|     600 | active   | 0.20–0.21               | 0.30–0.35               | 0.47–0.48            | 0.19–0.20             | 0.31–0.36             | 0.56–0.61          | 0.19–0.21                | 0.29–0.48                |
|     600 | idle     | 0.13–0.14               | 0.20–0.26               | 0.06                 | 0.14–0.15             | 0.37–0.38             | 0.08               | 0.12–0.13                | 0.17–0.20                |

- `split`: as above.
- `hog`: as `split`, plus a busy loop on each of the clients' 8 CPUs, so the
  clients process is starved. Its frame p99 went from 0.2–0.9 ms to 3–8 ms.
- `shared`: no `taskset`. Both processes may use all 16 CPUs.

```sh
taskset -c 0-3,8-11  cargo run --release --example quic_concurrency_stress -- --role server --tokio-threads 4
taskset -c 4-7,12-15 cargo run --release --example quic_concurrency_stress -- --role clients --tokio-threads 4
```

### Observations

- **Thread budgeting alone changes nothing.** `new` matches `old` in frame
  time, CPU and p99 spread. The extra IO and async compute threads were idle,
  so cutting them removed no contention.
- **Pinning stabilizes p99.** In the busy rows the p99 spread across runs
  drops from 0.33–0.67 ms to 0.06–0.23 ms, and frame avg improves by
  0.01–0.04 ms. In the idle rows the spread is small either way
  (0.01–0.21 ms) and doesn't favor pinning.
- **Pinning costs CPU.** Up to 30% more process CPU (1.03 vs 0.78 cores at
  600 active) and Tokio busy % rises from 20% to 28%. The 4 workers share 2
  physical cores, so each runs on half an SMT core. This is inferred and
  hasn't been profiled. Giving Tokio one physical core per worker would leave
  Bevy fewer cores, so it's a trade-off and not a default.
- **The server process is stable on its own.** Split-mode server p99 spread
  is ≤0.06 ms in every row across 3 runs. Its frame avg (0.20–0.21 ms at 600
  active) excludes the client half that the single-process numbers include
  (0.30–0.34 ms).
- **Server frame avg doesn't depend on client load.** With the clients
  process starved, server frame avg is unchanged (±0.01 ms) in every row.
- **Server p99 and CPU do shift a little under client load.** p99 is higher
  in 5 of 6 rows (+0.03–0.3 ms, the largest single runs being 0.49 and
  0.58 ms at 100 and 300 active). Only 600 active is unchanged. CPU is
  +15–30% in every row, with the same bytes received, in both hog batches
  (the earlier batch matched). The cause hasn't been profiled. Candidates are
  lower all-core boost clocks with 8 extra busy cores, and burstier arrivals
  from the starved clients. Process and core separation can't isolate either.
- **Gate result:** partly met. p99 is stable across runs with `--pin` or
  with split processes, but not with thread budgeting alone. Server-process
  frame avg is independent of client load. Server p99 and CPU are mostly
  independent, but rise modestly when the client side saturates its cores.
- The first hog batch exposed a race in the control protocol, not in the
  library: the server computed `lost` when `finish` arrived, by which time
  the next phase's first messages (720 B) had landed, giving `lost -720`.
  The clients now compute `lost` from the server's last report and send it
  with `finish`. The tables above are from the fixed build. The
  single-process runs don't use the control link.

## Phase E: quinn-proto driver

The quinn-proto driver (`src/driver/`) against the s2n backend at `7ba4f3e`.
Both builds run the Phase E benchmark, which adds the `broadcast` scenario,
one-way latency, per-driver busy time and the wake counter. On s2n, busy
time is per Tokio worker, and wakes are Tokio's `remote_schedule_count`
(built with `--cfg tokio_unstable`). s2n has no `datagram` scenario.

- Same machine as above. Runs were niced (`nice -n 10`), capped at 6 GB with
  no swap (`systemd-run --user --scope -p MemoryMax=6G -p MemorySwapMax=0`),
  and the desktop was in use.
- s2n's own frame times are about 0.2 ms higher than Phase 2's on this
  machine, so quinn is compared with s2n from the same session, not with the
  Phase 2 table.
- Fixes made during this phase are listed under [Fixes](#fixes). All numbers
  below include them unless marked "before".

### Default command

`--clients 100,300,600 --scenarios active,idle,broadcast,datagram`. Min–max
over 3 quinn runs and 2 s2n runs. s2n ran with `--warmup 12`: its build
still has the one-per-frame acceptor (see [Fixes](#fixes)), and a shorter
warmup leaves seconds of backlog in its latency.

| clients | scenario  | frame avg ms (s2n) | frame avg ms (quinn) | cpu cores (s2n) | cpu cores (quinn) | wakes/frame (s2n) | wakes/frame (quinn) | latency p99 ms (s2n) | latency p99 ms (quinn) |
| ------: | --------- | ------------------ | -------------------- | --------------- | ----------------- | ----------------: | ------------------: | -------------------- | ---------------------- |
|     100 | active    | 0.19–0.20          | 0.19–0.26            | 0.16–0.17       | 0.10–0.12         |               101 |                   2 | 16.73–16.74          | 16.76–17.24            |
|     100 | idle      | 0.16               | 0.15–0.16            | 0.04            | 0.03              |                10 |                   2 | 16.75                | 16.75–16.77            |
|     100 | broadcast | 0.19–0.23          | 0.18–0.22            | 0.19–0.21       | 0.11–0.12         |               107 |                   1 | 16.81–16.86          | 16.80–16.83            |
|     100 | datagram  | –                  | 0.19–0.21            | –               | 0.10–0.11         |                 – |                   2 | –                    | 16.77–16.83            |
|     300 | active    | 0.30–0.65          | 0.30–0.33            | 0.41–0.57       | 0.25–0.27         |               303 |                   2 | 33.31–33.38          | 16.79–16.83            |
|     300 | idle      | 0.22–0.25          | 0.18–0.19            | 0.08            | 0.05              |                31 |                   2 | 16.73–16.74          | 16.74–16.75            |
|     300 | broadcast | 0.31–0.45          | 0.31–0.32            | 0.49–0.59       | 0.29              |               321 |                   1 | 16.88–17.00          | 16.91–16.92            |
|     300 | datagram  | –                  | 0.28–0.32            | –               | 0.25–0.27         |                 – |                   2 | –                    | 16.77–16.80            |
|     600 | active    | 0.52–0.64          | 0.49–0.51            | 0.86–0.94       | 0.49–0.50         |               605 |                   2 | 66.73–66.75          | 16.86–16.90            |
|     600 | idle      | 0.32–0.33          | 0.23–0.27            | 0.12–0.13       | 0.08–0.09         |                62 |                   2 | 16.71                | 16.73–16.80            |
|     600 | broadcast | 0.53–0.62          | 0.48–0.51            | 0.96–1.02       | 0.55–0.56         |               642 |                   1 | 17.04–17.11          | 16.99–17.10            |
|     600 | datagram  | –                  | 0.45–0.47            | –               | 0.48–0.50         |                 – |                   2 | –                    | 16.82–16.85            |

Latency p50 was 16.6–16.9 ms everywhere, on both backends. One-way latency
includes the frames both ends take to hand a message over, so a 60 Hz app
can't get much below one frame (16.7 ms). Every phase lost 0 bytes. Peak RSS
was 160 MB for quinn and 1.0 GB for s2n.

### Bulk: `--clients 600 --payload 65536 --duration 5 --drain 90 --scenarios active`

| build                      | threads | received in 5 s | rx MB/s | busy % per driver | cpu cores | sent (API) | send full | peak RSS |
| -------------------------- | ------: | --------------: | ------: | ----------------- | --------: | ---------: | --------: | -------: |
| s2n                        |       4 |        5 145 MB |   1 029 | 87/87/87/86       |      3.21 |   12000 MB |         0 |   2.5 GB |
| quinn, before (2 runs)     |       2 |               – | 411–420 | 100/50            |      1.40 |          – |         – |        – |
| quinn                      |       2 | 2 533–2 539 MB  | 506–508 | 100/57            |      1.47 | 2221–2260 MB |  ~156 000 |   1.0 GB |
| quinn                      |       4 |        3 697 MB |     739 | 100/40/39/39      |      2.04 |    3376 MB |   137 989 |   1.1 GB |

quinn's server driver is at 100% in every bulk run. With `--tokio-threads 8`
(before the fixes) it was still 100%, against 20% for the others, and
received 582 MB/s. **The gap to s2n is the server's single thread**: s2n
spreads one server's connections over all its workers, while the driver runs
an endpoint and all its connections on one. That is the Phase G trigger.

Where the server driver's time goes, from temporary per-turn instrumentation
(4 threads, one 5 s run, before the GSO fix): about 1.1 µs per received
datagram in `Endpoint::handle`/`Connection::handle_event` (4.3 s for
3.85 M datagrams, out of 9.8 s busy), and 2.7 s in `poll_transmit` and
sends. `perf` can't attach on this machine (`perf_event_paranoid` is 2 and
SELinux blocks it), so there's no symbol-level profile.

Experiments on the server's bottleneck (4 threads, one run each, before the
GSO fix):

| change                                                      | rx MB/s | note                                    |
| ----------------------------------------------------------- | ------: | --------------------------------------- |
| none (`MAX_RECV_DATAGRAMS` 256)                             | 500–550 |                                         |
| `MAX_RECV_DATAGRAMS` 1024                                   |     562 |                                         |
| `MAX_RECV_DATAGRAMS` 4096                                   | 694–748 | kept: fewer turns, fuller send batches |
| `MAX_RECV_DATAGRAMS` 16384 / unlimited                      | 672–675 |                                         |
| ACK-eliciting threshold 9 (ack frequency)                   |     576 | no gain on top of 4096 (730)            |
| aws-lc-rs instead of ring (256 / 4096)                      | 504 / 686 | ring got 510 / 726: crypto isn't the limit |
| `--stream-budget` 2 MiB / 4 MiB (with 4096)                 | 669 / 622 | more latency, no throughput          |
| stream receive window 3.75 MB, s2n's (with 4096)            |     580 | no gain                                 |

Bigger budgets and windows only add queueing: the bottleneck is CPU, not
flow control. The budgets are now configurable anyway, with
`QuicDriverPlugin::with_stream_budgets` and `--stream-budget`.

### Single connection: driver vs quinn

2 GiB on one stream over loopback. Ours is
`cargo test --release --lib driver::tests::bulk_throughput -- --ignored --nocapture`.
quinn's is `bulk --upload-size 2G --download-size 0` from quinn's `bench`
crate at `quinn-proto-0.11.19`.

| build                 |         MiB/s |
| --------------------- | ------------: |
| driver, before        |     44.3–44.6 |
| driver                |           757 |
| quinn `bulk` (2 runs) |       785–812 |

Before the GSO fix, 4.5% of the sender's packets were lost, in bursts of
about one GSO batch, with a congestion event every ~1,000 packets. That
capped cwnd at ~67 KB, although the kernel dropped nothing.

### Single stream through the Bevy API

`--clients 1 --scenarios download,upload --payload 65536 --duration 5
--stream-budget <B> [--hz <N>]`, added after Phase E. One stream is filled
until its send budget is full every tick, and drained every tick. `download`
is server → client, `upload` client → server. One run each, quinn only. The
s2n backend is gone from the tree, and its benchmark port was lost. The
benchmark's "MB" is 2^20 bytes. 0 bytes lost in every run, and peak RSS was
70–100 MB.

| budget  | frames/s | rx/s (download / upload) | frame avg ms | busy % per driver | latency p50 / p99 ms |
| ------: | -------: | ------------------------ | ------------ | ----------------- | -------------------- |
|   1 MiB |       60 | 59.8 / 59.8 MB/s         | 0.16         | 9/8               | 16.7 / 16.8          |
|   4 MiB |       60 | 239.2 / 239.2 MB/s       | 0.21–0.25    | 33/30             | 16.7 / 16.8          |
|  16 MiB |       60 | 762.7 / 750.0 MB/s       | 0.47–0.48    | 99/90             | 16.6 / 33.5          |
|  64 MiB |       60 | 763.4 / 765.4 MB/s       | 0.46–0.49    | 99/90             | 83.5 / 100.4         |
|   1 MiB |      240 | 236.8 / 236.8 MB/s       | 0.15         | 36/31             | 4.2 / 4.3            |
|   4 MiB |      240 | 741.2 / 744.0 MB/s       | 0.22         | 99/89             | 8.4 / 8.5            |

At 60 Hz the fixed tick is Bevy's default 64 Hz; with `--hz` both run at
that rate.

- **Throughput is one budget per frame** until the network thread is the
  limit: 1 MiB × 60 frames/s = 60 MB/s, and 4 MiB gives exactly 4× that.
- **The ceiling is the sender's driver thread at 99%**, at 740–765 MB/s.
  That's the driver's own single-connection speed (757 MiB/s above), so the
  Bevy layer adds no limit beyond the per-frame handover.
- **Budgets past the ceiling only add latency.** 64 MiB moves no more than
  16 MiB, but data waits 5–6 frames (p50 84 ms) in the budget.
- **Frame rate counts as much as budget.** 1 MiB at 240 Hz matches 4 MiB at
  60 Hz, with a quarter of the latency.
- Phase G won't raise this, because quinn-proto runs a connection on one
  thread.

### Data-loss reproducer

`--clients 600 --payload 65536 --warmup 1 --duration 2 --drain 90 --scenarios active`:

| build | sent (API) | received in window | lost B after drain | send full | peak RSS |
| ----- | ---------: | -----------------: | -----------------: | --------: | -------: |
| s2n   |    4838 MB |             730 MB |              **0** |         0 |   2.4 GB |
| quinn |    1114 MB |            1114 MB |              **0** |    58 981 |   203 MB |

The 2 s window favours quinn, and the 5 s bulk run favours s2n: s2n ramps up
more slowly, then scales across its workers. quinn pushes back on `send`
instead of queueing, so its memory stays bounded.

### Fixes

- **GSO sends over 64 KB were lost.** The driver asked `poll_transmit` for
  up to quinn-udp's `max_gso_segments()` (64 on Linux). At 1,200–1,452 B per
  datagram, batches over 45–54 segments are larger than a UDP datagram.
  Linux fails them with `EMSGSIZE`, which the driver logged at `trace`, as it
  does for failed MTU probes. Batches are now capped at 10 segments (what
  quinn's own driver uses) and at 65,507 bytes. The sim socket now
  advertises 64 segments and rejects sends larger than a UDP datagram, and
  `bulk_sends_fit_in_a_udp_datagram` covers it.
- **`MAX_RECV_DATAGRAMS` 256 → 4096**, from the experiments above.
- **The acceptor plugins took one connection per server, and one stream per
  connection, each frame.** At 60 Hz, 600 connections took 10 s to appear on
  the server. Clients' data waited in the driver meanwhile, which showed up
  as multi-second latency p99 in `active` phases that add clients, on both
  backends. It probably also explains why Phase 2's 300- and 600-client
  `active` rows received more than they sent (not checked on that build).
  The acceptors now take everything that's waiting.
  `acceptors_take_everything_waiting_each_frame` covers it.

### Gate

- **Data-loss reproducer loses 0 bytes:** met.
- **600 active, frame avg and CPU at or below Phase 2:** CPU met (0.49–0.50
  cores, against 0.78 in Phase 2 and 0.86–0.94 for s2n today). Frame avg
  isn't met against Phase 2's 0.31–0.32 ms. quinn's 0.49–0.51 ms is at or
  below s2n's 0.52–0.64 ms in the same session, but the expected drop from
  far fewer wakes (2 per frame against 605) didn't show up.
- **Bulk received at least s2n's:** not met. quinn got 2,533–2,539 MB in 5 s
  with 2 threads and 3,697 MB with 4, against 5,145 MB for s2n in the same
  session (3,130 MB in the older baseline). The cause is the server's driver
  at 100%: Phase G.
- **Broadcast and latency recorded for both backends:** met.

## Phase G: multi-core server

A server keeps receiving on its own driver and hands each connection it
accepts to the driver with the fewest connections (see
[`phase-g-multicore.md`](phase-g-multicore.md)). With three or more drivers,
the server's own driver only receives and routes. `--no-spread` (that is,
`QuicDriverPlugin::with_connection_spreading(false)`) keeps the Phase E
behaviour, and each comparison below ran both in the same session. One run
each, with the same machine, nice, memory cap and desktop use as Phase E.

### G0: routing cost

Measured before building, with temporary timers (4 threads, 600 × 64 KB,
5 s). Over 4.69 M datagrams, `Endpoint::handle` took 0.46 s and
`Connection::handle_event` 4.92 s, about 0.1 µs against 1.05 µs per
datagram. So routing, the part that has to stay on the server's driver, is
under 10% of the per-datagram cost.

### Bulk: `--clients 600 --payload 65536 --duration 5 --drain 90 --scenarios active`

| threads | spread | rx MB/s | busy % per driver | cpu cores | frame avg ms | lost B |
| ------: | ------ | ------: | ----------------- | --------: | -----------: | -----: |
|       2 | no     |     520 | 100/74            |      1.60 |         0.90 |      0 |
|       2 | yes    |     608 | 100/99            |      1.83 |         0.94 |      0 |
|       4 | no     |     664 | 100/49/48/48      |      2.23 |         1.13 |      0 |
|       4 | yes    | **1 187** | 100/100/100/100 |      3.67 |         1.99 |      0 |

s2n got 1,029 MB/s with 4 threads (Phase E). In this single process the
clients' connections share the drivers with the server's, so every driver is
now at 100%.

### Separate server and clients processes

The server on `taskset -c 0-3,8-11`, the clients on `4-7,12-15` (4 physical
cores each), both with `--tokio-threads 4`. Same bulk options. Numbers from
the server's table.

| spread | rx MB/s  | busy % per driver | cpu cores | frames | frame avg ms | lost B |
| ------ | -------: | ----------------- | --------: | -----: | -----------: | -----: |
| no     |      692 | 100/0/0/0         |      1.15 |    299 |         4.36 |      0 |
| yes    | **1 979** | 98/95/87/85      |      4.34 |     41 |       119.12 |      0 |

- With spreading, the server's driver no longer limits it: 2.9× as much
  arrives.
- **The server's main thread is the next limit.** It handles every received
  chunk, about 0.45 µs each in the `no` run (4.36 ms per frame for
  11.5 MB). At about 2 GB/s that's around 1.7 M chunks a second, on cores
  whose SMT siblings run the busy drivers. The drivers deliver faster than
  one Bevy frame can take at 60 Hz, so frames stretch until the two balance,
  here at 41 frames in 5 s. In-process, frame time tracked throughput
  (1.13 ms at 664 MB/s, 1.99 ms at 1,187 MB/s). That cost is in the Bevy
  layer and the benchmark's receive system, not the driver.

### Default command

`--clients 100,300,600 --scenarios active,idle,broadcast,datagram`, default
2 threads. Rows at 600 clients:

| scenario  | spread | frame avg ms | cpu cores | busy % per driver | wakes/frame |
| --------- | ------ | -----------: | --------: | ----------------- | ----------: |
| active    | no     |         0.45 |      0.47 | 35/17             |           2 |
| active    | yes    |         0.44 |      0.44 | 27/24             |           2 |
| idle      | no     |         0.22 |      0.07 | 4/2               |           2 |
| idle      | yes    |         0.22 |      0.07 | 3/3               |           2 |
| broadcast | no     |         0.42 |      0.51 | 41/15             |           1 |
| broadcast | yes    |         0.43 |      0.48 | 28/27             |           2 |
| datagram  | no     |         0.42 |      0.46 | 34/17             |           2 |
| datagram  | yes    |         0.40 |      0.42 | 26/23             |           2 |

100 and 300 clients were the same with and without spreading, to within
0.02 ms and 0.02 cores. Latency p50/p99 didn't change (16.6–16.8 /
16.8–17.0 ms). The extra wakes from routing didn't show up in CPU. Broadcast
has 2 wakes per frame with spreading because the server's sends now go to
both drivers.

### Other checks

- **Data-loss reproducer** (`--warmup 1 --duration 2`, 2 threads): 0 bytes
  lost, 640 MB/s, busy 100/99.
- **One connection** (`driver::tests::bulk_throughput`): 754.5 MiB/s, against
  757 in Phase E. Sending on the duplicated socket handle costs nothing.

### Gate

- **Data-loss reproducer loses 0 bytes:** met.
- **Bulk at least s2n's 1,029 MB/s with 4 threads:** met, 1,187 MB/s
  in-process and 1,979 MB/s with separate processes.
- **600 active, CPU and frame avg no worse than Phase E:** met (0.44 cores
  and 0.44 ms, against 0.49–0.50 and 0.49–0.51).

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
- ~~`SimpleServerAcceptorPlugin` and `SimpleClientAcceptorPlugin` accept one
  connection or stream per frame.~~ Fixed in Phase E: they accept everything
  that's waiting.
