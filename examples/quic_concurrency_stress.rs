/// NOTE: Yes this is AI generated. No I don't care this silly example is
/// AI generated. It serves only one purpose and that's to stress and
/// abuse my API I've created, which this test does very well.
/// # QUIC Concurrency Stress Test
///
/// Repeatable benchmark of the async orchestrator. Runs a fixed plan of
/// client counts and scenarios, and reports CPU cost and data integrity
/// alongside frame time.
///
/// ## How it works
///
/// - A server is bound to a loopback address.
/// - For every client count in `--clients` (ascending), clients are added
///   until the target is reached and every client has an open
///   bidirectional stream. Clients are reused by later phases.
/// - For every scenario in `--scenarios` the phase runs:
///   1. **warmup** – clients send, nothing is measured.
///   2. **measure** – clients send a fixed payload every fixed tick, and
///      frame time, CPU, and Tokio metrics are recorded.
///   3. **drain** – sending stops, and the server keeps reading until
///      every sent byte has arrived (or the drain timeout passes).
/// - Scenarios:
///   - `active` – every stream sends.
///   - `idle` – every connection stays open, but only `--idle-fraction`
///     of the streams send. This exposes per-task polling cost.
/// - At the end a markdown table is printed to stdout, and optionally
///   appended to `--out <file>`. The process exits with an error if any
///   phase lost data or failed to open all of its streams.
///
/// ## Metrics
///
/// - `frame avg/p99` – main schedule work time (`First` → `Last`), which
///   excludes the runner's sleep.
/// - `interval p99` – wall-clock frame interval (`Time::delta`).
/// - `cpu` – process CPU time (user + sys) during the measure window,
///   shown as cores used.
/// - `tokio busy` – summed `worker_total_busy_duration` over all Tokio
///   workers, shown as a fraction of `workers × wall time`.
/// - `max gq` – maximum Tokio `global_queue_depth` sampled each frame.
/// - `lost` – cumulative bytes sent minus bytes received after the drain.
///   Must be 0.
///
/// ## Usage
///
/// ```ignore
/// cargo run --example quic_concurrency_stress --release -- \
///     --clients 100,300,600 --duration 10 --scenarios active,idle
/// ```
///
/// Run with `--help` for all options.
use bevy::{
    DefaultPlugins, MinimalPlugins,
    app::{
        App, AppExit, First, FixedPostUpdate, Last, PluginGroup, ScheduleRunnerPlugin,
        Startup, TaskPoolOptions, TaskPoolPlugin, TaskPoolThreadAssignmentPolicy,
        Update,
    },
    ecs::{
        component::Component,
        entity::Entity,
        hierarchy::{ChildOf, Children},
        message::MessageWriter,
        query::{With, Without},
        resource::Resource,
        schedule::IntoScheduleConfigs,
        system::{Commands, Query, Res, ResMut},
    },
    log::{LogPlugin, error, info, warn},
    render::{
        RenderPlugin,
        settings::{PowerPreference, WgpuSettings},
    },
    tasks::available_parallelism,
    time::Time,
    utils::default,
};
use bevy_s2n_quic::{
    QuicDefaultPlugins,
    async_plugin::QuicAsyncPlugin,
    client::{QuicClient, marker::QuicClientMarker},
    common::{
        connection::QuicConnection,
        runtime::TokioRuntime,
        stream::{receive::QuicReceiveStream, send::QuicSendStream},
    },
    server::{QuicServer, marker::QuicServerMarker},
};
use bytes::Bytes;
use s2n_quic::client::Connect;
use std::{
    fmt::Write as _,
    io::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

// ─── Tuning knobs ────────────────────────────────────────────────────────────

/// Server address (loopback only).
const BIND_ADDR: &str = "127.0.0.1:17779";

/// Default payload size, sent by every sending client every fixed-update tick.
const DEFAULT_PAYLOAD_BYTES: usize = 36;

/// Frame rate of the headless schedule runner.
const RUNNER_HZ: f64 = 60.0;

/// Maximum clients spawned per frame while ramping, to spread out handshakes.
const MAX_SPAWNS_PER_FRAME: usize = 50;

/// Log filter. The library's per-poll spans are INFO, so it must be at WARN
/// or the spans dominate the measurement.
const LOG_FILTER: &str = "info,bevy_s2n_quic=warn,s2n_quic=warn";

// ─── Configuration ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Active,
    Idle,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::Active => "active",
            Scenario::Idle => "idle",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    client_counts: Vec<usize>,
    scenarios: Vec<Scenario>,
    warmup: Duration,
    duration: Duration,
    drain: Duration,
    ready_timeout: Duration,
    idle_fraction: f64,
    payload: Bytes,
    tokio_threads: Option<usize>,
    windowed: bool,
    out: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client_counts: vec![100, 300, 600],
            scenarios: vec![Scenario::Active, Scenario::Idle],
            warmup: Duration::from_secs(2),
            duration: Duration::from_secs(10),
            drain: Duration::from_secs(5),
            ready_timeout: Duration::from_secs(30),
            idle_fraction: 0.1,
            payload: payload_of(DEFAULT_PAYLOAD_BYTES),
            tokio_threads: None,
            windowed: false,
            out: None,
        }
    }
}

const USAGE: &str = "\
Usage: quic_concurrency_stress [OPTIONS]

Options:
  --clients <N,N,..>      Client counts to run, ascending [default: 100,300,600]
  --scenarios <S,S,..>    Scenarios per client count: active, idle [default: active,idle]
  --duration <SECS>       Measure window per phase [default: 10]
  --warmup <SECS>         Warmup per phase before measuring [default: 2]
  --drain <SECS>          Max time to wait for in-flight data after a phase [default: 5]
  --ready-timeout <SECS>  Max time to wait for clients to connect [default: 30]
  --idle-fraction <F>     Fraction of streams sending in the idle scenario [default: 0.1]
  --payload <BYTES>       Bytes sent per sender per fixed tick [default: 36]
  --tokio-threads <N>     Tokio worker threads [default: library default]
  --windowed              Use DefaultPlugins with a window instead of headless
  --out <FILE>            Append the markdown results to FILE
  -h, --help              Print this help";

impl Config {
    fn from_args() -> Result<Self, String> {
        let mut cfg = Config::default();
        let mut args = std::env::args().skip(1);

        let secs = |v: String| -> Result<Duration, String> {
            v.parse::<f64>()
                .ok()
                .filter(|s| s.is_finite() && *s >= 0.0)
                .map(Duration::from_secs_f64)
                .ok_or_else(|| format!("invalid duration: {v}"))
        };

        while let Some(arg) = args.next() {
            let mut value = || args.next().ok_or_else(|| format!("{arg} needs a value"));

            match arg.as_str() {
                "--clients" => {
                    let mut counts = value()?
                        .split(',')
                        .map(|s| s.trim().parse::<usize>())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| format!("invalid --clients: {e}"))?;
                    counts.sort_unstable();
                    counts.dedup();
                    counts.retain(|&c| c > 0);
                    if counts.is_empty() {
                        return Err("--clients needs at least one count > 0".into());
                    }
                    cfg.client_counts = counts;
                }
                "--scenarios" => {
                    cfg.scenarios = value()?
                        .split(',')
                        .map(|s| match s.trim() {
                            "active" => Ok(Scenario::Active),
                            "idle" => Ok(Scenario::Idle),
                            other => Err(format!("unknown scenario: {other}")),
                        })
                        .collect::<Result<_, _>>()?;
                    if cfg.scenarios.is_empty() {
                        return Err("--scenarios needs at least one scenario".into());
                    }
                }
                "--duration" => cfg.duration = secs(value()?)?,
                "--warmup" => cfg.warmup = secs(value()?)?,
                "--drain" => cfg.drain = secs(value()?)?,
                "--ready-timeout" => cfg.ready_timeout = secs(value()?)?,
                "--idle-fraction" => {
                    let v = value()?;
                    cfg.idle_fraction = v
                        .parse::<f64>()
                        .ok()
                        .filter(|f| (0.0..=1.0).contains(f))
                        .ok_or_else(|| format!("invalid --idle-fraction: {v}"))?;
                }
                "--payload" => {
                    let v = value()?;
                    let size = v
                        .parse::<usize>()
                        .ok()
                        .filter(|&n| n > 0)
                        .ok_or_else(|| format!("invalid --payload: {v}"))?;
                    cfg.payload = payload_of(size);
                }
                "--tokio-threads" => {
                    let v = value()?;
                    cfg.tokio_threads = Some(
                        v.parse::<usize>()
                            .ok()
                            .filter(|&n| n > 0)
                            .ok_or_else(|| format!("invalid --tokio-threads: {v}"))?,
                    );
                }
                "--windowed" => cfg.windowed = true,
                "--out" => cfg.out = Some(PathBuf::from(value()?)),
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        Ok(cfg)
    }

    fn phases(&self) -> Vec<Phase> {
        self.client_counts
            .iter()
            .flat_map(|&clients| {
                self.scenarios.iter().map(move |&scenario| Phase {
                    clients,
                    scenario,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
struct Phase {
    clients: usize,
    scenario: Scenario,
}

impl Phase {
    /// Number of streams that send during this phase.
    fn senders(&self, idle_fraction: f64) -> usize {
        match self.scenario {
            Scenario::Active => self.clients,
            Scenario::Idle => ((self.clients as f64) * idle_fraction).ceil() as usize,
        }
    }
}

// ─── Components ──────────────────────────────────────────────────────────────

/// Index of a benchmark client, used to pick the sending subset.
#[derive(Component, Clone, Copy)]
struct StressClientIndex(usize);

#[derive(Component)]
struct StressClientStream {
    index: usize,
}

// ─── Resources ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Ramp,
    Warmup,
    Measure,
    Drain,
    Done,
}

#[derive(Resource)]
struct StressBench {
    cfg: Config,
    phases: Vec<Phase>,
    phase_idx: usize,
    stage: Stage,
    stage_started: Instant,
    spawned_clients: usize,
    /// Streams per phase that are allowed to send (index < senders).
    senders: usize,

    // Cumulative counters over the whole run.
    bytes_sent: u64,
    bytes_received: u64,
    send_full: u64,

    // Measure window state.
    window: Option<WindowStart>,
    frame_work_ms: Vec<f64>,
    frame_interval_ms: Vec<f64>,
    max_global_queue: usize,
    streams_ready: usize,
    pending: Option<PhaseResult>,

    frame_start: Instant,
    results: Vec<PhaseResult>,
    run_start: Instant,
}

impl StressBench {
    fn new(cfg: Config) -> Self {
        let phases = cfg.phases();
        let now = Instant::now();
        Self {
            cfg,
            phases,
            phase_idx: 0,
            stage: Stage::Ramp,
            stage_started: now,
            spawned_clients: 0,
            senders: 0,
            bytes_sent: 0,
            bytes_received: 0,
            send_full: 0,
            window: None,
            frame_work_ms: Vec::new(),
            frame_interval_ms: Vec::new(),
            max_global_queue: 0,
            streams_ready: 0,
            pending: None,
            frame_start: now,
            results: Vec::new(),
            run_start: now,
        }
    }

    fn phase(&self) -> Phase {
        self.phases[self.phase_idx]
    }

    fn enter(&mut self, stage: Stage) {
        self.stage = stage;
        self.stage_started = Instant::now();
    }

    fn sending(&self) -> bool {
        matches!(self.stage, Stage::Warmup | Stage::Measure)
    }
}

/// Counter snapshot at the start of a measure window.
struct WindowStart {
    at: Instant,
    cpu: Duration,
    tokio_busy: Duration,
    bytes_sent: u64,
    bytes_received: u64,
    send_full: u64,
}

#[derive(Debug, Clone)]
struct PhaseResult {
    phase: Phase,
    senders: usize,
    streams_ready: usize,
    wall: Duration,
    frames: usize,
    frame_avg_ms: f64,
    frame_p99_ms: f64,
    frame_max_ms: f64,
    interval_p99_ms: f64,
    cpu: Duration,
    tokio_busy: Duration,
    tokio_workers: usize,
    max_global_queue: usize,
    sent: u64,
    received: u64,
    send_full: u64,
    /// Cumulative sent − received after the drain.
    lost: i64,
}

impl PhaseResult {
    fn ok(&self) -> bool {
        self.lost == 0 && self.streams_ready >= self.phase.clients
    }
}

// ─── App ─────────────────────────────────────────────────────────────────────

fn main() -> AppExit {
    let cfg = match Config::from_args() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            return AppExit::error();
        }
    };

    let task_pool = TaskPoolPlugin {
        task_pool_options: TaskPoolOptions {
            compute: TaskPoolThreadAssignmentPolicy {
                min_threads: 1,
                max_threads: 4,
                percent: 0.5,
                on_thread_spawn: None,
                on_thread_destroy: None,
            },
            ..default()
        },
    };
    let log = LogPlugin {
        filter: LOG_FILTER.into(),
        level: bevy::log::Level::INFO,
        ..default()
    };

    let mut app = App::new();

    if cfg.windowed {
        app.add_plugins(
            DefaultPlugins
                .set(RenderPlugin {
                    render_creation: WgpuSettings {
                        power_preference: PowerPreference::LowPower,
                        ..default()
                    }
                    .into(),
                    ..default()
                })
                .set(log)
                .set(task_pool),
        );
    } else {
        app.add_plugins((
            MinimalPlugins
                .set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                    1.0 / RUNNER_HZ,
                )))
                .set(task_pool),
            log,
        ));
    }

    let async_plugin = match cfg.tokio_threads {
        Some(n) => QuicAsyncPlugin::new_with_threads(n),
        None => QuicAsyncPlugin::default(),
    };

    app.add_plugins(QuicDefaultPlugins.set(async_plugin))
        .insert_resource(StressBench::new(cfg))
        .add_systems(Startup, setup)
        .add_systems(First, frame_begin)
        .add_systems(Update, (drive, spawn_clients, open_streams).chain())
        .add_systems(FixedPostUpdate, (client_send, server_recv))
        .add_systems(Last, frame_end)
        .run()
}

// ─── Setup ───────────────────────────────────────────────────────────────────

fn setup(mut commands: Commands, runtime: Res<TokioRuntime>, bench: Res<StressBench>) {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let cert_str = format!("{manifest}/examples/certs/cert.pem");
    let key_str = format!("{manifest}/examples/certs/key.pem");
    let ip: SocketAddr = BIND_ADDR.parse().unwrap();

    commands.spawn(
        QuicServer::bind(&runtime, ip, Path::new(&cert_str), Path::new(&key_str))
            .expect("Failed to bind server"),
    );

    let cfg = &bench.cfg;
    info!(
        "=== QUIC Concurrency Stress Test ===\n\
         Mode:      {}\n\
         Clients:   {:?}  |  Scenarios: {:?}\n\
         Phase:     warmup {:?}, measure {:?}, drain ≤{:?}\n\
         Payload:   {} B per sender per fixed tick  |  idle fraction {}\n\
         Tokio:     {} worker(s)  |  logical CPUs {}",
        if cfg.windowed { "windowed" } else { "headless" },
        cfg.client_counts,
        cfg.scenarios.iter().map(|s| s.name()).collect::<Vec<_>>(),
        cfg.warmup,
        cfg.duration,
        cfg.drain,
        cfg.payload.len(),
        cfg.idle_fraction,
        runtime.handle().metrics().num_workers(),
        available_parallelism(),
    );
}

// ─── Frame timing ────────────────────────────────────────────────────────────

fn frame_begin(mut bench: ResMut<StressBench>) {
    bench.frame_start = Instant::now();
}

fn frame_end(
    time: Res<Time>,
    runtime: Res<TokioRuntime>,
    mut bench: ResMut<StressBench>,
) {
    if bench.stage != Stage::Measure {
        return;
    }

    let work = bench.frame_start.elapsed().as_secs_f64() * 1000.0;
    bench.frame_work_ms.push(work);
    bench.frame_interval_ms.push(time.delta_secs_f64() * 1000.0);

    let depth = runtime.handle().metrics().global_queue_depth();
    bench.max_global_queue = bench.max_global_queue.max(depth);
}

// ─── Stage machine ───────────────────────────────────────────────────────────

fn drive(
    runtime: Res<TokioRuntime>,
    mut bench: ResMut<StressBench>,
    ready_streams: Query<(), (With<StressClientStream>, With<QuicSendStream>)>,
    mut exit: MessageWriter<AppExit>,
) {
    let elapsed = bench.stage_started.elapsed();

    match bench.stage {
        Stage::Ramp => {
            let phase = bench.phase();
            let ready = ready_streams.iter().count();
            let timed_out = elapsed >= bench.cfg.ready_timeout;

            if ready >= phase.clients || timed_out {
                if timed_out {
                    warn!(
                        "Only {ready}/{} streams ready after {:?}, continuing",
                        phase.clients, bench.cfg.ready_timeout
                    );
                }
                bench.streams_ready = ready;
                bench.senders = phase.senders(bench.cfg.idle_fraction);
                info!(
                    "[{}/{}] {} clients, {} ({} sending): warming up",
                    bench.phase_idx + 1,
                    bench.phases.len(),
                    phase.clients,
                    phase.scenario.name(),
                    bench.senders,
                );
                bench.enter(Stage::Warmup);
            }
        }

        Stage::Warmup => {
            if elapsed >= bench.cfg.warmup {
                bench.frame_work_ms.clear();
                bench.frame_interval_ms.clear();
                bench.max_global_queue = 0;
                bench.window = Some(WindowStart {
                    at: Instant::now(),
                    cpu: process_cpu_time(),
                    tokio_busy: tokio_busy(&runtime),
                    bytes_sent: bench.bytes_sent,
                    bytes_received: bench.bytes_received,
                    send_full: bench.send_full,
                });
                bench.enter(Stage::Measure);
            }
        }

        Stage::Measure => {
            if elapsed >= bench.cfg.duration {
                let start = bench.window.take().expect("measure window missing");
                let mut work = std::mem::take(&mut bench.frame_work_ms);
                let mut interval = std::mem::take(&mut bench.frame_interval_ms);

                bench.pending = Some(PhaseResult {
                    phase: bench.phase(),
                    senders: bench.senders,
                    streams_ready: bench.streams_ready,
                    wall: start.at.elapsed(),
                    frames: work.len(),
                    frame_avg_ms: mean(&work),
                    frame_p99_ms: percentile(&mut work, 0.99),
                    frame_max_ms: percentile(&mut work, 1.0),
                    interval_p99_ms: percentile(&mut interval, 0.99),
                    cpu: process_cpu_time().saturating_sub(start.cpu),
                    tokio_busy: tokio_busy(&runtime).saturating_sub(start.tokio_busy),
                    tokio_workers: runtime.handle().metrics().num_workers(),
                    max_global_queue: bench.max_global_queue,
                    sent: bench.bytes_sent - start.bytes_sent,
                    received: bench.bytes_received - start.bytes_received,
                    send_full: bench.send_full - start.send_full,
                    lost: 0,
                });
                bench.enter(Stage::Drain);
            }
        }

        Stage::Drain => {
            let drained = bench.bytes_received >= bench.bytes_sent;

            if drained || elapsed >= bench.cfg.drain {
                let lost = bench.bytes_sent as i64 - bench.bytes_received as i64;
                let mut result = bench.pending.take().expect("pending result missing");
                result.lost = lost;

                if lost != 0 {
                    warn!(
                        "Byte mismatch after drain: sent {} / received {} (Δ {lost})",
                        bench.bytes_sent, bench.bytes_received
                    );
                }
                info!("{}", summary_line(&result));
                bench.results.push(result);

                if bench.phase_idx + 1 < bench.phases.len() {
                    bench.phase_idx += 1;
                    bench.enter(Stage::Ramp);
                } else {
                    bench.enter(Stage::Done);
                    let ok = report(&bench);
                    exit.write(if ok { AppExit::Success } else { AppExit::error() });
                }
            }
        }

        Stage::Done => {}
    }
}

// ─── Client spawning ─────────────────────────────────────────────────────────

fn spawn_clients(
    mut commands: Commands,
    runtime: Res<TokioRuntime>,
    mut bench: ResMut<StressBench>,
) {
    if bench.stage != Stage::Ramp {
        return;
    }

    let target = bench.phase().clients;
    let to_spawn = target
        .saturating_sub(bench.spawned_clients)
        .min(MAX_SPAWNS_PER_FRAME);
    if to_spawn == 0 {
        return;
    }

    let manifest = env!("CARGO_MANIFEST_DIR");
    let cert_str = format!("{manifest}/examples/certs/cert.pem");
    let cert_path = Path::new(&cert_str);
    let ip: SocketAddr = BIND_ADDR.parse().unwrap();

    for _ in 0..to_spawn {
        let index = bench.spawned_clients;
        // Count failures too, so a bad client cannot stall the ramp forever;
        // the missing stream shows up in `streams_ready`.
        bench.spawned_clients += 1;

        let mut client = match QuicClient::new_with_tls(&runtime, cert_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create client: {e}");
                continue;
            }
        };
        let conn = client.open_connection(Connect::new(ip).with_server_name("localhost"));
        commands.spawn(client).with_children(|p| {
            p.spawn((conn, StressClientIndex(index)));
        });
    }
}

// ─── Opening streams ─────────────────────────────────────────────────────────

#[allow(clippy::type_complexity)]
fn open_streams(
    mut commands: Commands,
    connections: Query<
        (Entity, &mut QuicConnection, &StressClientIndex),
        (Without<Children>, With<QuicClientMarker>),
    >,
) {
    for (entity, mut conn, index) in connections {
        match conn.open_bidrectional_stream() {
            Ok(bundle) => {
                commands.spawn((
                    bundle,
                    StressClientStream { index: index.0 },
                    QuicClientMarker,
                    ChildOf(entity),
                ));
            }
            Err(e) => {
                warn!("open_streams: {e}");
            }
        }
    }
}

// ─── Client send (fixed tick) ────────────────────────────────────────────────

fn client_send(
    mut streams: Query<
        (&mut QuicSendStream, &StressClientStream),
        With<QuicClientMarker>,
    >,
    mut bench: ResMut<StressBench>,
) {
    if !bench.sending() {
        return;
    }

    let senders = bench.senders;
    let payload = bench.cfg.payload.clone();
    let mut sent = 0u64;
    let mut full = 0u64;

    for (mut stream, meta) in &mut streams {
        if meta.index >= senders || !stream.is_open() {
            continue;
        }
        match stream.send(payload.clone()) {
            Ok(()) => sent += payload.len() as u64,
            Err(_) => full += 1,
        }
    }

    bench.bytes_sent += sent;
    bench.send_full += full;
}

// ─── Server receive (fixed tick) ─────────────────────────────────────────────
//
// Server-side streams are automatically accepted by SimpleServerAcceptorPlugin
// and tagged with QuicServerMarker (see server/acceptor.rs).
//
// Uses the non-blocking `recv` rather than `recv_many`, which blocks the
// calling thread until at least one packet is available. That would stall
// the frame on any silent stream (e.g. the idle scenario).

fn server_recv(
    mut streams: Query<&mut QuicReceiveStream, With<QuicServerMarker>>,
    mut bench: ResMut<StressBench>,
) {
    let mut received = 0u64;

    // Closed streams are still drained so buffered data is not miscounted.
    for mut stream in &mut streams {
        let mut any = false;
        while let Some(packet) = stream.recv() {
            received += packet.payload.len() as u64;
            any = true;
        }
        if any {
            stream.log_outstanding_errors();
        }
    }

    bench.bytes_received += received;
}

/// A payload of `size` bytes cycling through `A..Z`.
fn payload_of(size: usize) -> Bytes {
    (0..size).map(|i| b'A' + (i % 26) as u8).collect::<Vec<_>>().into()
}

// ─── Metrics helpers ─────────────────────────────────────────────────────────

/// Total user + system CPU time consumed by this process.
#[cfg(unix)]
fn process_cpu_time() -> Duration {
    // SAFETY: `getrusage` only writes into the provided, zeroed struct.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return Duration::ZERO;
        }
        usage
    };
    let tv = |t: libc::timeval| {
        Duration::from_secs(t.tv_sec as u64) + Duration::from_micros(t.tv_usec as u64)
    };
    tv(usage.ru_utime) + tv(usage.ru_stime)
}

#[cfg(not(unix))]
fn process_cpu_time() -> Duration {
    Duration::ZERO
}

/// Busy time summed over all Tokio workers.
fn tokio_busy(runtime: &TokioRuntime) -> Duration {
    let metrics = runtime.handle().metrics();
    (0..metrics.num_workers())
        .map(|w| metrics.worker_total_busy_duration(w))
        .sum()
}

fn mean(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.iter().sum::<f64>() / samples.len() as f64
}

/// Nearest-rank percentile, `p` in `[0, 1]`. Sorts `samples` in place.
fn percentile(samples: &mut [f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable_by(f64::total_cmp);
    let rank = ((p * samples.len() as f64).ceil() as usize).clamp(1, samples.len());
    samples[rank - 1]
}

// ─── Reporting ───────────────────────────────────────────────────────────────

fn summary_line(r: &PhaseResult) -> String {
    let wall = r.wall.as_secs_f64();
    format!(
        "{:>4} clients {:<6} ({:>4} sending): frame avg {:.2}ms p99 {:.2}ms | \
         cpu {:.2} cores | tokio busy {:.0}% | max gq {} | rx {}/s | lost {} B{}",
        r.phase.clients,
        r.phase.scenario.name(),
        r.senders,
        r.frame_avg_ms,
        r.frame_p99_ms,
        r.cpu.as_secs_f64() / wall,
        busy_pct(r),
        r.max_global_queue,
        fmt_bytes((r.received as f64 / wall) as u64),
        r.lost,
        if r.ok() { "" } else { "  [FAIL]" },
    )
}

fn busy_pct(r: &PhaseResult) -> f64 {
    let capacity = r.wall.as_secs_f64() * r.tokio_workers as f64;
    if capacity == 0.0 {
        return 0.0;
    }
    100.0 * r.tokio_busy.as_secs_f64() / capacity
}

/// Prints the markdown report to stdout (and `--out`). Returns `true` if
/// every phase passed.
fn report(bench: &StressBench) -> bool {
    let cfg = &bench.cfg;
    let mut md = String::new();

    let _ = writeln!(md, "### quic_concurrency_stress — {}", git_describe());
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "- Mode: {} | logical CPUs: {} | tokio workers: {} | runner: {RUNNER_HZ} Hz",
        if cfg.windowed { "windowed" } else { "headless" },
        available_parallelism(),
        bench.results.first().map_or(0, |r| r.tokio_workers),
    );
    let _ = writeln!(
        md,
        "- Per phase: warmup {:?}, measure {:?}, drain ≤{:?} | payload {} B/tick | idle fraction {}",
        cfg.warmup,
        cfg.duration,
        cfg.drain,
        cfg.payload.len(),
        cfg.idle_fraction,
    );
    let _ = writeln!(
        md,
        "- Total run time: {:.1}s",
        bench.run_start.elapsed().as_secs_f64()
    );
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "| clients | scenario | senders | ready | frames | frame avg ms | frame p99 ms \
         | frame max ms | interval p99 ms | cpu cores | tokio busy (worker-s) | tokio busy % \
         | max gq | sent | received | send full | lost B | ok |"
    );
    let _ = writeln!(
        md,
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    );

    for r in &bench.results {
        let wall = r.wall.as_secs_f64();
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.0} \
             | {} | {} | {} | {} | {} | {} |",
            r.phase.clients,
            r.phase.scenario.name(),
            r.senders,
            r.streams_ready,
            r.frames,
            r.frame_avg_ms,
            r.frame_p99_ms,
            r.frame_max_ms,
            r.interval_p99_ms,
            r.cpu.as_secs_f64() / wall,
            r.tokio_busy.as_secs_f64(),
            busy_pct(r),
            r.max_global_queue,
            fmt_bytes(r.sent),
            fmt_bytes(r.received),
            r.send_full,
            r.lost,
            if r.ok() { "✓" } else { "✗" },
        );
    }

    let ok = bench.results.iter().all(PhaseResult::ok);
    let _ = writeln!(md);

    println!("\n{md}");
    let _ = std::io::stdout().flush();

    if let Some(path) = &cfg.out {
        let res = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut f| f.write_all(md.as_bytes()));
        match res {
            Ok(()) => info!("Appended results to {}", path.display()),
            Err(e) => error!("Failed to write {}: {e}", path.display()),
        }
    }

    if !ok {
        error!("One or more phases failed (data loss or streams not ready)");
    }
    ok
}

fn git_describe() -> String {
    std::process::Command::new("git")
        .args(["describe", "--always", "--dirty"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| "unknown commit".to_owned())
}

fn fmt_bytes(b: u64) -> String {
    if b >= 1_048_576 {
        format!("{:.2} MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{b} B")
    }
}
