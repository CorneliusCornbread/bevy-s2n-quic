/// NOTE: Yes this is AI generated. No I don't care this silly example is
/// AI generated. It serves only one purpose and that's to stress and
/// abuse my API I've created, which this test does very well.
/// # QUIC Concurrency Stress Test
///
/// Repeatable benchmark of the async runtime. Runs a fixed plan of
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
///   2. **measure** – clients send `--burst` messages of a fixed payload
///      every fixed tick, and frame time, CPU, and Tokio metrics are
///      recorded.
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
/// ## Roles
///
/// - `--role both` (default) runs the server and the clients in one app.
/// - `--role server` and `--role clients` run them as separate processes, so
///   the server's numbers don't include the clients' work. The clients
///   process drives the phase plan over a small TCP control connection
///   (`--control`), and the server reports how many bytes it has received
///   back over it, so the data-loss check still works. Each process prints
///   its own table. Start the server first:
///
/// ```ignore
/// taskset -c 0-3,8-11  cargo run --release --example quic_concurrency_stress -- --role server
/// taskset -c 4-7,12-15 cargo run --release --example quic_concurrency_stress -- --role clients
/// ```
///
/// ## Threads
///
/// Bevy's task pools and the Tokio workers are sized to fit in the CPUs the
/// process may use (which respects `taskset`): the main thread, one IO
/// thread, one async compute thread, up to 4 compute threads, plus the Tokio
/// workers. `--pin` (Linux) also pins the Tokio workers and the Bevy threads
/// to separate physical cores, and `--tokio-nice <N>` sets the Tokio workers'
/// nice value. Both use `QuicDriverPlugin::on_thread_start` and Bevy's
/// `on_thread_spawn` hooks.
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
        Startup, TaskPoolOptions, TaskPoolPlugin, TaskPoolThreadAssignmentPolicy, Update,
    },
    ecs::{
        component::Component,
        entity::Entity,
        hierarchy::{ChildOf, Children},
        message::MessageWriter,
        query::{With, Without},
        resource::Resource,
        schedule::IntoScheduleConfigs,
        system::{Commands, Local, Query, Res, ResMut},
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
    QuicDefaultPlugins, backend,
    client::{Connect, QuicClient, marker::QuicClientMarker},
    common::{
        connection::QuicConnection,
        runtime::QuicRuntime,
        stream::{receive::QuicReceiveStream, send::QuicSendStream},
    },
    config::{QuicClientConfig, QuicServerConfig},
    driver_plugin::QuicDriverPlugin,
    server::{QuicServer, marker::QuicServerMarker},
};
use bytes::Bytes;
use std::{
    fmt::Write as _,
    io::{self, Read as _, Write as _},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

// ─── Tuning knobs ────────────────────────────────────────────────────────────

/// Default QUIC server address.
const DEFAULT_ADDR: &str = "127.0.0.1:17779";

/// Default control connection address for `--role server|clients`.
const DEFAULT_CONTROL_ADDR: &str = "127.0.0.1:17780";

/// Default payload size, sent by every sending client every fixed-update tick.
const DEFAULT_PAYLOAD_BYTES: usize = 36;

/// Frame rate of the headless schedule runner.
const RUNNER_HZ: f64 = 60.0;

/// Maximum clients spawned per frame while ramping, to spread out handshakes.
const MAX_SPAWNS_PER_FRAME: usize = 50;

/// Upper bound on Bevy compute threads. The benchmark's systems barely run in
/// parallel, so more threads only add wakeups.
const MAX_COMPUTE_THREADS: usize = 4;

/// How often `--role clients` retries the control connection.
const CONTROL_RETRY: Duration = Duration::from_millis(250);

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

    fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(Scenario::Active),
            "idle" => Some(Scenario::Idle),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// Server and clients in one process.
    Both,
    /// Only the server, driven by a `Clients` process over the control link.
    Server,
    /// Only the clients, which drive the phase plan.
    Clients,
}

impl Role {
    fn name(self) -> &'static str {
        match self {
            Role::Both => "both",
            Role::Server => "server",
            Role::Clients => "clients",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    role: Role,
    addr: SocketAddr,
    control: SocketAddr,
    client_counts: Vec<usize>,
    scenarios: Vec<Scenario>,
    warmup: Duration,
    duration: Duration,
    drain: Duration,
    ready_timeout: Duration,
    idle_fraction: f64,
    payload: Bytes,
    burst: usize,
    tokio_threads: Option<usize>,
    pin: bool,
    tokio_nice: Option<i32>,
    windowed: bool,
    out: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            role: Role::Both,
            addr: DEFAULT_ADDR.parse().unwrap(),
            control: DEFAULT_CONTROL_ADDR.parse().unwrap(),
            client_counts: vec![100, 300, 600],
            scenarios: vec![Scenario::Active, Scenario::Idle],
            warmup: Duration::from_secs(2),
            duration: Duration::from_secs(10),
            drain: Duration::from_secs(5),
            ready_timeout: Duration::from_secs(30),
            idle_fraction: 0.1,
            payload: payload_of(DEFAULT_PAYLOAD_BYTES),
            burst: 1,
            tokio_threads: None,
            pin: false,
            tokio_nice: None,
            windowed: false,
            out: None,
        }
    }
}

const USAGE: &str = "\
Usage: quic_concurrency_stress [OPTIONS]

Options:
  --role <ROLE>           both, server or clients [default: both]
  --addr <IP:PORT>        QUIC server address [default: 127.0.0.1:17779]
  --control <IP:PORT>     Control address for --role server|clients [default: 127.0.0.1:17780]
  --clients <N,N,..>      Client counts to run, ascending [default: 100,300,600]
  --scenarios <S,S,..>    Scenarios per client count: active, idle [default: active,idle]
  --duration <SECS>       Measure window per phase [default: 10]
  --warmup <SECS>         Warmup per phase before measuring [default: 2]
  --drain <SECS>          Max time to wait for in-flight data after a phase [default: 5]
  --ready-timeout <SECS>  Max time to wait for clients (and the control link) [default: 30]
  --idle-fraction <F>     Fraction of streams sending in the idle scenario [default: 0.1]
  --payload <BYTES>       Bytes per message [default: 36]
  --burst <N>             Messages sent per sender per fixed tick [default: 1]
  --tokio-threads <N>     Tokio worker threads [default: library default]
  --pin                   Pin Tokio workers and Bevy threads to separate cores (Linux)
  --tokio-nice <N>        Nice value for the Tokio worker threads (Linux)
  --windowed              Use DefaultPlugins with a window instead of headless
  --out <FILE>            Append the markdown results to FILE
  -h, --help              Print this help

With --role server, the phase options are ignored: the clients process sends them.";

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
        let addr = |v: String| -> Result<SocketAddr, String> {
            v.parse().map_err(|e| format!("invalid address {v}: {e}"))
        };

        while let Some(arg) = args.next() {
            let mut value = || args.next().ok_or_else(|| format!("{arg} needs a value"));

            match arg.as_str() {
                "--role" => {
                    cfg.role = match value()?.as_str() {
                        "both" => Role::Both,
                        "server" => Role::Server,
                        "clients" => Role::Clients,
                        other => return Err(format!("unknown role: {other}")),
                    }
                }
                "--addr" => cfg.addr = addr(value()?)?,
                "--control" => cfg.control = addr(value()?)?,
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
                        .map(|s| {
                            Scenario::parse(s.trim())
                                .ok_or_else(|| format!("unknown scenario: {s}"))
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
                "--burst" => {
                    let v = value()?;
                    cfg.burst = v
                        .parse::<usize>()
                        .ok()
                        .filter(|&n| n > 0)
                        .ok_or_else(|| format!("invalid --burst: {v}"))?;
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
                "--pin" => cfg.pin = true,
                "--tokio-nice" => {
                    let v = value()?;
                    cfg.tokio_nice = Some(
                        v.parse::<i32>()
                            .ok()
                            .filter(|n| (-20..=19).contains(n))
                            .ok_or_else(|| format!("invalid --tokio-nice: {v}"))?,
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

        if (cfg.pin || cfg.tokio_nice.is_some()) && !cfg!(target_os = "linux") {
            return Err("--pin and --tokio-nice are only supported on Linux".into());
        }

        Ok(cfg)
    }

    fn phases(&self) -> Vec<Phase> {
        self.client_counts
            .iter()
            .flat_map(|&clients| {
                self.scenarios
                    .iter()
                    .map(move |&scenario| Phase { clients, scenario })
            })
            .collect()
    }

    fn runs_server(&self) -> bool {
        self.role != Role::Clients
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

// ─── Thread placement ────────────────────────────────────────────────────────

/// Thread counts for Bevy's task pools and the Tokio runtime, sized so that
/// all of them fit in the CPUs this process may use where possible.
#[derive(Debug, Clone)]
struct ThreadPlan {
    /// CPUs this process may run on.
    cpus: usize,
    tokio: usize,
    io: usize,
    async_compute: usize,
    compute: usize,
    /// `(tokio, bevy)` CPU sets when `--pin` is set.
    pinned: Option<(Vec<usize>, Vec<usize>)>,
    tokio_nice: Option<i32>,
}

impl ThreadPlan {
    /// The main thread, one IO and one async compute thread. The benchmark
    /// doesn't use the last two, but Bevy always creates them.
    const FIXED_BEVY_THREADS: usize = 3;

    fn new(cfg: &Config) -> Self {
        // `available_parallelism` respects the affinity mask (e.g. `taskset`).
        let cpus = available_parallelism();
        let tokio = cfg
            .tokio_threads
            .unwrap_or_else(QuicRuntime::default_thread_count);
        let compute = cpus
            .saturating_sub(tokio + Self::FIXED_BEVY_THREADS)
            .clamp(1, MAX_COMPUTE_THREADS);

        let pinned = cfg.pin.then(|| {
            let allowed = affinity::allowed_cpus().unwrap_or_else(|e| {
                warn_once(&PIN_WARNED, || format!("can't read the CPU affinity: {e}"));
                (0..cpus).collect()
            });
            affinity::split(&allowed, tokio)
        });

        Self {
            cpus,
            tokio,
            io: 1,
            async_compute: 1,
            compute,
            pinned,
            tokio_nice: cfg.tokio_nice,
        }
    }

    fn bevy_threads(&self) -> usize {
        1 + self.io + self.async_compute + self.compute
    }

    fn total(&self) -> usize {
        self.bevy_threads() + self.tokio
    }

    /// Bevy's pools with exact thread counts. Pool threads are pinned to the
    /// Bevy CPU set when pinning.
    fn task_pool_plugin(&self) -> TaskPoolPlugin {
        let on_spawn = self.pinned.as_ref().map(|(_, bevy)| {
            let bevy = bevy.clone();
            Arc::new(move || pin_or_warn(&bevy)) as Arc<dyn Fn() + Send + Sync>
        });
        let fixed = |threads| TaskPoolThreadAssignmentPolicy {
            min_threads: threads,
            max_threads: threads,
            percent: 0.0,
            on_thread_spawn: on_spawn.clone(),
            on_thread_destroy: None,
        };
        let pools = self.io + self.async_compute + self.compute;

        TaskPoolPlugin {
            task_pool_options: TaskPoolOptions {
                min_total_threads: pools,
                max_total_threads: pools,
                io: fixed(self.io),
                async_compute: fixed(self.async_compute),
                compute: fixed(self.compute),
            },
        }
    }

    /// The QUIC runtime, with its threads pinned and niced through the
    /// thread start hook.
    fn driver_plugin(&self) -> QuicDriverPlugin {
        let plugin = QuicDriverPlugin::default().with_threads(self.tokio);
        let tokio_cpus = self.pinned.as_ref().map(|(tokio, _)| tokio.clone());
        let nice = self.tokio_nice;

        if tokio_cpus.is_none() && nice.is_none() {
            return plugin;
        }

        // Also runs for Tokio's blocking-pool threads, which is fine here.
        plugin.on_thread_start(move || {
            if let Some(cpus) = &tokio_cpus {
                pin_or_warn(cpus);
            }
            if let Some(nice) = nice
                && let Err(e) = affinity::set_current_thread_nice(nice)
            {
                warn_once(&NICE_WARNED, || {
                    format!("can't set Tokio worker nice to {nice}: {e}")
                });
            }
        })
    }

    fn describe(&self) -> String {
        let mut s = format!(
            "tokio {} + bevy {} (main 1, io {}, async {}, compute {}) on {} CPUs",
            self.tokio,
            self.bevy_threads(),
            self.io,
            self.async_compute,
            self.compute,
            self.cpus,
        );
        if self.total() > self.cpus {
            s.push_str(" (oversubscribed)");
        }
        match &self.pinned {
            Some((tokio, bevy)) => {
                let _ = write!(
                    s,
                    " | pinned: tokio {}, bevy {}",
                    fmt_cpus(tokio),
                    fmt_cpus(bevy)
                );
            }
            None => s.push_str(" | not pinned"),
        }
        if let Some(nice) = self.tokio_nice {
            let _ = write!(s, " | tokio nice {nice}");
        }
        s
    }
}

static PIN_WARNED: AtomicBool = AtomicBool::new(false);
static NICE_WARNED: AtomicBool = AtomicBool::new(false);

/// Thread callbacks can run before logging is set up, so use stderr, and
/// only once rather than once per thread.
fn warn_once(flag: &AtomicBool, msg: impl FnOnce() -> String) {
    if !flag.swap(true, Ordering::Relaxed) {
        eprintln!("warning: {}", msg());
    }
}

fn pin_or_warn(cpus: &[usize]) {
    if let Err(e) = affinity::pin_current_thread(cpus) {
        warn_once(&PIN_WARNED, || {
            format!("can't pin thread to CPUs {}: {e}", fmt_cpus(cpus))
        });
    }
}

/// Formats a sorted CPU list as ranges, e.g. `0-5,8-13`.
fn fmt_cpus(cpus: &[usize]) -> String {
    let mut s = String::new();
    let mut i = 0;
    while i < cpus.len() {
        let start = cpus[i];
        while i + 1 < cpus.len() && cpus[i + 1] == cpus[i] + 1 {
            i += 1;
        }
        if !s.is_empty() {
            s.push(',');
        }
        if cpus[i] == start {
            let _ = write!(s, "{start}");
        } else {
            let _ = write!(s, "{start}-{}", cpus[i]);
        }
        i += 1;
    }
    s
}

#[cfg(target_os = "linux")]
mod affinity {
    use std::io;

    /// The CPUs the calling thread may run on.
    pub fn allowed_cpus() -> io::Result<Vec<usize>> {
        // SAFETY: `cpu_set_t` is plain data, and `sched_getaffinity` only
        // writes into the provided set.
        let set = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) != 0 {
                return Err(io::Error::last_os_error());
            }
            set
        };
        Ok((0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
            .collect())
    }

    /// Restricts the calling thread to `cpus`.
    pub fn pin_current_thread(cpus: &[usize]) -> io::Result<()> {
        // SAFETY: as above. `pid` 0 is the calling thread.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for &cpu in cpus {
                libc::CPU_SET(cpu, &mut set);
            }
            if libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    /// Sets the calling thread's nice value. On Linux nice is per thread.
    /// Negative values need `CAP_SYS_NICE` (or a raised `RLIMIT_NICE`).
    pub fn set_current_thread_nice(nice: i32) -> io::Result<()> {
        // SAFETY: plain syscalls on the calling thread.
        let res = unsafe {
            let tid = libc::gettid();
            libc::setpriority(libc::PRIO_PROCESS as _, tid as libc::id_t, nice)
        };
        if res != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// A physical core: `(package, core)`.
    type CoreId = (u32, u32);

    /// The physical core a logical CPU belongs to, from sysfs.
    fn core_of(cpu: usize) -> Option<CoreId> {
        let read = |file: &str| -> Option<u32> {
            std::fs::read_to_string(format!(
                "/sys/devices/system/cpu/cpu{cpu}/topology/{file}"
            ))
            .ok()?
            .trim()
            .parse()
            .ok()
        };
        Some((read("physical_package_id")?, read("core_id")?))
    }

    /// Splits `cpus` into `(tokio, bevy)` sets. Tokio gets whole physical
    /// cores, from the end of the list, until it has at least `tokio_threads`
    /// logical CPUs. SMT siblings stay on the same side, so Tokio and Bevy
    /// don't share a core. At least one core is left for Bevy.
    pub fn split(cpus: &[usize], tokio_threads: usize) -> (Vec<usize>, Vec<usize>) {
        // Logical CPUs grouped by physical core, in order of first appearance.
        let mut cores: Vec<(Option<CoreId>, Vec<usize>)> = Vec::new();
        for &cpu in cpus {
            let core = core_of(cpu);
            match cores.iter_mut().find(|(c, _)| core.is_some() && *c == core) {
                Some((_, members)) => members.push(cpu),
                None => cores.push((core, vec![cpu])),
            }
        }

        let mut tokio = Vec::new();
        while tokio.len() < tokio_threads && cores.len() > 1 {
            let (_, members) = cores.pop().expect("checked len");
            tokio.extend(members);
        }

        let mut bevy: Vec<usize> = cores.into_iter().flat_map(|(_, m)| m).collect();
        tokio.sort_unstable();
        bevy.sort_unstable();
        (tokio, bevy)
    }
}

#[cfg(not(target_os = "linux"))]
mod affinity {
    use std::io;

    fn unsupported() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "only supported on Linux")
    }

    pub fn allowed_cpus() -> io::Result<Vec<usize>> {
        Err(unsupported())
    }

    pub fn pin_current_thread(_cpus: &[usize]) -> io::Result<()> {
        Err(unsupported())
    }

    pub fn set_current_thread_nice(_nice: i32) -> io::Result<()> {
        Err(unsupported())
    }

    pub fn split(cpus: &[usize], _tokio_threads: usize) -> (Vec<usize>, Vec<usize>) {
        (cpus.to_vec(), cpus.to_vec())
    }
}

// ─── Control link (--role server|clients) ────────────────────────────────────
//
// Newline-delimited text over TCP, polled once per frame without blocking.
//
// clients → server:
//   begin <clients> <scenario> <senders> <ready>   start of a measure window
//   end <bytes sent in window>                     end of the measure window
//   finish <total bytes sent> <lost bytes>         drain over, phase complete
//   quit                                           run complete
// server → clients:
//   recv <total bytes received>                    sent whenever it changes
//
// The clients compute `lost` from the server's last `recv`. The server can't
// compute it when `finish` arrives: by then the next phase may have started
// sending.

struct ControlLink {
    stream: TcpStream,
    inbox: Vec<u8>,
    outbox: Vec<u8>,
    /// Set once the peer closed the connection or it failed.
    closed: Option<String>,
}

impl ControlLink {
    fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            inbox: Vec::new(),
            outbox: Vec::new(),
            closed: None,
        })
    }

    /// Sends a line, queueing whatever doesn't fit in the socket buffer for
    /// the next [Self::poll].
    fn send(&mut self, line: impl std::fmt::Display) {
        let _ = writeln!(self.outbox, "{line}");
        self.try_io(Self::write_out);
    }

    /// Writes queued output and returns the complete lines received so far.
    /// Check `closed` afterwards.
    fn poll(&mut self) -> Vec<String> {
        self.try_io(Self::write_out);
        self.try_io(Self::read_in);

        let mut lines = Vec::new();
        while let Some(end) = self.inbox.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.inbox.drain(..=end).collect();
            lines.push(String::from_utf8_lossy(&line).trim().to_owned());
        }
        lines
    }

    fn try_io(&mut self, io: fn(&mut Self) -> io::Result<()>) {
        if self.closed.is_none()
            && let Err(e) = io(self)
        {
            self.closed = Some(e.to_string());
        }
    }

    fn write_out(&mut self) -> io::Result<()> {
        while !self.outbox.is_empty() {
            match self.stream.write(&self.outbox) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => drop(self.outbox.drain(..n)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn read_in(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 4096];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => return Err(io::Error::other("connection closed by peer")),
                Ok(n) => self.inbox.extend_from_slice(&buf[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Blocks until every queued line is written.
    fn flush_blocking(&mut self) -> io::Result<()> {
        self.stream.set_nonblocking(false)?;
        self.stream.write_all(&self.outbox)?;
        self.outbox.clear();
        self.stream.flush()
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
    /// `--role clients`: waiting for the control link to the server.
    Connect,
    /// Opening clients and streams. For `--role server`: waiting for the
    /// next `begin`.
    Ramp,
    Warmup,
    Measure,
    Drain,
    Done,
}

#[derive(Resource)]
struct StressBench {
    cfg: Config,
    threads: ThreadPlan,
    phases: Vec<Phase>,
    phase_idx: usize,
    stage: Stage,
    stage_started: Instant,
    spawned_clients: usize,
    /// Streams per phase that are allowed to send (index < senders).
    senders: usize,

    // Cumulative counters over the whole run. With `--role clients`,
    // `bytes_received` is the server's last reported total.
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

    // `--role server|clients` only.
    control: Option<ControlLink>,
    control_listener: Option<TcpListener>,
    /// The phase announced by the last `begin` (`--role server`).
    remote_phase: Option<Phase>,
    /// Last `bytes_received` sent to the clients (`--role server`).
    reported_received: Option<u64>,

    frame_start: Instant,
    results: Vec<PhaseResult>,
    run_start: Instant,
}

impl StressBench {
    fn new(cfg: Config, threads: ThreadPlan) -> Self {
        let phases = cfg.phases();
        let now = Instant::now();
        let stage = match cfg.role {
            Role::Clients => Stage::Connect,
            Role::Both | Role::Server => Stage::Ramp,
        };
        Self {
            cfg,
            threads,
            phases,
            phase_idx: 0,
            stage,
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
            control: None,
            control_listener: None,
            remote_phase: None,
            reported_received: None,
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

    fn send_control(&mut self, line: impl std::fmt::Display) {
        if let Some(link) = &mut self.control {
            link.send(line);
        }
    }

    /// Starts the measure window.
    fn start_window(&mut self, runtime: &QuicRuntime) {
        self.frame_work_ms.clear();
        self.frame_interval_ms.clear();
        self.max_global_queue = 0;
        self.window = Some(WindowStart {
            at: Instant::now(),
            cpu: process_cpu_time(),
            tokio_busy: tokio_busy(runtime),
            remote_schedules: remote_schedules(runtime),
            bytes_sent: self.bytes_sent,
            bytes_received: self.bytes_received,
            send_full: self.send_full,
        });
        self.enter(Stage::Measure);
    }

    /// Ends the measure window, keeping the result until the drain is over.
    fn finish_window(&mut self, runtime: &QuicRuntime, phase: Phase) {
        let start = self.window.take().expect("measure window missing");
        let mut work = std::mem::take(&mut self.frame_work_ms);
        let mut interval = std::mem::take(&mut self.frame_interval_ms);

        self.pending = Some(PhaseResult {
            phase,
            senders: self.senders,
            streams_ready: self.streams_ready,
            wall: start.at.elapsed(),
            frames: work.len(),
            frame_avg_ms: mean(&work),
            frame_p99_ms: percentile(&mut work, 0.99),
            frame_max_ms: percentile(&mut work, 1.0),
            interval_p99_ms: percentile(&mut interval, 0.99),
            cpu: process_cpu_time().saturating_sub(start.cpu),
            tokio_busy: tokio_busy(runtime).saturating_sub(start.tokio_busy),
            tokio_workers: backend::tokio_handle(runtime).metrics().num_workers(),
            remote_schedules: remote_schedules(runtime)
                .zip(start.remote_schedules)
                .map(|(end, start)| end - start),
            max_global_queue: self.max_global_queue,
            sent: self.bytes_sent - start.bytes_sent,
            received: self.bytes_received.saturating_sub(start.bytes_received),
            send_full: self.send_full - start.send_full,
            lost: 0,
        });
        self.enter(Stage::Drain);
    }

    /// Completes the pending phase with the bytes lost after the drain.
    fn complete_phase(&mut self, lost: i64) {
        let mut result = self.pending.take().expect("pending result missing");
        result.lost = lost;

        if lost != 0 {
            warn!(
                "Byte mismatch after drain: sent {} / received {} (Δ {lost})",
                self.bytes_sent, self.bytes_received
            );
        }
        info!("{}", summary_line(&result));
        self.results.push(result);
    }
}

/// Counter snapshot at the start of a measure window.
struct WindowStart {
    at: Instant,
    cpu: Duration,
    tokio_busy: Duration,
    remote_schedules: Option<u64>,
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
    /// Tasks scheduled from outside the Tokio workers during the window.
    /// Only available with `--cfg tokio_unstable`.
    remote_schedules: Option<u64>,
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

    // Planned before pinning, which would shrink `available_parallelism`.
    let threads = ThreadPlan::new(&cfg);
    if let Some((_, bevy)) = &threads.pinned {
        // The main thread runs the schedule. Threads it spawns inherit this
        // until their own hooks re-pin them.
        pin_or_warn(bevy);
    }

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
                .set(threads.task_pool_plugin()),
        );
    } else {
        app.add_plugins((
            MinimalPlugins
                .set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                    1.0 / RUNNER_HZ,
                )))
                .set(threads.task_pool_plugin()),
            log,
        ));
    }

    let role = cfg.role;
    app.add_plugins(QuicDefaultPlugins.set(threads.driver_plugin()))
        .insert_resource(StressBench::new(cfg, threads))
        .add_systems(Startup, setup)
        .add_systems(First, frame_begin)
        .add_systems(Last, frame_end);

    match role {
        Role::Both => app
            .add_systems(Update, (drive, spawn_clients, open_streams).chain())
            .add_systems(FixedPostUpdate, (client_send, server_recv)),
        Role::Server => app
            .add_systems(Update, serve_control)
            .add_systems(FixedPostUpdate, server_recv),
        Role::Clients => app
            .add_systems(
                Update,
                (client_control, drive, spawn_clients, open_streams).chain(),
            )
            .add_systems(FixedPostUpdate, client_send),
    };

    app.run()
}

// ─── Setup ───────────────────────────────────────────────────────────────────

fn setup(
    mut commands: Commands,
    runtime: Res<QuicRuntime>,
    mut bench: ResMut<StressBench>,
) {
    let cfg = bench.cfg.clone();

    if cfg.runs_server() {
        let manifest = env!("CARGO_MANIFEST_DIR");
        let config = QuicServerConfig::from_pem_files(
            format!("{manifest}/examples/certs/cert.pem"),
            format!("{manifest}/examples/certs/key.pem"),
        )
        .expect("Failed to load the server certificate");

        commands.spawn(
            QuicServer::bind(&runtime, cfg.addr, &config).expect("Failed to bind server"),
        );
    }

    if cfg.role == Role::Server {
        let listener =
            TcpListener::bind(cfg.control).expect("Failed to bind control address");
        listener
            .set_nonblocking(true)
            .expect("Failed to make the control listener non-blocking");
        bench.control_listener = Some(listener);
    }

    let mode = if cfg.windowed { "windowed" } else { "headless" };
    if cfg.role == Role::Server {
        info!(
            "=== QUIC Concurrency Stress Test (server) ===\n\
             Mode:      {mode}  |  QUIC {}  |  control {}\n\
             Threads:   {}\n\
             Waiting for `--role clients` to connect...",
            cfg.addr,
            cfg.control,
            bench.threads.describe(),
        );
        return;
    }

    info!(
        "=== QUIC Concurrency Stress Test ({}) ===\n\
         Mode:      {mode}  |  QUIC {}\n\
         Clients:   {:?}  |  Scenarios: {:?}\n\
         Phase:     warmup {:?}, measure {:?}, drain ≤{:?}\n\
         Payload:   {} B × {} per sender per fixed tick  |  idle fraction {}\n\
         Threads:   {}",
        cfg.role.name(),
        cfg.addr,
        cfg.client_counts,
        cfg.scenarios.iter().map(|s| s.name()).collect::<Vec<_>>(),
        cfg.warmup,
        cfg.duration,
        cfg.drain,
        cfg.payload.len(),
        cfg.burst,
        cfg.idle_fraction,
        bench.threads.describe(),
    );
}

// ─── Frame timing ────────────────────────────────────────────────────────────

fn frame_begin(mut bench: ResMut<StressBench>) {
    bench.frame_start = Instant::now();
}

fn frame_end(time: Res<Time>, runtime: Res<QuicRuntime>, mut bench: ResMut<StressBench>) {
    if bench.stage != Stage::Measure {
        return;
    }

    let work = bench.frame_start.elapsed().as_secs_f64() * 1000.0;
    bench.frame_work_ms.push(work);
    bench.frame_interval_ms.push(time.delta_secs_f64() * 1000.0);

    let depth = backend::tokio_handle(&runtime)
        .metrics()
        .global_queue_depth();
    bench.max_global_queue = bench.max_global_queue.max(depth);
}

// ─── Stage machine (--role both|clients) ─────────────────────────────────────

fn drive(
    runtime: Res<QuicRuntime>,
    mut bench: ResMut<StressBench>,
    ready_streams: Query<(), (With<StressClientStream>, With<QuicSendStream>)>,
    mut exit: MessageWriter<AppExit>,
    mut last_attempt: Local<Option<Instant>>,
) {
    let elapsed = bench.stage_started.elapsed();

    match bench.stage {
        Stage::Connect => {
            if last_attempt.is_some_and(|t| t.elapsed() < CONTROL_RETRY) {
                return;
            }
            *last_attempt = Some(Instant::now());

            let control = bench.cfg.control;
            match TcpStream::connect_timeout(&control, CONTROL_RETRY)
                .and_then(ControlLink::new)
            {
                Ok(link) => {
                    info!("Connected to the server's control link at {control}");
                    bench.control = Some(link);
                    bench.enter(Stage::Ramp);
                }
                Err(e) if elapsed >= bench.cfg.ready_timeout => {
                    error!("Can't reach the server's control link at {control}: {e}");
                    exit.write(AppExit::error());
                    bench.enter(Stage::Done);
                }
                Err(_) => {}
            }
        }

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
                bench.start_window(&runtime);

                let phase = bench.phase();
                let (senders, ready) = (bench.senders, bench.streams_ready);
                bench.send_control(format_args!(
                    "begin {} {} {senders} {ready}",
                    phase.clients,
                    phase.scenario.name(),
                ));
            }
        }

        Stage::Measure => {
            if elapsed >= bench.cfg.duration {
                let phase = bench.phase();
                bench.finish_window(&runtime, phase);

                let window_sent = bench.pending.as_ref().map_or(0, |r| r.sent);
                bench.send_control(format_args!("end {window_sent}"));
            }
        }

        Stage::Drain => {
            let drained = bench.bytes_received >= bench.bytes_sent;

            if drained || elapsed >= bench.cfg.drain {
                let lost = bench.bytes_sent as i64 - bench.bytes_received as i64;
                bench.complete_phase(lost);

                let sent = bench.bytes_sent;
                bench.send_control(format_args!("finish {sent} {lost}"));

                if bench.phase_idx + 1 < bench.phases.len() {
                    bench.phase_idx += 1;
                    bench.enter(Stage::Ramp);
                } else {
                    bench.enter(Stage::Done);
                    if let Some(link) = &mut bench.control {
                        link.send("quit");
                        if let Err(e) = link.flush_blocking() {
                            error!("Failed to tell the server the run is over: {e}");
                        }
                    }
                    let ok = report(&bench);
                    exit.write(if ok {
                        AppExit::Success
                    } else {
                        AppExit::error()
                    });
                }
            }
        }

        Stage::Done => {}
    }
}

/// `--role clients`: picks up the server's received byte count.
fn client_control(mut bench: ResMut<StressBench>, mut exit: MessageWriter<AppExit>) {
    let Some(link) = &mut bench.control else {
        return;
    };

    let mut received = None;
    for line in link.poll() {
        match line.split_once(' ') {
            Some(("recv", n)) => match n.parse::<u64>() {
                Ok(n) => received = Some(n),
                Err(e) => warn!("Bad control message {line:?}: {e}"),
            },
            _ => warn!("Unknown control message {line:?}"),
        }
    }
    let closed = link.closed.clone();

    if let Some(n) = received {
        bench.bytes_received = n;
    }
    if let Some(reason) = closed
        && bench.stage != Stage::Done
    {
        error!("Lost the server's control link: {reason}");
        bench.enter(Stage::Done);
        exit.write(AppExit::error());
    }
}

// ─── Server control (--role server) ──────────────────────────────────────────

fn serve_control(
    runtime: Res<QuicRuntime>,
    mut bench: ResMut<StressBench>,
    mut exit: MessageWriter<AppExit>,
) {
    if bench.stage == Stage::Done {
        return;
    }

    let Some(mut link) = bench.control.take() else {
        let Some(listener) = &bench.control_listener else {
            return;
        };
        match listener.accept() {
            Ok((stream, peer)) => match ControlLink::new(stream) {
                Ok(link) => {
                    info!("Clients connected from {peer}");
                    bench.control = Some(link);
                    bench.run_start = Instant::now();
                }
                Err(e) => error!("Failed to set up the control link: {e}"),
            },
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => error!("Control accept failed: {e}"),
        }
        return;
    };

    for line in link.poll() {
        if let Err(e) = handle_control(&line, &runtime, &mut bench) {
            warn!("Bad control message {line:?}: {e}");
        }
        if bench.stage == Stage::Done {
            break;
        }
    }

    if bench.stage == Stage::Done {
        let ok = report(&bench);
        exit.write(if ok {
            AppExit::Success
        } else {
            AppExit::error()
        });
        return;
    }

    if bench.reported_received != Some(bench.bytes_received) {
        link.send(format_args!("recv {}", bench.bytes_received));
        bench.reported_received = Some(bench.bytes_received);
    }

    if let Some(reason) = &link.closed {
        error!("Lost the clients' control link: {reason}");
        bench.enter(Stage::Done);
        report(&bench);
        exit.write(AppExit::error());
        return;
    }

    bench.control = Some(link);
}

fn handle_control(
    line: &str,
    runtime: &QuicRuntime,
    bench: &mut StressBench,
) -> Result<(), String> {
    let mut words = line.split_whitespace();
    let mut next = |what: &str| words.next().ok_or_else(|| format!("missing {what}"));
    let number = |s: &str| s.parse::<u64>().map_err(|e| e.to_string());

    match next("command")? {
        "begin" => {
            let clients = number(next("clients")?)? as usize;
            let scenario_name = next("scenario")?;
            let scenario = Scenario::parse(scenario_name)
                .ok_or_else(|| format!("unknown scenario {scenario_name}"))?;
            bench.senders = number(next("senders")?)? as usize;
            bench.streams_ready = number(next("ready")?)? as usize;

            let phase = Phase { clients, scenario };
            info!(
                "{} clients, {} ({} sending): measuring",
                phase.clients,
                phase.scenario.name(),
                bench.senders
            );
            bench.remote_phase = Some(phase);
            bench.start_window(runtime);
        }
        "end" => {
            if bench.stage != Stage::Measure {
                return Err("not measuring".into());
            }
            let window_sent = number(next("bytes")?)?;
            let phase = bench.remote_phase.ok_or("no phase")?;
            bench.finish_window(runtime, phase);
            if let Some(result) = &mut bench.pending {
                result.sent = window_sent;
            }
        }
        "finish" => {
            if bench.stage != Stage::Drain {
                return Err("not draining".into());
            }
            bench.bytes_sent = number(next("bytes")?)?;
            let lost = next("lost")?.parse::<i64>().map_err(|e| e.to_string())?;
            bench.complete_phase(lost);
            bench.enter(Stage::Ramp);
        }
        "quit" => bench.enter(Stage::Done),
        other => return Err(format!("unknown command {other}")),
    }
    Ok(())
}

// ─── Client spawning ─────────────────────────────────────────────────────────

fn spawn_clients(
    mut commands: Commands,
    runtime: Res<QuicRuntime>,
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
    let config = match QuicClientConfig::new()
        .with_root_pem_file(format!("{manifest}/examples/certs/cert.pem"))
    {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load the client certificate: {e}");
            return;
        }
    };
    let addr = bench.cfg.addr;

    for _ in 0..to_spawn {
        let index = bench.spawned_clients;
        // Count failures too, so a bad client cannot stall the ramp forever;
        // the missing stream shows up in `streams_ready`.
        bench.spawned_clients += 1;

        let mut client = match QuicClient::new(&runtime, &config) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create client: {e}");
                continue;
            }
        };
        let conn =
            client.open_connection(Connect::new(addr).with_server_name("localhost"));
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
    let burst = bench.cfg.burst;
    let mut sent = 0u64;
    let mut full = 0u64;

    for (mut stream, meta) in &mut streams {
        if meta.index >= senders || !stream.is_open() {
            continue;
        }
        for _ in 0..burst {
            match stream.send(payload.clone()) {
                Ok(()) => sent += payload.len() as u64,
                Err(_) => full += 1,
            }
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
// `recv_many` never blocks, so silent streams (e.g. the idle scenario) cost
// only an empty channel check.

const SERVER_RECV_BATCH: usize = 256;

fn server_recv(
    mut streams: Query<&mut QuicReceiveStream, With<QuicServerMarker>>,
    mut bench: ResMut<StressBench>,
    mut buffer: Local<Vec<aeronet_io::packet::RecvPacket>>,
) {
    let mut received = 0u64;

    // Closed streams are still drained so buffered data is not miscounted.
    for mut stream in &mut streams {
        let mut any = false;
        while stream.recv_many(&mut buffer, SERVER_RECV_BATCH) > 0 {
            received += buffer
                .drain(..)
                .map(|packet| packet.payload.len() as u64)
                .sum::<u64>();
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
    (0..size)
        .map(|i| b'A' + (i % 26) as u8)
        .collect::<Vec<_>>()
        .into()
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
fn tokio_busy(runtime: &QuicRuntime) -> Duration {
    let metrics = backend::tokio_handle(runtime).metrics();
    (0..metrics.num_workers())
        .map(|w| metrics.worker_total_busy_duration(w))
        .sum()
}

/// Tasks scheduled onto the runtime from outside its worker threads, mostly
/// wakes from the Bevy thread. Needs `RUSTFLAGS="--cfg tokio_unstable"`.
fn remote_schedules(runtime: &QuicRuntime) -> Option<u64> {
    #[cfg(tokio_unstable)]
    return Some(
        backend::tokio_handle(runtime)
            .metrics()
            .remote_schedule_count(),
    );

    #[cfg(not(tokio_unstable))]
    {
        let _ = runtime;
        None
    }
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
         cpu {:.2} cores | tokio busy {:.0}% | max gq {} | rx {}/s | lost {} B{}{}",
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
        r.remote_schedules
            .map(|n| format!(
                " | remote wakes {:.0}/frame",
                n as f64 / r.frames.max(1) as f64
            ))
            .unwrap_or_default(),
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

    let _ = writeln!(
        md,
        "### quic_concurrency_stress ({}) — {}",
        cfg.role.name(),
        git_describe()
    );
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "- Mode: {} | role: {} | logical CPUs: {} | runner: {RUNNER_HZ} Hz",
        if cfg.windowed { "windowed" } else { "headless" },
        cfg.role.name(),
        bench.threads.cpus,
    );
    let _ = writeln!(md, "- Threads: {}", bench.threads.describe());
    if cfg.role != Role::Server {
        let _ = writeln!(
            md,
            "- Per phase: warmup {:?}, measure {:?}, drain ≤{:?} | payload {} B × {}/tick | idle fraction {}",
            cfg.warmup,
            cfg.duration,
            cfg.drain,
            cfg.payload.len(),
            cfg.burst,
            cfg.idle_fraction,
        );
    }
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

    let ok = !bench.results.is_empty() && bench.results.iter().all(PhaseResult::ok);
    let _ = writeln!(md);

    if bench.results.iter().any(|r| r.remote_schedules.is_some()) {
        let _ = writeln!(md);
        let _ = writeln!(md, "| clients | scenario | remote schedules | per frame |");
        let _ = writeln!(md, "|---:|---|---:|---:|");
        for r in &bench.results {
            let n = r.remote_schedules.unwrap_or(0);
            let _ = writeln!(
                md,
                "| {} | {} | {} | {:.0} |",
                r.phase.clients,
                r.phase.scenario.name(),
                n,
                n as f64 / r.frames.max(1) as f64,
            );
        }
    }

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
