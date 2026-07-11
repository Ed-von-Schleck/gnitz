//! Spawns a `gnitz-server` subprocess for integration tests, tied to a
//! private tmpdir. On a failing test the server's stderr tail is printed
//! and the tmpdir is preserved for post-mortem; on a clean pass the
//! tmpdir is removed.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use tempfile::TempDir;

/// Wall-clock budget for the server to create its listening socket.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound on the readiness poll interval; the poll backs off up to this.
const POLL_MAX: Duration = Duration::from_millis(50);

pub struct ServerHandle {
    process: Child,
    pub sock_path: String,
    stderr_path: PathBuf,
    tmpdir: Option<TempDir>,
    /// Restart inputs (server binary, data dir, worker count); TLS handles
    /// use them to respawn on the same port and data directory.
    bin: String,
    data_dir: PathBuf,
    workers: usize,
    tls: bool,
}

impl ServerHandle {
    pub fn start() -> Option<Self> {
        Self::start_n(1)
    }

    pub fn start_n(workers: usize) -> Option<Self> {
        Self::start_with_env(workers, &[])
    }

    /// Like [`Self::start_n`], but sets extra environment variables on the
    /// server process only (not the test's own process env), so a test that
    /// needs a server-side seam — e.g. `GNITZ_CLIENT_SEND_TIMEOUT_MS` — cannot
    /// race a sibling test through the process-global env under parallel
    /// `cargo test`.
    pub fn start_with_env(workers: usize, extra_env: &[(&str, &str)]) -> Option<Self> {
        Self::start_inner(workers, extra_env, None)
    }

    /// Start with a TLS listener on `127.0.0.1:0` (ephemeral port — safe
    /// under parallel `cargo test`). Opt-in: the default `start*`
    /// constructors stay TLS-free so the ~270 AF_UNIX-only tests don't pay
    /// per-boot cert minting + a TCP bind for zero coverage. Resolve the
    /// bound address with [`Self::tls_target`].
    pub fn start_tls(workers: usize) -> Option<Self> {
        Self::start_tls_with_env(workers, &[])
    }

    /// [`Self::start_tls`] with extra server-side environment variables.
    pub fn start_tls_with_env(workers: usize, extra_env: &[(&str, &str)]) -> Option<Self> {
        Self::start_inner(workers, extra_env, Some("127.0.0.1:0"))
    }

    /// [`Self::start_tls`] on the IPv6 loopback (`[::1]:0`) — exercises
    /// bracketed-target parsing and the dev cert's `::1` IP SAN.
    pub fn start_tls_v6(workers: usize) -> Option<Self> {
        Self::start_inner(workers, &[], Some("[::1]:0"))
    }

    fn start_inner(workers: usize, extra_env: &[(&str, &str)], tls_listen: Option<&str>) -> Option<Self> {
        let bin = env::var("GNITZ_SERVER_BIN")
            .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/../../gnitz-server").to_string());
        if !PathBuf::from(&bin).is_file() {
            return None;
        }

        let tmpdir = tempfile::Builder::new()
            .prefix("gnitz_test_")
            .tempdir()
            .expect("failed to create tempdir");
        let data_dir = tmpdir.path().join("data");
        let sock_path = tmpdir.path().join("gnitz.sock");
        let stderr_path = tmpdir.path().join("server_stderr.log");

        let tls_arg = tls_listen.map(|addr| format!("--tls-listen={addr}"));
        let process = match spawn_and_wait_ready(
            &bin,
            &data_dir,
            Path::new(&sock_path),
            &stderr_path,
            workers,
            extra_env,
            tls_arg.as_deref(),
        ) {
            Ok(p) => p,
            Err(msg) => {
                let kept = tmpdir.keep();
                panic!("{msg}\nartifacts preserved at: {}", kept.display());
            }
        };

        Some(ServerHandle {
            process,
            sock_path: sock_path.to_str().unwrap().to_string(),
            stderr_path,
            tmpdir: Some(tmpdir),
            bin,
            data_dir,
            workers,
            tls: tls_listen.is_some(),
        })
    }

    /// The bound TLS endpoint (`IP:PORT`), read from the atomically-published
    /// `<data_dir>/tls_endpoint` (rename makes existence imply complete
    /// content). Polls with the same backoff as the socket wait — the file
    /// is written between the AF_UNIX `listen()` and "GnitzDB ready".
    fn tls_endpoint(&self) -> String {
        assert!(self.tls, "tls_endpoint requires a start_tls server");
        let path = self.data_dir.join("tls_endpoint");
        let endpoint = poll_with_backoff(|| {
            path.exists().then(|| {
                fs::read_to_string(&path)
                    .expect("tls_endpoint unreadable")
                    .trim()
                    .to_string()
            })
        });
        endpoint.unwrap_or_else(|| panic!("server did not publish {} within {STARTUP_TIMEOUT:?}", path.display()))
    }

    /// `tls://127.0.0.1:PORT?insecure` for the bound listener.
    pub fn tls_target(&self) -> String {
        format!("tls://{}?insecure", self.tls_endpoint())
    }

    /// Path of the server's minted dev certificate (public PEM).
    pub fn tls_ca_path(&self) -> PathBuf {
        self.data_dir.join("tls_dev_cert.pem")
    }

    /// `tls://IP:PORT?ca=<data_dir>/tls_dev_cert.pem` — verifies the
    /// server's minted dev certificate.
    pub fn tls_ca_target(&self) -> String {
        format!("tls://{}?ca={}", self.tls_endpoint(), self.tls_ca_path().display())
    }

    /// Kill the server and respawn it on the same data dir, same socket
    /// path, and the SAME TLS port (read back from `tls_endpoint` before the
    /// kill), so clients holding the old target can observe fail-fast errors
    /// and then reconnect.
    pub fn restart(&mut self) {
        let endpoint = self.tls_endpoint();
        self.process.kill().ok();
        self.process.wait().ok();
        // Remove the stale endpoint file so tls_endpoint() polling observes
        // the NEW boot's publish (same content, but existence must imply the
        // new listener is bound).
        fs::remove_file(self.data_dir.join("tls_endpoint")).ok();
        let tls_arg = format!("--tls-listen={endpoint}");
        self.process = spawn_and_wait_ready(
            &self.bin,
            &self.data_dir,
            Path::new(&self.sock_path),
            &self.stderr_path,
            self.workers,
            &[],
            Some(&tls_arg),
        )
        // ServerHandle::Drop preserves the tmpdir during the unwind.
        .unwrap_or_else(|msg| panic!("restart failed: {msg}"));
        // The AF_UNIX probe above proves `listen()` is live, but the TLS
        // bind (and its endpoint publish) happens slightly later in boot —
        // block until the new listener is up so a caller's immediate
        // reconnect cannot race it.
        let republished = self.tls_endpoint();
        assert_eq!(republished, endpoint, "restart must rebind the same TLS endpoint");
    }
}

/// Spawn the server and block until a probe connect on the AF_UNIX socket
/// succeeds. `Err` (with the stderr tail embedded) on early exit or timeout;
/// the caller decides how to preserve artifacts.
#[allow(clippy::too_many_arguments)]
fn spawn_and_wait_ready(
    bin: &str,
    data_dir: &Path,
    sock_path: &Path,
    stderr_path: &Path,
    workers: usize,
    extra_env: &[(&str, &str)],
    tls_arg: Option<&str>,
) -> Result<Child, String> {
    // Append on restart so the first boot's stderr survives for post-mortem.
    let stderr_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(stderr_path)
        .expect("failed to open server stderr log file");

    let mut cmd = Command::new(bin);
    cmd.arg(data_dir)
        .arg(sock_path)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file)); // captured for post-mortem

    // Each server eagerly fallocate+pre-faults its whole SAL at startup, so the
    // 1 GiB production default would let a parallel `cargo test` run (one server
    // per test) exhaust the shared tmpfs data_dir and take a SIGBUS on the
    // forced page-fault. Integration tests are functional (tiny writes), so cap
    // the SAL small unless the caller already pinned a size.
    if env::var_os("GNITZ_SAL_BYTES").is_none() {
        cmd.env("GNITZ_SAL_BYTES", "134217728"); // 128 MiB
    }
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    if workers > 1 {
        cmd.arg(format!("--workers={workers}"));
    }
    if let Some(tls) = tls_arg {
        cmd.arg(tls);
    }
    let mut proc = cmd.spawn().expect("failed to spawn server");

    // Readiness is "a client connect succeeds", NOT "the socket file
    // exists". `server_create` creates the AF_UNIX file at `bind()` but
    // only accepts connections after the later `listen()`; a client that
    // races into that window (widened by CPU starvation when dozens of
    // servers boot at once under a parallel `cargo test`) gets
    // ECONNREFUSED, and the test's real `connect().unwrap()` then flakes.
    // A successful probe connect proves `listen()` is live — every
    // subsequent connect then queues in the backlog and succeeds. The
    // probe stream is dropped immediately; the server treats the pre-HELLO
    // EOF as a benign client disconnect.
    let ready = poll_with_backoff(|| {
        if sock_path.exists() && std::os::unix::net::UnixStream::connect(sock_path).is_ok() {
            return Some(Ok(()));
        }
        if let Ok(Some(status)) = proc.try_wait() {
            let tail = read_stderr_tail(stderr_path);
            return Some(Err(format!("server exited early ({status})\nstderr tail:\n{tail}")));
        }
        None
    });
    match ready {
        Some(Ok(())) => Ok(proc),
        Some(Err(msg)) => Err(msg),
        None => {
            proc.kill().ok();
            proc.wait().ok();
            let tail = read_stderr_tail(stderr_path);
            Err(format!(
                "server did not accept a connection within {STARTUP_TIMEOUT:?}\nstderr tail:\n{tail}"
            ))
        }
    }
}

/// Poll `step` under the shared startup timing (exponential backoff up to
/// `POLL_MAX`, bounded by `STARTUP_TIMEOUT`): `Some(v)` as soon as `step`
/// produces one, `None` on timeout.
fn poll_with_backoff<T>(mut step: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let mut backoff = Duration::from_millis(1);
    loop {
        if let Some(v) = step() {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(POLL_MAX);
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        // Sample the exit status *before* we kill, so our own SIGKILL doesn't
        // mask a real crash. Only kill if the server is still running.
        let crashed = match self.process.try_wait() {
            Ok(Some(status)) => !status.success(),
            Ok(None) => {
                self.process.kill().ok();
                self.process.wait().ok();
                false
            }
            Err(_) => false,
        };

        let tmpdir = self.tmpdir.take();
        if std::thread::panicking() || crashed {
            let tail = read_stderr_tail(&self.stderr_path);
            eprintln!("\n──── server stderr (last 100 lines) ────");
            eprintln!("{tail}");
            eprintln!("──── end server stderr ────");
            if let Some(dir) = tmpdir {
                eprintln!("server artifacts preserved at: {}", dir.keep().display());
            }
        }
        // Clean pass: dropping the TempDir removes the directory.
    }
}

/// Last ~100 lines of `path`, bounded to the final 128 KiB.
/// Lossy UTF-8 so a binary-garbage tail from a hard crash still prints.
/// Returns an empty string if the file is missing or unreadable.
fn read_stderr_tail(path: &Path) -> String {
    use std::io::{Read, Seek, SeekFrom};
    const MAX_TAIL_BYTES: u64 = 128 * 1024;
    const MAX_TAIL_LINES: usize = 100;

    let Ok(mut f) = fs::File::open(path) else {
        return String::new();
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let offset = len.saturating_sub(MAX_TAIL_BYTES);
    if offset > 0 {
        let _ = f.seek(SeekFrom::Start(offset));
    }
    let mut buf = Vec::new();
    if f.read_to_end(&mut buf).is_err() {
        return String::new();
    }
    let text = String::from_utf8_lossy(&buf);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(MAX_TAIL_LINES);
    lines[start..].join("\n")
}
