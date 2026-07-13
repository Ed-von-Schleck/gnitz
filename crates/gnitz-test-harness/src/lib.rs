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
    /// mTLS client cert/key PEM paths (minted by `start_mtls`), for
    /// [`Self::mtls_target`]. `None` for non-mTLS servers.
    mtls_client: Option<(PathBuf, PathBuf)>,
    /// `--tls-client-ca` PEM path, if this server requires mTLS. Stored so
    /// [`Self::restart`] replays the flag on respawn.
    client_ca: Option<PathBuf>,
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
        Self::start_inner(workers, extra_env, None, false)
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
        Self::start_inner(workers, extra_env, Some("127.0.0.1:0"), false)
    }

    /// [`Self::start_tls`] on the IPv6 loopback (`[::1]:0`) — exercises
    /// bracketed-target parsing and the dev cert's `::1` IP SAN.
    pub fn start_tls_v6(workers: usize) -> Option<Self> {
        Self::start_inner(workers, &[], Some("[::1]:0"), false)
    }

    /// Start with a TLS listener requiring **mTLS**: a client CA and a
    /// CA-signed leaf are minted, the server boots with
    /// `--tls-client-ca=<ca.pem>`, and [`Self::mtls_target`] presents the
    /// leaf. Ephemeral loopback port.
    pub fn start_mtls(workers: usize) -> Option<Self> {
        Self::start_mtls_with_env(workers, &[])
    }

    /// [`Self::start_mtls`] with extra server-side environment variables.
    pub fn start_mtls_with_env(workers: usize, extra_env: &[(&str, &str)]) -> Option<Self> {
        Self::start_inner(workers, extra_env, Some("127.0.0.1:0"), true)
    }

    /// Spawn a server with a **raw** TLS argv (no client-CA minting), returning
    /// the ready handle on success or the early-exit stderr tail on failure.
    /// For tests that assert on a boot *refusal* (the non-loopback bind guard)
    /// or that need custom TLS flags — an explicit listen address,
    /// `--tls-max-conns=N`, `--allow-unauthenticated`. `None` if the server
    /// binary is absent.
    pub fn try_start_tls(workers: usize, tls_args: &[&str]) -> Option<Result<Self, String>> {
        let scaffold = boot_scaffold()?;
        let owned: Vec<String> = tls_args.iter().map(|s| s.to_string()).collect();
        let has_listen = owned.iter().any(|a| a.starts_with("--tls-listen="));
        Some(
            match spawn_and_wait_ready(
                &scaffold.bin,
                &scaffold.data_dir,
                &scaffold.sock_path,
                &scaffold.stderr_path,
                workers,
                &[],
                &owned,
            ) {
                Ok(process) => Ok(Self::assemble(process, scaffold, workers, has_listen, None, None)),
                // On the failure path `scaffold` (with its tmpdir) drops here, cleaning up.
                Err(msg) => Err(msg),
            },
        )
    }

    /// Spawn a server with a raw TLS argv and wait for it to EXIT during boot
    /// — for asserting a boot *abort* (e.g. the non-loopback bind refusal).
    /// Returns `(exited_zero, stderr_tail)`; panics if the server does NOT
    /// exit within the startup budget (an unexpected successful boot). `None`
    /// if the server binary is absent.
    ///
    /// This does NOT use the readiness probe: `server_create` binds+listens
    /// the AF_UNIX socket BEFORE `setup_tls_listener` runs, so a probe connect
    /// can succeed in the window before a refusal aborts — waiting on the
    /// process itself is race-free.
    pub fn boot_expecting_exit(workers: usize, tls_args: &[&str]) -> Option<(bool, String)> {
        let scaffold = boot_scaffold()?;
        let owned: Vec<String> = tls_args.iter().map(|s| s.to_string()).collect();
        let mut proc = configure_command(
            &scaffold.bin,
            &scaffold.data_dir,
            &scaffold.sock_path,
            &scaffold.stderr_path,
            workers,
            &[],
            &owned,
        )
        .spawn()
        .expect("failed to spawn server");
        match poll_with_backoff(|| proc.try_wait().ok().flatten()) {
            Some(status) => Some((status.success(), read_stderr_tail(&scaffold.stderr_path))),
            None => {
                proc.kill().ok();
                proc.wait().ok();
                panic!(
                    "server did not exit within {STARTUP_TIMEOUT:?}; expected a boot abort\nstderr tail:\n{}",
                    read_stderr_tail(&scaffold.stderr_path)
                );
            }
        }
    }

    fn start_inner(workers: usize, extra_env: &[(&str, &str)], tls_listen: Option<&str>, mtls: bool) -> Option<Self> {
        let scaffold = boot_scaffold()?;

        // Build the TLS argv. mTLS additionally mints a client CA + a
        // CA-signed leaf into the tmpdir and enables `--tls-client-ca`.
        let mut tls_args: Vec<String> = Vec::new();
        let mut mtls_client: Option<(PathBuf, PathBuf)> = None;
        let mut client_ca: Option<PathBuf> = None;
        if let Some(addr) = tls_listen {
            tls_args.push(format!("--tls-listen={addr}"));
            if mtls {
                let (ca, leaf_cert, leaf_key) = mint_client_ca_and_leaf(scaffold.tmpdir.path());
                tls_args.push(format!("--tls-client-ca={}", ca.display()));
                client_ca = Some(ca);
                mtls_client = Some((leaf_cert, leaf_key));
            }
        }

        let process = match spawn_and_wait_ready(
            &scaffold.bin,
            &scaffold.data_dir,
            &scaffold.sock_path,
            &scaffold.stderr_path,
            workers,
            extra_env,
            &tls_args,
        ) {
            Ok(p) => p,
            Err(msg) => {
                let kept = scaffold.tmpdir.keep();
                panic!("{msg}\nartifacts preserved at: {}", kept.display());
            }
        };

        Some(Self::assemble(
            process,
            scaffold,
            workers,
            tls_listen.is_some(),
            mtls_client,
            client_ca,
        ))
    }

    /// Assemble a handle from a spawned child and its [`BootScaffold`] (taking
    /// ownership of the tmpdir), recording the TLS/mTLS metadata the spawn
    /// paths differ on. Called only on a successful spawn.
    fn assemble(
        process: Child,
        scaffold: BootScaffold,
        workers: usize,
        tls: bool,
        mtls_client: Option<(PathBuf, PathBuf)>,
        client_ca: Option<PathBuf>,
    ) -> Self {
        ServerHandle {
            process,
            sock_path: scaffold.sock_path.to_str().unwrap().to_string(),
            stderr_path: scaffold.stderr_path,
            tmpdir: Some(scaffold.tmpdir),
            bin: scaffold.bin,
            data_dir: scaffold.data_dir,
            workers,
            tls,
            mtls_client,
            client_ca,
        }
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

    /// The minted client leaf `(cert_pem, key_pem)` paths for a
    /// [`Self::start_mtls`] server — for building a cross-server "untrusted
    /// client cert" target (one server's leaf against another's CA).
    pub fn mtls_client_cert_key(&self) -> (PathBuf, PathBuf) {
        self.mtls_client
            .clone()
            .expect("mtls_client_cert_key requires a start_mtls server")
    }

    /// `tls://IP:PORT?cert=<leaf>&key=<leafkey>&ca=<server dev cert>` — the
    /// full mTLS target: presents the CA-signed client leaf and verifies the
    /// server's minted dev certificate. Requires a [`Self::start_mtls`] server.
    pub fn mtls_target(&self) -> String {
        let (cert, key) = self
            .mtls_client
            .as_ref()
            .expect("mtls_target requires a start_mtls server");
        format!(
            "tls://{}?cert={}&key={}&ca={}",
            self.tls_endpoint(),
            cert.display(),
            key.display(),
            self.tls_ca_path().display(),
        )
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
        let mut tls_args = vec![format!("--tls-listen={endpoint}")];
        if let Some(ca) = &self.client_ca {
            tls_args.push(format!("--tls-client-ca={}", ca.display()));
        }
        self.process = spawn_and_wait_ready(
            &self.bin,
            &self.data_dir,
            Path::new(&self.sock_path),
            &self.stderr_path,
            self.workers,
            &[],
            &tls_args,
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

/// A resolved server binary + a fresh tmpdir with the standard
/// `data` / socket / stderr layout. Produced once by [`boot_scaffold`] and
/// consumed by [`ServerHandle::assemble`] on a successful spawn (which takes
/// ownership of the tmpdir); otherwise dropped, cleaning up.
struct BootScaffold {
    bin: String,
    tmpdir: TempDir,
    data_dir: PathBuf,
    sock_path: PathBuf,
    stderr_path: PathBuf,
}

/// Resolve the server binary (`None` when absent, so tests skip) and mint a
/// fresh tmpdir with the standard data/socket/stderr layout — the single
/// source of the spawn preamble every `ServerHandle` entry point shares.
fn boot_scaffold() -> Option<BootScaffold> {
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
    Some(BootScaffold {
        bin,
        tmpdir,
        data_dir,
        sock_path,
        stderr_path,
    })
}

/// Mint a self-signed client CA and a leaf certificate signed by it, writing
/// all three PEMs (CA public cert, leaf cert, leaf private key) into `dir`.
/// Returns `(ca_cert, leaf_cert, leaf_key)` paths. The CA has cA=TRUE and no
/// key-usage extension (so webpki imposes no CA key-usage constraint); the
/// leaf has no EKU (still authenticates — the documented residual). Default
/// rcgen validity (1975–4096) never expires.
fn mint_client_ca_and_leaf(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let ca_key = rcgen::KeyPair::generate().expect("mint: ca key");
    let mut ca_params = rcgen::CertificateParams::new(vec!["gnitz-client-ca".to_string()]).expect("mint: ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("mint: self-sign ca");

    let leaf_key = rcgen::KeyPair::generate().expect("mint: leaf key");
    let leaf_params = rcgen::CertificateParams::new(vec!["gnitz-client".to_string()]).expect("mint: leaf params");
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer).expect("mint: sign leaf");

    let ca_path = dir.join("client_ca.pem");
    let leaf_cert_path = dir.join("client_leaf.pem");
    let leaf_key_path = dir.join("client_leaf_key.pem");
    fs::write(&ca_path, ca_cert.pem()).expect("mint: write ca pem");
    fs::write(&leaf_cert_path, leaf_cert.pem()).expect("mint: write leaf pem");
    fs::write(&leaf_key_path, leaf_key.serialize_pem()).expect("mint: write leaf key pem");
    (ca_path, leaf_cert_path, leaf_key_path)
}

/// Build (but do not spawn) the server `Command`: positional args, captured
/// stderr, the small-SAL cap, extra env, worker count, and TLS argv. Shared by
/// [`spawn_and_wait_ready`] (readiness probe) and [`ServerHandle::boot_expecting_exit`]
/// (wait-for-exit).
#[allow(clippy::too_many_arguments)]
fn configure_command(
    bin: &str,
    data_dir: &Path,
    sock_path: &Path,
    stderr_path: &Path,
    workers: usize,
    extra_env: &[(&str, &str)],
    tls_args: &[String],
) -> Command {
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
    for tls in tls_args {
        cmd.arg(tls);
    }
    cmd
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
    tls_args: &[String],
) -> Result<Child, String> {
    let mut proc = configure_command(bin, data_dir, sock_path, stderr_path, workers, extra_env, tls_args)
        .spawn()
        .expect("failed to spawn server");

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
