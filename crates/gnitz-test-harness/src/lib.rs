//! Spawns a `gnitz-server` subprocess for integration tests, tied to a
//! private tmpdir. On a failing test the server's stderr tail is printed
//! and the tmpdir is preserved for post-mortem; on a clean pass the
//! tmpdir is removed.
//!
//! A server dies with the thread that started it, so no server outlives a
//! test process that is killed rather than unwound.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use tempfile::TempDir;

/// Wall-clock budget for the server to log its readiness marker.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the boot is checked for readiness or an early exit.
const POLL_INTERVAL: Duration = Duration::from_millis(2);
/// What the server logs once every listener is bound and published.
const READY_MARKER: &str = "GnitzDB ready";

/// Server binary plus the tmpdir's data/socket/stderr paths: what a boot and a
/// restart both spawn from.
struct BootPaths {
    bin: String,
    data_dir: PathBuf,
    sock_path: PathBuf,
    stderr_path: PathBuf,
}

pub struct ServerHandle {
    process: Child,
    paths: BootPaths,
    tmpdir: Option<TempDir>,
    workers: usize,
    /// The boot's TLS argv and server-side environment, replayed by
    /// [`Self::restart`].
    args: Vec<String>,
    env: Vec<(String, String)>,
    /// mTLS client cert/key PEM paths (minted by `start_mtls`), for
    /// [`Self::mtls_target`]. `None` for non-mTLS servers.
    mtls_client: Option<(PathBuf, PathBuf)>,
    /// The token that file held at boot, until `start_tls_with_token` hands it out.
    first_token: Option<String>,
}

impl ServerHandle {
    /// The AF_UNIX socket this server is listening on.
    pub fn sock_path(&self) -> &str {
        self.paths.sock_path.to_str().expect("tmpdir path is not UTF-8")
    }

    /// The server's data directory, where its workers' logs are too.
    pub fn data_dir(&self) -> &Path {
        &self.paths.data_dir
    }

    pub fn start() -> Self {
        Self::start_n(1)
    }

    pub fn start_n(workers: usize) -> Self {
        Self::start_with_env(workers, &[])
    }

    /// [`Self::start_n`] with extra environment variables on the server
    /// process alone.
    pub fn start_with_env(workers: usize, extra_env: &[(&str, &str)]) -> Self {
        Self::start_inner(workers, extra_env, None, Auth::None)
    }

    /// Start with a TLS listener on an ephemeral loopback port, reached through
    /// [`Self::tls_target`].
    pub fn start_tls(workers: usize) -> Self {
        Self::start_tls_with_env(workers, &[])
    }

    /// [`Self::start_tls`] with extra server-side environment variables.
    pub fn start_tls_with_env(workers: usize, extra_env: &[(&str, &str)]) -> Self {
        Self::start_inner(workers, extra_env, Some("127.0.0.1:0"), Auth::None)
    }

    /// [`Self::start_tls`] on the IPv6 loopback (`[::1]:0`) — exercises
    /// bracketed-target parsing and the minted cert's `::1` IP SAN.
    pub fn start_tls_v6(workers: usize) -> Self {
        Self::start_inner(workers, &[], Some("[::1]:0"), Auth::None)
    }

    /// [`Self::start_tls`] requiring mTLS against a freshly minted client CA,
    /// whose signed leaf [`Self::mtls_target`] presents.
    pub fn start_mtls(workers: usize) -> Self {
        Self::start_inner(workers, &[], Some("127.0.0.1:0"), Auth::ClientCa)
    }

    /// [`Self::start_tls_with_env`] requiring an access token, and the one its
    /// token file holds at boot.
    pub fn start_tls_with_token(workers: usize, extra_env: &[(&str, &str)]) -> (Self, String) {
        let mut srv = Self::start_inner(workers, extra_env, Some("127.0.0.1:0"), Auth::Token);
        let token = srv.first_token.take().expect("a token server mints its first token");
        (srv, token)
    }

    /// Mint a token into this server's token file with `--new-token`; the
    /// server holds it after [`Self::sighup`].
    pub fn new_token(&self, name: &str) -> String {
        mint_token(&self.paths.bin, self.token_file(), name)
    }

    /// The token file a [`Self::start_tls_with_token`] server reads.
    pub fn token_file(&self) -> &Path {
        self.args
            .iter()
            .find_map(|a| a.strip_prefix("--auth-tokens="))
            .map(Path::new)
            .expect("token_file requires a start_tls_with_token server")
    }

    /// Send the master SIGHUP, which reloads the TLS listener, and wait until
    /// its log reports the reload's outcome. Returns that line.
    pub fn sighup(&mut self) -> String {
        let offset = fs::metadata(&self.paths.stderr_path)
            .expect("stderr log metadata")
            .len();
        let status = Command::new("kill")
            .args(["-HUP", &self.process.id().to_string()])
            .status()
            .expect("kill -HUP");
        assert!(status.success(), "kill -HUP failed");
        await_log(&mut self.process, &self.paths.stderr_path, offset, |log| {
            log.lines().find(|l| l.contains("SIGHUP:")).map(str::to_string)
        })
        .unwrap_or_else(|e| panic!("the server never reported the reload: {e}"))
    }

    /// Whether the master and every worker it forked are running. A worker's
    /// death stops the master too, but only once its death watch next looks;
    /// a dead worker is a zombie of the master until then.
    pub fn is_alive(&mut self) -> bool {
        if self.process.try_wait().expect("try_wait").is_some() {
            return false;
        }
        let pid = self.process.id();
        // The master's main thread forks the workers. A master gone in between
        // has no children file, and the next look at it answers.
        let children = fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")).unwrap_or_default();
        children.split_whitespace().all(|child| {
            fs::read_to_string(format!("/proc/{child}/stat"))
                .ok()
                .and_then(|stat| stat.rsplit_once(')').map(|(_, rest)| rest.trim_start().to_owned()))
                .is_some_and(|rest| !rest.starts_with('Z'))
        })
    }

    /// A missing binary panics rather than skips: a suite passing without a
    /// server tests nothing.
    fn start_inner(workers: usize, extra_env: &[(&str, &str)], tls_listen: Option<&str>, auth: Auth) -> Self {
        let bin = env::var("GNITZ_SERVER_BIN")
            .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/../../gnitz-server").to_string());
        assert!(
            PathBuf::from(&bin).is_file(),
            "no server binary at {bin}: `make server` builds one (`make test` does it for you)"
        );
        let tmpdir = tempfile::Builder::new()
            .prefix("gnitz_test_")
            .tempdir()
            .expect("failed to create tempdir");
        let paths = BootPaths {
            bin,
            data_dir: tmpdir.path().join("data"),
            sock_path: tmpdir.path().join("gnitz.sock"),
            stderr_path: tmpdir.path().join("server_stderr.log"),
        };
        let env: Vec<(String, String)> = extra_env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

        // Build the TLS argv, minting what the client authentication needs.
        let mut args: Vec<String> = Vec::new();
        let mut mtls_client: Option<(PathBuf, PathBuf)> = None;
        let mut first_token = None;
        if let Some(addr) = tls_listen {
            args.push(format!("--tls-listen={addr}"));
            match auth {
                Auth::None => {}
                Auth::ClientCa => {
                    let (ca, leaf_cert, leaf_key) = mint_client_ca_and_leaf(tmpdir.path());
                    args.push(format!("--tls-client-ca={}", ca.display()));
                    mtls_client = Some((leaf_cert, leaf_key));
                }
                Auth::Token => {
                    let file = tmpdir.path().join("tokens");
                    first_token = Some(mint_token(&paths.bin, &file, "first"));
                    args.push(format!("--auth-tokens={}", file.display()));
                }
            }
        }

        let process = match spawn_and_wait_ready(&paths, workers, &env, &args) {
            Ok(p) => p,
            Err(msg) => {
                let kept = tmpdir.keep();
                panic!("{msg}\nartifacts preserved at: {}", kept.display());
            }
        };

        ServerHandle {
            process,
            paths,
            tmpdir: Some(tmpdir),
            workers,
            args,
            env,
            mtls_client,
            first_token,
        }
    }

    fn is_tls(&self) -> bool {
        self.args.iter().any(|a| a.starts_with("--tls-listen="))
    }

    /// The bound TLS endpoint (`IP:PORT`), read from `<data_dir>/tls_endpoint`,
    /// which the server publishes before it logs readiness.
    pub fn tls_endpoint(&self) -> String {
        assert!(self.is_tls(), "tls_endpoint requires a start_tls server");
        let path = self.paths.data_dir.join("tls_endpoint");
        fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
            .trim()
            .to_string()
    }

    /// `tls://IP:PORT?ca=<data_dir>/tls_cert.pem` for the bound listener —
    /// verifies the server's minted certificate.
    pub fn tls_target(&self) -> String {
        format!("tls://{}?ca={}", self.tls_endpoint(), self.tls_ca_path().display())
    }

    /// Path of the server's minted certificate.
    pub fn tls_ca_path(&self) -> PathBuf {
        self.paths.data_dir.join("tls_cert.pem")
    }

    /// The minted client leaf `(cert_pem, key_pem)` paths for a
    /// [`Self::start_mtls`] server — for building a cross-server "untrusted
    /// client cert" target (one server's leaf against another's CA).
    pub fn mtls_client_cert_key(&self) -> (PathBuf, PathBuf) {
        self.mtls_client
            .clone()
            .expect("mtls_client_cert_key requires a start_mtls server")
    }

    /// [`Self::tls_target`] presenting the CA-signed client leaf. Requires a
    /// [`Self::start_mtls`] server.
    pub fn mtls_target(&self) -> String {
        let (cert, key) = self
            .mtls_client
            .as_ref()
            .expect("mtls_target requires a start_mtls server");
        format!("{}&cert={}&key={}", self.tls_target(), cert.display(), key.display())
    }

    /// [`Self::tls_target`] presenting `token`, which a fresh file in the
    /// server's tmpdir holds.
    pub fn token_target(&self, token: &str) -> String {
        let dir = self.tmpdir.as_ref().expect("a running server's tmpdir").path();
        let path = (0..)
            .map(|i| dir.join(format!("client_token_{i}")))
            .find(|p| !p.exists())
            .expect("an unused name");
        fs::write(&path, format!("{token}\n")).expect("write the client's token file");
        format!("{}&token_file={}", self.tls_target(), path.display())
    }

    /// Kill the server and respawn it on the same data dir and socket path,
    /// with every flag and environment variable of the first boot. A TLS server
    /// rebinds the port it had.
    pub fn restart(&mut self) {
        if self.is_tls() {
            let listen = format!("--tls-listen={}", self.tls_endpoint());
            for a in self.args.iter_mut().filter(|a| a.starts_with("--tls-listen=")) {
                *a = listen.clone();
            }
        }
        self.process.kill().ok();
        self.process.wait().ok();
        self.process = spawn_and_wait_ready(&self.paths, self.workers, &self.env, &self.args)
            // ServerHandle::Drop preserves the tmpdir during the unwind.
            .unwrap_or_else(|msg| panic!("restart failed: {msg}"));
    }
}

/// How a TLS listener authenticates its clients, beyond what a loopback bind needs.
#[derive(Clone, Copy)]
enum Auth {
    None,
    /// `--tls-client-ca`: a client certificate a minted CA signed.
    ClientCa,
    /// `--auth-tokens`: an access token.
    Token,
}

/// Run `bin --new-token file name`, returning the token it prints.
fn mint_token(bin: &str, file: &Path, name: &str) -> String {
    let out = Command::new(bin)
        .arg("--new-token")
        .arg(file)
        .arg(name)
        .output()
        .expect("run --new-token");
    assert!(
        out.status.success(),
        "--new-token failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("a UTF-8 token").trim().to_string()
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

/// Spawn the server and block until it logs [`READY_MARKER`]; `Err` carries the
/// stderr tail of an early exit or a timeout.
fn spawn_and_wait_ready(
    paths: &BootPaths,
    workers: usize,
    extra_env: &[(String, String)],
    args: &[String],
) -> Result<Child, String> {
    // Appended on restart so the first boot's stderr survives for post-mortem;
    // this boot's marker is looked for past what is already there.
    let stderr_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.stderr_path)
        .expect("failed to open server stderr log file");
    let offset = stderr_file.metadata().expect("stderr log metadata").len();

    let mut cmd = Command::new(&paths.bin);
    // SAFETY: `prctl` is async-signal-safe, and the closure allocates nothing.
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| match libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) {
            0 => Ok(()),
            _ => Err(std::io::Error::last_os_error()),
        });
    }
    cmd.arg(&paths.data_dir)
        .arg(&paths.sock_path)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file.try_clone().expect("dup stderr log fd")));

    // Each server fallocates its whole SAL, and a parallel `cargo test` runs one
    // server per test; these tests write little.
    if env::var_os("GNITZ_SAL_BYTES").is_none() {
        cmd.env("GNITZ_SAL_BYTES", "134217728"); // 128 MiB
    }

    // Servers pinning independently would pile onto the same cores.
    if env::var_os("GNITZ_CPU_AFFINITY").is_none() {
        cmd.env("GNITZ_CPU_AFFINITY", "0");
    }
    cmd.envs(extra_env.iter().map(|(k, v)| (k, v)));
    cmd.arg(format!("--workers={workers}"));
    cmd.args(args);
    let mut proc = cmd.spawn().expect("failed to spawn server");

    await_log(&mut proc, &paths.stderr_path, offset, |log| {
        log.contains(READY_MARKER).then_some(())
    })
    .map_err(|e| {
        proc.kill().ok();
        proc.wait().ok();
        format!("server did not log {READY_MARKER:?}: {e}")
    })?;
    Ok(proc)
}

/// Poll the log at `path` past `offset` until `found` finds what it looks for
/// there; `Err` carries the stderr tail of `proc` exiting or of a timeout.
fn await_log<T>(
    proc: &mut Child,
    path: &Path,
    offset: u64,
    mut found: impl FnMut(&str) -> Option<T>,
) -> Result<T, String> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = fs::File::open(path).expect("open server stderr log");
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let mut logged = Vec::new();
    loop {
        if let Ok(Some(status)) = proc.try_wait() {
            return Err(format!(
                "server exited ({status})\nstderr tail:\n{}",
                read_stderr_tail(path)
            ));
        }
        logged.clear();
        file.seek(SeekFrom::Start(offset)).expect("seek stderr log");
        file.read_to_end(&mut logged).expect("read stderr log");
        if let Some(t) = found(&String::from_utf8_lossy(&logged)) {
            return Ok(t);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "nothing within {STARTUP_TIMEOUT:?}\nstderr tail:\n{}",
                read_stderr_tail(path)
            ));
        }
        thread::sleep(POLL_INTERVAL);
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
            let tail = read_stderr_tail(&self.paths.stderr_path);
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

/// A schema name no other test in this process has used: `prefix` plus a
/// process-wide sequence number. Keeps a failure unambiguous when several
/// tests share one server.
pub fn unique_schema(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!("{prefix}{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

// ── Syscall counting ─────────────────────────────────────────────────────────

/// Syscall totals from one `strace -f -c` run, plus the report they were read
/// from, for an assertion to print.
pub struct SyscallCounts {
    calls: std::collections::HashMap<String, usize>,
    pub report: String,
}

impl SyscallCounts {
    /// Calls to `name` across every traced thread.
    pub fn get(&self, name: &str) -> usize {
        self.calls.get(name).copied().unwrap_or(0)
    }

    /// Calls to any of `names` — one operation's spelling varies with the libc
    /// and the socket family, so a count is usually over a set.
    pub fn sum(&self, names: &[&str]) -> usize {
        names.iter().map(|n| self.get(n)).sum()
    }
}

/// Re-run **this** test binary's `child_test` under `strace -f -c`, with `env`
/// set, and total the syscalls it made. The child reads its work out of `env`
/// and no-ops without it, so an ordinary run leaves it inert.
///
/// `None` when `strace` is not installed — the caller skips.
pub fn strace_test(child_test: &str, env: &[(&str, &str)]) -> Option<SyscallCounts> {
    let strace =
        env::var_os("PATH").and_then(|p| env::split_paths(&p).map(|d| d.join("strace")).find(|c| c.is_file()))?;
    let exe = env::current_exe().expect("current_exe");
    // A file rather than the child's stdout, which carries libtest's own report.
    let report = tempfile::NamedTempFile::new().expect("strace report file");
    let mut cmd = Command::new(strace);
    cmd.args(["-f", "-c", "-o"]).arg(report.path()).arg(&exe).args([
        "--exact",
        child_test,
        "--nocapture",
        "--test-threads=1",
    ]);
    for (k, v) in env {
        cmd.env(k, v);
    }
    // A child recording its frames would count the recording's own writes.
    cmd.env_remove("GNITZ_RECORD_FRAMES");
    let out = cmd.output().expect("spawn strace");
    assert!(
        out.status.success(),
        "the traced child failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // libtest exits 0 when its filter matches nothing.
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("1 passed"),
        "`{child_test}` names no test in this binary:\n{stdout}"
    );
    let report = fs::read_to_string(report.path()).expect("read the strace report");
    // A summary row is `% time  seconds  usecs/call  calls [errors] syscall`,
    // so the name is last and the call count is column 3 either way.
    let calls = report
        .lines()
        .filter_map(|l| {
            let c: Vec<&str> = l.split_whitespace().collect();
            let name = c.last()?;
            Some((name.to_string(), c.get(3)?.parse::<usize>().ok()?))
        })
        .collect();
    Some(SyscallCounts { calls, report })
}
