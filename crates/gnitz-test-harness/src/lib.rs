//! Spawns a `gnitz-server` subprocess for integration tests, tied to a
//! private tmpdir. On a failing test the server's stderr tail is printed
//! and the tmpdir is preserved for post-mortem; on a clean pass the
//! tmpdir is removed.
//!
//! A server dies with the thread that started it, so no server outlives a
//! test process that is killed rather than unwound.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use std::{env, fs};

use gnitz_foundation::log::READY_LINE;
use gnitz_foundation::process::{await_log, default_env, die_with_parent, log_tail};
use tempfile::TempDir;

/// Wall-clock budget for the server to log its readiness marker.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Server binary plus the tmpdir's data/socket/stderr paths: what a boot and a
/// restart both spawn from.
struct BootPaths {
    bin: PathBuf,
    data_dir: PathBuf,
    sock_path: PathBuf,
    stderr_path: PathBuf,
}

pub struct ServerHandle {
    process: Child,
    /// A pidfd of every worker the master forked, each readable once its
    /// worker exits.
    workers_alive: Vec<OwnedFd>,
    paths: BootPaths,
    tmpdir: Option<TempDir>,
    workers: usize,
    /// The boot's flags and server-side environment, replayed by
    /// [`Self::restart`].
    flags: Flags,
    env: Vec<(String, String)>,
    /// The token the token file held at boot, until `start_tls_with_token`
    /// hands it out.
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

    /// [`Self::start_with_env`] with `--backup-dir` naming [`Self::backups`], a
    /// directory of its tmpdir.
    pub fn start_with_backups(workers: usize, extra_env: &[(&str, &str)]) -> Self {
        Self::boot(workers, extra_env, None, Auth::None, Boot::WithBackups)
    }

    /// [`Self::start_n`] over the existing data directory `data_dir` — a backup
    /// entry, say — which outlives the handle.
    pub fn start_on(data_dir: &Path, workers: usize) -> Self {
        Self::boot(workers, &[], None, Auth::None, Boot::On(data_dir.to_path_buf()))
    }

    /// The `--backup-dir` of a [`Self::start_with_backups`] server.
    pub fn backups(&self) -> PathBuf {
        self.flags
            .backup_dir
            .clone()
            .expect("backups requires a start_with_backups server")
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
        match self.client_auth() {
            Some(ClientAuth::Token { file }) => file,
            _ => panic!("token_file requires a start_tls_with_token server"),
        }
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
        let path = &self.paths.stderr_path;
        await_log(
            &mut self.process,
            path,
            offset,
            STARTUP_TIMEOUT,
            MAX_TAIL_LINES,
            |log| log.lines().find(|l| l.contains("SIGHUP:")).map(str::to_string),
        )
        .unwrap_or_else(|why| panic!("the server never reported the reload: {why}"))
    }

    /// Whether the master and every worker it forked are running. A worker's
    /// death stops the master too, but only once its death watch next looks.
    pub fn is_alive(&mut self) -> bool {
        if self.process.try_wait().expect("try_wait").is_some() {
            return false;
        }
        let mut fds: Vec<libc::pollfd> = self
            .workers_alive
            .iter()
            .map(|fd| libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            })
            .collect();
        // SAFETY: `fds` is a live array of `fds.len()` pollfds.
        unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, 0) == 0 }
    }

    fn start_inner(workers: usize, extra_env: &[(&str, &str)], tls_listen: Option<&str>, auth: Auth) -> Self {
        Self::boot(workers, extra_env, tls_listen, auth, Boot::Fresh)
    }

    fn boot(workers: usize, extra_env: &[(&str, &str)], tls_listen: Option<&str>, auth: Auth, how: Boot) -> Self {
        let bin = server_bin();
        let tmpdir = tempfile::Builder::new()
            .prefix("gnitz_test_")
            .tempdir()
            .expect("failed to create tempdir");
        let data_dir = match &how {
            Boot::On(dir) => dir.clone(),
            Boot::Fresh | Boot::WithBackups => tmpdir.path().join("data"),
        };
        let paths = BootPaths {
            bin,
            data_dir,
            sock_path: tmpdir.path().join("gnitz.sock"),
            stderr_path: tmpdir.path().join("server_stderr.log"),
        };
        let env: Vec<(String, String)> = extra_env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

        let backup_dir = matches!(how, Boot::WithBackups).then(|| {
            let backups = tmpdir.path().join("backups");
            fs::create_dir(&backups).expect("create the backup directory");
            backups
        });
        // Mint what the client authentication needs.
        let mut first_token = None;
        let tls = tls_listen.map(|listen| Tls {
            listen: listen.to_string(),
            auth: match auth {
                Auth::None => ClientAuth::None,
                Auth::ClientCa => {
                    let (ca, leaf_cert, leaf_key) = mint_client_ca_and_leaf(tmpdir.path());
                    ClientAuth::ClientCa { ca, leaf_cert, leaf_key }
                }
                Auth::Token => {
                    let file = tmpdir.path().join("tokens");
                    first_token = Some(mint_token(&paths.bin, &file, "first"));
                    ClientAuth::Token { file }
                }
            },
        });
        let flags = Flags { backup_dir, tls };

        let process = match spawn_and_wait_ready(&paths, workers, &env, &flags.argv()) {
            Ok(p) => p,
            Err(msg) => {
                let kept = tmpdir.keep();
                panic!("{msg}\nartifacts preserved at: {}", kept.display());
            }
        };

        ServerHandle {
            workers_alive: worker_pidfds(&process),
            process,
            paths,
            tmpdir: Some(tmpdir),
            workers,
            flags,
            env,
            first_token,
        }
    }

    fn is_tls(&self) -> bool {
        self.flags.tls.is_some()
    }

    /// How the TLS listener authenticates its clients; `None` without one.
    fn client_auth(&self) -> Option<&ClientAuth> {
        self.flags.tls.as_ref().map(|t| &t.auth)
    }

    /// The minted client leaf `(cert_pem, key_pem)` of a [`Self::start_mtls`]
    /// server; `caller` names the accessor that requires one.
    fn mtls_leaf(&self, caller: &str) -> (&Path, &Path) {
        match self.client_auth() {
            Some(ClientAuth::ClientCa { leaf_cert, leaf_key, .. }) => (leaf_cert, leaf_key),
            _ => panic!("{caller} requires a start_mtls server"),
        }
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
        let (cert, key) = self.mtls_leaf("mtls_client_cert_key");
        (cert.to_path_buf(), key.to_path_buf())
    }

    /// [`Self::tls_target`] presenting the CA-signed client leaf. Requires a
    /// [`Self::start_mtls`] server.
    pub fn mtls_target(&self) -> String {
        let (cert, key) = self.mtls_leaf("mtls_target");
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
    /// rebinds the port it had. `Err` carries the stderr tail of a boot that
    /// did not come up; the handle then holds no running server.
    pub fn restart(&mut self) -> Result<(), String> {
        let bound = self.is_tls().then(|| self.tls_endpoint());
        if let (Some(tls), Some(bound)) = (&mut self.flags.tls, bound) {
            tls.listen = bound;
        }
        self.process.kill().ok();
        self.process.wait().ok();
        self.process = spawn_and_wait_ready(&self.paths, self.workers, &self.env, &self.flags.argv())
            .map_err(|msg| format!("restart failed: {msg}"))?;
        self.workers_alive = worker_pidfds(&self.process);
        Ok(())
    }

    /// Print the tail of the master's stderr and of every worker's log.
    pub fn print_logs(&self) {
        eprintln!("\n──── server stderr (last {MAX_TAIL_LINES} lines) ────");
        eprintln!("{}", log_tail(&self.paths.stderr_path, MAX_TAIL_LINES));
        for w in 0..self.workers {
            eprintln!("──── worker {w} (last {MAX_TAIL_LINES} lines) ────");
            let log = self.paths.data_dir.join(format!("worker_{w}.log"));
            eprintln!("{}", log_tail(&log, MAX_TAIL_LINES));
        }
        eprintln!("──── end server logs ────");
    }

    /// Stop the server and remove its tmpdir, whatever became of it.
    pub fn discard(mut self) {
        let tmpdir = self.tmpdir.take();
        drop(self);
        drop(tmpdir);
    }
}

/// What a boot starts from.
enum Boot {
    /// A data directory of its own, created by the boot.
    Fresh,
    /// [`Self::Fresh`], with a backup directory beside it.
    WithBackups,
    /// An existing data directory the handle does not own.
    On(PathBuf),
}

/// How a TLS listener authenticates its clients, beyond what a loopback bind needs.
#[derive(Clone, Copy)]
enum Auth {
    None,
    /// A client certificate a minted CA signed.
    ClientCa,
    /// An access token.
    Token,
}

/// The server flags a boot passes beyond the data directory, socket and worker
/// count.
struct Flags {
    /// `--backup-dir`.
    backup_dir: Option<PathBuf>,
    tls: Option<Tls>,
}

/// A TLS listener's flags.
struct Tls {
    /// `--tls-listen`.
    listen: String,
    auth: ClientAuth,
}

/// An [`Auth`] with what its boot minted for it.
enum ClientAuth {
    None,
    /// `--tls-client-ca=ca`, and the leaf PEMs it signed.
    ClientCa {
        ca: PathBuf,
        leaf_cert: PathBuf,
        leaf_key: PathBuf,
    },
    /// `--auth-tokens=file`.
    Token {
        file: PathBuf,
    },
}

impl Flags {
    fn argv(&self) -> Vec<String> {
        let mut args = Vec::new();
        if let Some(dir) = &self.backup_dir {
            args.push(format!("--backup-dir={}", dir.display()));
        }
        if let Some(tls) = &self.tls {
            args.push(format!("--tls-listen={}", tls.listen));
            match &tls.auth {
                ClientAuth::None => {}
                ClientAuth::ClientCa { ca, .. } => args.push(format!("--tls-client-ca={}", ca.display())),
                ClientAuth::Token { file } => args.push(format!("--auth-tokens={}", file.display())),
            }
        }
        args
    }
}

/// A pidfd of every worker the ready server `master` forked. The master forks
/// them all before it logs ready, and never again.
fn worker_pidfds(master: &Child) -> Vec<OwnedFd> {
    let pid = master.id();
    // The master's main thread forks the workers.
    let children = fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")).unwrap_or_default();
    children
        .split_whitespace()
        .filter_map(|child| child.parse::<libc::pid_t>().ok())
        .filter_map(|child| {
            // SAFETY: pidfd_open takes a pid and flags, and returns a new fd or -1.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, child, 0) };
            // SAFETY: a non-negative return is an fd this process now owns.
            (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd as i32) })
        })
        .collect()
}

/// Run `bin --new-token file name`, returning the token it prints.
fn mint_token(bin: &Path, file: &Path, name: &str) -> String {
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
/// leaf has no EKU, which webpki accepts. Default rcgen validity (1975–4096)
/// never expires.
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

/// Spawn the server and block until it logs [`READY_LINE`]; `Err` carries the
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
    die_with_parent(&mut cmd, libc::SIGKILL);
    cmd.arg(&paths.data_dir)
        .arg(&paths.sock_path)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file.try_clone().expect("dup stderr log fd")));

    // Each server fallocates its whole SAL, and a parallel `cargo test` runs one
    // server per test; these tests write little.
    default_env(&mut cmd, "GNITZ_SAL_BYTES", "134217728"); // 128 MiB

    // Servers pinning independently would pile onto the same cores.
    default_env(&mut cmd, "GNITZ_CPU_AFFINITY", "0");
    cmd.envs(extra_env.iter().map(|(k, v)| (k, v)));
    cmd.arg(format!("--workers={workers}"));
    cmd.args(args);
    let mut proc = cmd.spawn().expect("failed to spawn server");

    await_log(
        &mut proc,
        &paths.stderr_path,
        offset,
        STARTUP_TIMEOUT,
        MAX_TAIL_LINES,
        |log| log.contains(READY_LINE).then_some(()),
    )
    .map_err(|why| {
        proc.kill().ok();
        proc.wait().ok();
        format!("server did not log {READY_LINE:?}: {why}")
    })?;
    Ok(proc)
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

        // A discarded server leaves nothing to report.
        let Some(tmpdir) = self.tmpdir.take() else { return };
        if std::thread::panicking() || crashed {
            self.print_logs();
            eprintln!("server artifacts preserved at: {}", tmpdir.keep().display());
        }
        // Clean pass: dropping the TempDir removes the directory.
    }
}

/// The most lines of a log a report quotes.
const MAX_TAIL_LINES: usize = 100;

/// The server binary the suites run: `GNITZ_SERVER_BIN`, or the one `make
/// server` builds at the repository root. A missing binary panics rather than
/// skips: a suite passing without a server tests nothing.
pub fn server_bin() -> PathBuf {
    let bin = env::var_os("GNITZ_SERVER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../gnitz-server")));
    assert!(
        bin.is_file(),
        "no server binary at {}: `make server` builds one (`make test` does it for you)",
        bin.display()
    );
    bin
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
