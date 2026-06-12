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
    process:       Child,
    pub sock_path: String,
    stderr_path:   PathBuf,
    tmpdir:        Option<TempDir>,
}

impl ServerHandle {
    pub fn start() -> Option<Self> {
        Self::start_n(1)
    }

    pub fn start_n(workers: usize) -> Option<Self> {
        let bin = env::var("GNITZ_SERVER_BIN").unwrap_or_else(|_| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../gnitz-server").to_string()
        });
        if !PathBuf::from(&bin).is_file() {
            return None;
        }

        let tmpdir = tempfile::Builder::new()
            .prefix("gnitz_test_")
            .tempdir()
            .expect("failed to create tempdir");
        let data_dir    = tmpdir.path().join("data");
        let sock_path   = tmpdir.path().join("gnitz.sock");
        let stderr_path = tmpdir.path().join("server_stderr.log");
        let stderr_file = fs::File::create(&stderr_path)
            .expect("failed to create server stderr log file");

        let mut cmd = Command::new(&bin);
        cmd.arg(&data_dir).arg(&sock_path)
           .stdout(Stdio::null())
           .stderr(Stdio::from(stderr_file)); // captured for post-mortem
        if workers > 1 {
            cmd.arg(format!("--workers={workers}"));
        }
        let mut proc = cmd.spawn().expect("failed to spawn server");

        let deadline = Instant::now() + STARTUP_TIMEOUT;
        let mut backoff = Duration::from_millis(1);
        loop {
            if sock_path.exists() {
                break;
            }
            if let Ok(Some(status)) = proc.try_wait() {
                let tail = read_stderr_tail(&stderr_path);
                let kept = tmpdir.keep();
                panic!("server exited early ({status})\nstderr tail:\n{tail}\nartifacts preserved at: {}",
                    kept.display());
            }
            if Instant::now() >= deadline {
                proc.kill().ok();
                proc.wait().ok();
                let tail = read_stderr_tail(&stderr_path);
                let kept = tmpdir.keep();
                panic!("server did not create socket within {STARTUP_TIMEOUT:?}\nstderr tail:\n{tail}\nartifacts preserved at: {}",
                    kept.display());
            }
            thread::sleep(backoff);
            backoff = (backoff * 2).min(POLL_MAX);
        }

        Some(ServerHandle {
            process: proc,
            sock_path: sock_path.to_str().unwrap().to_string(),
            stderr_path,
            tmpdir: Some(tmpdir),
        })
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
