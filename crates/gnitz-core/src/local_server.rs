//! A `gnitz-server` this process runs for its own use: the shell's session
//! server and Python's `gnitz.local_server`. A client links no server; it finds
//! one installed separately and runs it.
//!
//! The server gets a scratch directory holding its socket, its log and, unless
//! it is given a data directory to keep, its data. Stopping it SIGTERMs it, which
//! it answers by checkpointing, waits for its exit and removes the scratch
//! directory. A server whose starting thread ends without stopping it is
//! SIGTERMed by the kernel.

use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use gnitz_foundation::log::READY_LINE;
use gnitz_foundation::posix_io::ScratchDir;
use gnitz_foundation::process::{await_log, default_env, die_with_parent, log_tail};

/// The server's binary name.
pub const SERVER: &str = "gnitz-server";

/// The SAL a throwaway server preallocates. A server's own default is sized for
/// a database worth keeping, and a throwaway one would hold a gigabyte of disk
/// for a few rows.
const THROWAWAY_SAL_BYTES: &str = "67108864"; // 64 MiB

/// How long a boot may take before it is given up on. A kept data directory
/// replays its log tail before it serves.
const BOOT_TIMEOUT: Duration = Duration::from_secs(300);

/// How long a SIGTERMed server may take to checkpoint and exit before its whole
/// process group is SIGKILLed.
const STOP_TIMEOUT: Duration = Duration::from_secs(120);

/// How often a stop is checked on.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// The lines of a server's log an error quotes.
const LOG_TAIL_LINES: usize = 20;

/// A running server of this process's own.
pub struct LocalServer {
    child: Child,
    /// The socket, the log and a throwaway server's data; `None` once stopped.
    scratch: Option<ScratchDir>,
    socket: PathBuf,
    data_dir: PathBuf,
    /// The server's stderr, in the scratch directory.
    log: PathBuf,
}

impl LocalServer {
    /// Run the server at `bin` with `workers` workers, keeping its data in
    /// `data_dir` where one is given and in the scratch directory otherwise,
    /// and return once it serves.
    pub fn start(bin: &Path, data_dir: Option<&Path>, workers: u32) -> Result<Self, String> {
        let scratch = ScratchDir::new("gnitz-").map_err(|e| format!("cannot create a scratch directory: {e}"))?;
        let socket = scratch.path().join("gnitz.sock");
        let log_path = scratch.path().join("server.log");
        let log = fs::File::create(&log_path).map_err(|e| format!("cannot create {}: {e}", log_path.display()))?;

        let mut cmd = Command::new(bin);
        cmd.arg(format!("--workers={workers}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log);
        let data_dir = match data_dir {
            Some(dir) => dir.to_path_buf(),
            None => {
                default_env(&mut cmd, "GNITZ_SAL_BYTES", THROWAWAY_SAL_BYTES);
                scratch.path().join("data")
            }
        };
        cmd.arg(&data_dir).arg(&socket);
        // A server of this process's own shares the host with whatever else
        // runs there, so its threads are not pinned onto the first cores.
        default_env(&mut cmd, "GNITZ_CPU_AFFINITY", "0");
        // A group of its own: a terminal's Ctrl-C reaches this process alone,
        // which decides whether it stops the server.
        cmd.process_group(0);
        // SIGTERMed when the thread that started it ends without stopping it.
        die_with_parent(&mut cmd, libc::SIGTERM);
        let child = cmd.spawn().map_err(|e| format!("cannot run {}: {e}", bin.display()))?;

        let mut server = LocalServer {
            child,
            scratch: Some(scratch),
            socket,
            data_dir,
            log: log_path,
        };
        server.await_ready()?;
        Ok(server)
    }

    /// The Unix socket the server listens on: the target a client dials.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// The directory the server keeps its data in.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Whether the server is still running.
    pub fn running(&mut self) -> bool {
        self.scratch.is_some() && matches!(self.child.try_wait(), Ok(None))
    }

    /// Stop the server and remove the scratch directory; a no-op once stopped.
    /// `Err` reports a server that had already exited by itself after it
    /// served, quoting its log, which goes with the scratch directory.
    pub fn stop(&mut self) -> Result<(), String> {
        if self.scratch.is_none() {
            return Ok(());
        }
        let report = match self.child.try_wait() {
            Ok(Some(status)) => Err(format!(
                "{SERVER} exited ({status}) while it served:\n{}",
                log_tail(&self.log, LOG_TAIL_LINES)
            )),
            _ => {
                self.terminate();
                Ok(())
            }
        };
        self.scratch = None;
        report
    }

    /// SIGTERM the server and reap it, SIGKILLing its process group, workers
    /// included, should it not exit within [`STOP_TIMEOUT`].
    fn terminate(&mut self) {
        let pid = self.child.id() as libc::pid_t;
        // SAFETY: `kill` takes no pointer; `pid` is this child's, not yet reaped.
        unsafe { libc::kill(pid, libc::SIGTERM) };
        let deadline = Instant::now() + STOP_TIMEOUT;
        while matches!(self.child.try_wait(), Ok(None)) {
            if Instant::now() >= deadline {
                // SAFETY: as above; the group id is the master's pid, and is
                // the group's until every member is gone.
                unsafe { libc::killpg(pid, libc::SIGKILL) };
                let _ = self.child.wait();
                return;
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    fn await_ready(&mut self) -> Result<(), String> {
        let Err(why) = await_log(&mut self.child, &self.log, 0, BOOT_TIMEOUT, LOG_TAIL_LINES, |log| {
            log.contains(READY_LINE).then_some(())
        }) else {
            return Ok(());
        };
        let _ = self.stop();
        Err(format!("{SERVER} failed before it served: {why}"))
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        if let Err(report) = self.stop() {
            eprintln!("gnitz: {report}");
        }
    }
}

/// The `gnitz-server` to run: the one `GNITZ_SERVER_BIN` names where it is set,
/// else the one in `installed`, else the first on `PATH`.
pub fn find_server(installed: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(bin) = env::var_os("GNITZ_SERVER_BIN") {
        let bin = PathBuf::from(bin);
        return match is_executable(&bin) {
            true => Ok(bin),
            false => Err(format!(
                "GNITZ_SERVER_BIN names {}, which is no executable",
                bin.display()
            )),
        };
    }
    let path = env::var_os("PATH");
    installed
        .map(Path::to_path_buf)
        .into_iter()
        .chain(path.iter().flat_map(env::split_paths))
        .map(|dir| dir.join(SERVER))
        .find(|bin| is_executable(bin))
        .ok_or_else(|| match installed {
            Some(dir) => format!("no {SERVER} in {} or on PATH", dir.display()),
            None => format!("no {SERVER} on PATH"),
        })
}

fn is_executable(path: &Path) -> bool {
    fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(test)]
#[path = "tests/local_server.rs"]
mod tests;
