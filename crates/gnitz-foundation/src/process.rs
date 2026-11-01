//! A child process this one runs: tied to the thread that spawns it, and
//! awaited on what it logs.

use std::env;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// How often [`await_log`] reads the log again.
const POLL_INTERVAL: Duration = Duration::from_millis(2);

/// How much of a log's end [`log_tail`] reads.
const TAIL_BYTES: u64 = 128 * 1024;

/// Set `key` for the child unless this process's environment sets it.
pub fn default_env(cmd: &mut Command, key: &str, value: &str) {
    if env::var_os(key).is_none() {
        cmd.env(key, value);
    }
}

/// Have the kernel send the child `signal` when the thread that spawns it
/// ends. The spawn fails with ESRCH when this process died between the fork
/// and the `prctl`, since the signal would then never come. A no-op off Linux.
#[cfg(target_os = "linux")]
pub fn die_with_parent(cmd: &mut Command, signal: libc::c_int) {
    use std::os::unix::process::CommandExt;
    let parent = std::process::id() as libc::pid_t;
    // SAFETY: `prctl` and `getppid` are async-signal-safe, and the closure
    // allocates nothing.
    unsafe {
        cmd.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, signal) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            match libc::getppid() == parent {
                true => Ok(()),
                false => Err(std::io::Error::from_raw_os_error(libc::ESRCH)),
            }
        });
    }
}

/// Have the kernel send the child `signal` when the thread that spawns it
/// ends. A no-op off Linux.
#[cfg(not(target_os = "linux"))]
pub fn die_with_parent(_: &mut Command, _: libc::c_int) {}

/// Poll the log at `path`, past its first `offset` bytes, until `found` finds
/// what it looks for there, `child` exits, or `timeout` passes. `Err` says
/// which, followed by the last `tail_lines` lines of the log.
pub fn await_log<T>(
    child: &mut Child,
    path: &Path,
    offset: u64,
    timeout: Duration,
    tail_lines: usize,
    mut found: impl FnMut(&str) -> Option<T>,
) -> Result<T, String> {
    let deadline = Instant::now() + timeout;
    let mut log: Option<File> = None;
    let mut logged = Vec::new();
    let why = loop {
        // The status before the log: a child that has exited has written all
        // of its log, so what it logged before exiting is found.
        let status = child.try_wait();
        // A log that cannot be read yet is read as empty; the exit or the
        // deadline ends the wait.
        if log.is_none() {
            log = File::open(path)
                .and_then(|mut f| f.seek(SeekFrom::Start(offset)).map(|_| f))
                .ok();
        }
        if let Some(f) = &mut log {
            let _ = f.read_to_end(&mut logged);
        }
        if let Some(t) = found(&String::from_utf8_lossy(&logged)) {
            return Ok(t);
        }
        match status {
            Ok(Some(status)) => break format!("exited ({status})"),
            _ if Instant::now() >= deadline => break format!("timed out after {timeout:?}"),
            _ => std::thread::sleep(POLL_INTERVAL),
        }
    };
    Err(format!("{why}; its log ends:\n{}", log_tail(path, tail_lines)))
}

/// The last `lines` lines of the log at `path`, read from its final
/// [`TAIL_BYTES`]; lossy UTF-8, so the tail of a hard crash still prints.
/// Empty when the file is missing or unreadable.
pub fn log_tail(path: &Path, lines: usize) -> String {
    let mut buf = Vec::new();
    let read = File::open(path).and_then(|mut f| {
        let len = f.metadata()?.len();
        f.seek(SeekFrom::Start(len.saturating_sub(TAIL_BYTES)))?;
        f.read_to_end(&mut buf)
    });
    if read.is_err() {
        return String::new();
    }
    let text = String::from_utf8_lossy(&buf);
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[cfg(test)]
#[path = "tests/process.rs"]
mod tests;
