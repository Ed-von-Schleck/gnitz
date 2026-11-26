//! The server's exit codes and the fail-stop macro.
//!
//! Private to `runtime`: `#[macro_use]` puts `gnitz_fatal_abort!` in scope only
//! inside `runtime`'s submodules, so nothing under `catalog/` or `query/` can
//! name it. Every fallible path there returns its error, and the decision is
//! taken at the `runtime` call sites that own the recovery.

/// The server process's exit codes, which `--help` lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServerExit {
    /// Stopped by a shutdown signal.
    Clean = 0,
    BootFailed = 1,
    /// A worker process died while serving.
    WorkerDied = 2,
    /// `sysexits.h`'s `EX_USAGE`: argv was refused, before anything was created,
    /// or a `GNITZ_*` variable was, at whatever point of the boot read it.
    Usage = 64,
    /// `sysexits.h`'s `EX_DATAERR`: the data directory is in a format this build
    /// does not read, and was left as it was.
    DataFormat = 65,
    /// `gnitz_fatal_abort!`: 128 + SIGABRT.
    FailStop = 134,
}

impl ServerExit {
    pub(crate) const fn code(self) -> i32 {
        self as i32
    }
}

/// Emit a FATAL log line and immediately terminate **this process** with
/// [`ServerExit::FailStop`].
///
/// The termination is unconditional and uninterceptable: `_exit` runs no atexit
/// handler, no TLS destructor, no `Drop` and no stdio flush, `catch_unwind` does
/// not see it, and no wrapper can intercept it. None of those are safe to run
/// with a broken SAL mmap or in-flight io_uring SQEs, which is why the server
/// uses it. The exit code matches the "aborted" convention systemd/monit expect.
///
/// The expansion has type `!`, so the macro also stands in value positions (a
/// match arm whose siblings yield values).
macro_rules! gnitz_fatal_abort {
    ($($arg:tt)*) => {{
        gnitz_foundation::log::emit("FATAL", format_args!($($arg)*));
        unsafe { ::libc::_exit($crate::runtime::ServerExit::FailStop.code()) }
    }};
}
