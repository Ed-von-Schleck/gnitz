//! The fail-stop macro.
//!
//! Private to `runtime`: `#[macro_use]` puts `gnitz_fatal_abort!` in scope only
//! inside `runtime`'s submodules, so nothing under `catalog/` or `query/` can
//! name it. Every fallible path there returns its error, and the decision is
//! taken at the `runtime` call sites that own the recovery.

/// Emit a FATAL log line and immediately terminate **this process** with exit
/// code 134 (= 128 + SIGABRT).
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
        unsafe { ::libc::_exit(134) }
    }};
}
