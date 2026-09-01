//! The fail-stop primitive and its macro.
//!
//! Reachable from `runtime` and from nothing below it. What enforces that is
//! the declaration order in the crate root: this module is `#[macro_use]`d
//! *after* `catalog` and `query`, and the attribute reaches only code that
//! follows it, so `gnitz_fatal_abort!` is not in scope there. Every fallible
//! path in those two rungs returns its error, and the decision to end the
//! process is taken at the `runtime` call sites that own the recovery.
//! `tests/rungs.rs` asserts the outcome, so a reorder of the crate root cannot
//! silently un-make it.

/// Terminate the process with exit code 134 (= 128 + SIGABRT) without running
/// atexit handlers, TLS destructors or a stdio flush. Called by
/// `gnitz_fatal_abort!`; it lives beside the macro so that expands to no
/// unqualified `libc` path.
///
/// Diverges, like the `_exit` it wraps: `gnitz_fatal_abort!` is used in value
/// positions (a match arm whose siblings yield values), which a `()` return
/// would break.
pub fn abort_134() -> ! {
    unsafe { libc::_exit(134) }
}

/// Emit a FATAL log line and immediately terminate **this process** with exit
/// code 134 (= 128 + SIGABRT), through [`abort_134`].
///
/// The termination is unconditional and uninterceptable: `_exit` runs no atexit
/// handler, no TLS destructor, no `Drop` and no stdio flush, `catch_unwind` does
/// not see it, and no wrapper can intercept it. None of those are safe to run
/// with a broken SAL mmap or in-flight io_uring SQEs, which is why the server
/// uses it. The exit code matches the "aborted" convention systemd/monit expect.
macro_rules! gnitz_fatal_abort {
    ($($arg:tt)*) => {{
        gnitz_store::foundation::log::_emit("FATAL", format_args!($($arg)*));
        $crate::fatal::abort_134()
    }};
}
