//! Every signal disposition of the server's processes.
//!
//! A reload and a stop are the master's: it catches SIGHUP (TLS reload) and
//! SIGTERM/SIGINT (graceful shutdown), and its signal loop polls the flags the
//! handlers set. A worker ignores all three, so a Ctrl-C or a `pkill` reaching
//! the whole group cannot kill a worker ahead of the master's checkpoint.

use std::sync::atomic::{AtomicBool, Ordering};

/// Set by the SIGTERM/SIGINT handler; polled by the signal loop. A plain
/// `AtomicBool` store is async-signal-safe, where touching reactor state is
/// not, so the handler does nothing but flip this.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_shutdown_signal(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
}

/// Set by the SIGHUP handler; polled and cleared by the signal loop.
static RELOAD_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_reload_signal(_sig: libc::c_int) {
    RELOAD_REQUESTED.store(true, Ordering::Relaxed);
}

/// Install the async-signal-safe `handler` for `sig`. A handler only flips its
/// flag; the signal loop's timer picks it up.
fn install_handler(sig: libc::c_int, handler: extern "C" fn(libc::c_int)) {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handler as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = libc::SA_RESTART;
        libc::sigaction(sig, &sa, std::ptr::null_mut());
    }
}

/// Catch SIGHUP, so the signal loop reloads the TLS listener.
pub(super) fn catch_reload() {
    install_handler(libc::SIGHUP, handle_reload_signal);
}

/// Catch SIGTERM/SIGINT, so the signal loop drives a final checkpoint before
/// exiting.
pub(super) fn catch_shutdown() {
    install_handler(libc::SIGTERM, handle_shutdown_signal);
    install_handler(libc::SIGINT, handle_shutdown_signal);
}

/// Whether a SIGHUP arrived since the last call; clears the flag.
pub(super) fn take_reload() -> bool {
    RELOAD_REQUESTED.swap(false, Ordering::Relaxed)
}

/// Whether a SIGTERM or SIGINT has arrived.
pub(super) fn shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}

/// A forked worker's dispositions: die with the master, and leave reload and
/// stop to it. Exits the process if `master_pid` is already gone.
pub(super) fn detach_worker(master_pid: i32) {
    // Die immediately if the master exits for any reason: a worker parks on the
    // SAL with no timeout, so this is its only notice.
    unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
    for sig in [libc::SIGHUP, libc::SIGINT, libc::SIGTERM] {
        unsafe { libc::signal(sig, libc::SIG_IGN) };
    }
    // Re-check: parent may have died in the fork→prctl window.
    if unsafe { libc::getppid() } != master_pid {
        unsafe { libc::_exit(0) };
    }
}
