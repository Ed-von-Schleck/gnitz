//! Every signal disposition of the server's processes.
//!
//! A reload and a stop are the master's: it blocks SIGHUP (TLS reload) from
//! the start of the boot and SIGTERM/SIGINT (graceful shutdown) from serve
//! entry, and reads all three off a signalfd. Until serve entry SIGTERM/SIGINT
//! keep their default disposition, so a stop ends a boot still recovering. A
//! worker ignores all three, so a Ctrl-C or a `pkill` reaching the whole group
//! cannot kill a worker ahead of the master's checkpoint.

use std::os::fd::{AsFd, FromRawFd, OwnedFd};

use crate::runtime::reactor::Reactor;

/// The signal set of `sigs`.
fn sigset(sigs: &[libc::c_int]) -> libc::sigset_t {
    // SAFETY: `sigemptyset` initializes the set before `sigaddset` reads it.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for &sig in sigs {
            libc::sigaddset(&mut set, sig);
        }
        set
    }
}

/// Apply `how` (`SIG_BLOCK` or `SIG_UNBLOCK`) to `sigs` in the calling
/// thread's mask. The master runs no thread of its own, so its main thread's
/// mask decides where a signal sent to the process goes.
fn mask(how: libc::c_int, sigs: &[libc::c_int]) {
    let set = sigset(sigs);
    // SAFETY: `set` is an initialized sigset; no old mask is asked for.
    let rc = unsafe { libc::pthread_sigmask(how, &set, std::ptr::null_mut()) };
    assert_eq!(rc, 0, "pthread_sigmask: {}", std::io::Error::from_raw_os_error(rc));
}

/// Hold SIGHUP pending until [`signal_fd`] reads it, so a reload asked for
/// during the boot is served once the server is.
pub(super) fn catch_reload() {
    mask(libc::SIG_BLOCK, &[libc::SIGHUP]);
}

/// The signalfd the master reads SIGHUP, SIGTERM and SIGINT off.
pub(super) struct Signals {
    /// Open for the process's life: the ring's read names it by a `'static`
    /// borrow.
    fd: &'static OwnedFd,
    /// The read buffer, one `signalfd_siginfo` long; the ring's read carries
    /// it and hands it back.
    buf: Box<[u8]>,
}

impl Signals {
    /// Block SIGTERM/SIGINT too, and open the signalfd every one of the three
    /// is read from from here on.
    pub(super) fn open() -> Signals {
        let sigs = [libc::SIGHUP, libc::SIGTERM, libc::SIGINT];
        mask(libc::SIG_BLOCK, &sigs);
        let set = sigset(&sigs);
        // SAFETY: `set` is an initialized sigset.
        let fd = unsafe { libc::signalfd(-1, &set, libc::SFD_CLOEXEC) };
        if fd < 0 {
            gnitz_fatal_abort!("signalfd failed: {}", std::io::Error::last_os_error());
        }
        // SAFETY: `fd` is a descriptor `signalfd` just opened and nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Signals {
            fd: Box::leak(Box::new(fd)),
            buf: vec![0; size_of::<libc::signalfd_siginfo>()].into_boxed_slice(),
        }
    }

    /// The next signal read off the signalfd.
    pub(super) async fn next(&mut self, reactor: &Reactor) -> libc::c_int {
        let (rc, buf) = reactor.read(self.fd.as_fd(), std::mem::take(&mut self.buf)).await;
        self.buf = buf;
        let buf = &self.buf;
        if rc != buf.len() as i32 {
            gnitz_fatal_abort!("signalfd read answered {rc}");
        }
        // SAFETY: `buf` holds one whole `signalfd_siginfo`, a plain C struct;
        // the read is unaligned since a byte buffer promises no alignment.
        let info: libc::signalfd_siginfo = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast()) };
        info.ssi_signo as libc::c_int
    }
}

/// A forked worker's dispositions: die with the master, and leave reload and
/// stop to it. Exits the process if `master_pid` is already gone.
pub(super) fn detach_worker(master_pid: i32) {
    // Die immediately if the master exits for any reason: a worker parks on the
    // SAL with no timeout, so this is its only notice.
    if gnitz_foundation::process::set_parent_death_signal(master_pid, libc::SIGKILL).is_err() {
        unsafe { libc::_exit(0) };
    }
    let sigs = [libc::SIGHUP, libc::SIGINT, libc::SIGTERM];
    for sig in sigs {
        unsafe { libc::signal(sig, libc::SIG_IGN) };
    }
    // The fork inherits the master's blocked SIGHUP; a blocked signal is held
    // pending even while ignored, so unblocking lets the kernel discard it.
    mask(libc::SIG_UNBLOCK, &sigs);
}
