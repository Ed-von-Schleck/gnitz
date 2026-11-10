use std::num::NonZero;

use super::Workers;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::{fork_child, within};
use gnitz_foundation::posix_io::retry_eintr;

/// A child that lives until it is killed, or until this thread exits.
fn spawn_live() -> NonZero<libc::pid_t> {
    // `PR_SET_PDEATHSIG` on the forking thread: a panicking test unwinds off
    // this thread and takes the paused child with it, rather than orphaning it.
    let pid = unsafe {
        fork_child(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0);
            libc::pause();
        })
    };
    NonZero::new(pid).unwrap()
}

/// Whether `pid` is still a child of this process, running or zombie.
fn is_child(pid: NonZero<libc::pid_t>) -> bool {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let flags = libc::WEXITED | libc::WNOHANG | libc::WNOWAIT;
    retry_eintr(|| unsafe { libc::waitid(libc::P_PID, pid.get() as libc::id_t, &mut info, flags) }).is_ok()
}

/// `death` resolves with the rank of the worker that exited, every time it is
/// asked, and reaps none: the dead one is still this process's child behind
/// it. Dropping the workers then reaps every one, the dead and the live.
#[test]
fn death_names_the_dead_worker_without_reaping_it() {
    let live = spawn_live();
    let dead = NonZero::new(unsafe { fork_child(|| {}) }).unwrap();
    within(move || {
        let reactor = make_reactor();
        let workers = Workers::new(vec![live, dead]);
        assert_eq!(reactor.block_on(workers.death(&reactor)), 1);
        assert_eq!(reactor.block_on(workers.death(&reactor)), 1, "and again");
        assert!(is_child(dead), "the watch left the dead worker unreaped");
        drop(workers);
        assert!(!is_child(live) && !is_child(dead), "the drop reaped both");
    });
}
