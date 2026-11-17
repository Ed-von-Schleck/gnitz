use std::num::NonZero;

use super::{WorkerEnd, Workers};
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::{fork_child, within};
use gnitz_foundation::posix_io::retry_eintr;

/// A child that lives until it is killed, or until this thread exits.
fn spawn_live() -> NonZero<libc::pid_t> {
    // Tied to the forking thread: a panicking test unwinds off this thread and
    // takes the paused child with it, rather than orphaning it.
    let parent = std::process::id() as libc::pid_t;
    let pid = unsafe {
        fork_child(|| {
            if gnitz_foundation::process::set_parent_death_signal(parent, libc::SIGKILL).is_ok() {
                libc::pause();
            }
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

/// `death` resolves with the rank of the worker that exited and how it ended,
/// every time it is asked, and reaps none: the dead one is still this
/// process's child behind it. Dropping the workers then reaps every one, the
/// dead and the live.
#[test]
fn death_names_the_dead_worker_without_reaping_it() {
    let live = spawn_live();
    let dead = NonZero::new(unsafe { fork_child(|| libc::_exit(3)) }).unwrap();
    within(move || {
        let reactor = make_reactor();
        let workers = Workers::new(vec![live, dead]);
        assert_eq!(reactor.block_on(workers.death(&reactor)), (1, WorkerEnd::Exited(3)));
        assert_eq!(
            reactor.block_on(workers.death(&reactor)),
            (1, WorkerEnd::Exited(3)),
            "and again"
        );
        assert!(is_child(dead), "the watch left the dead worker unreaped");
        drop(workers);
        assert!(!is_child(live) && !is_child(dead), "the drop reaped both");
    });
}

/// A worker a signal killed is reported with that signal.
#[test]
fn death_names_the_signal_that_killed_a_worker() {
    let killed = spawn_live();
    let mut workers = Workers::new(Vec::new());
    workers.push(spawn_live());
    workers.push(killed);
    unsafe { libc::kill(killed.get(), libc::SIGKILL) };
    within(move || {
        let reactor = make_reactor();
        let (w, end) = reactor.block_on(workers.death(&reactor));
        assert_eq!((w, end), (1, WorkerEnd::Killed(libc::SIGKILL)));
        assert_eq!(end.to_string(), "was killed by signal 9 (SIGKILL)");
    });
}
