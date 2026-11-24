//! The master's signals: masked on the thread that opens them, and taken off
//! their signalfd through the reactor.

use super::*;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::{try_poll_once, within};

/// A signal sent to the thread that opened the signals is held blocked, and is
/// taken off the signalfd by the read that thread submits.
#[test]
fn a_blocked_signal_is_taken_off_the_signalfd() {
    within(|| {
        let r = make_reactor();
        // Masks the signals on this thread alone, which the signal is sent to,
        // so no other thread can take it.
        let mut signals = Signals::open();
        let mut next = std::pin::pin!(signals.next(r));
        assert!(try_poll_once(next.as_mut()).is_none(), "no signal yet");
        assert_eq!(unsafe { libc::pthread_kill(libc::pthread_self(), libc::SIGHUP) }, 0);
        assert_eq!(r.block_on(next), libc::SIGHUP);
    });
}
