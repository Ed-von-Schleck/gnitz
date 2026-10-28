//! Reactor state: `fsync` submits eagerly and yields its CQE.

use std::os::fd::OwnedFd;
use std::time::Duration;

use super::test_support::*;
use super::*;

/// `fsync` submits its SQE before returning, so its CQE arrives with no tick
/// driven — which is what lets it run while the caller works on — and resolves to
/// the CQE `res` verbatim: 0, or a socket's `-EINVAL`.
#[test]
fn fsync_is_submitted_eagerly_and_yields_the_cqe_res() {
    let r = make_reactor();
    let file = OwnedFd::from(tempfile::tempfile().expect("tempfile"));
    let (sock, _) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    for (fd, want) in [(file, 0), (OwnedFd::from(sock), -libc::EINVAL)] {
        let mut fut = std::pin::pin!(r.fsync(leaked(fd)));
        let deadline = Instant::now() + Duration::from_secs(10);
        let got = loop {
            r.run_ready();
            if let Some(rc) = try_poll_once(fut.as_mut()) {
                break rc;
            }
            assert!(
                Instant::now() < deadline,
                "no CQE without a tick: the SQE was not submitted"
            );
        };
        assert_eq!(got, want);
    }
}
