use super::*;

/// The prerequisite kernel, unconfined: the check this machine's own server
/// boots past.
#[test]
fn this_host_gives_the_reactor_its_ring() {
    check_host().unwrap();
}
