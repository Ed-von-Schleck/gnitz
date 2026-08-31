use super::*;

#[test]
fn eventfd_wakes_once_and_drains_its_counter() {
    let fd = eventfd_create().unwrap();
    assert_eq!(eventfd_wait(fd, 1), Wake::Idle);
    eventfd_signal(fd);
    assert_eq!(eventfd_wait(fd, 1000), Wake::Signalled);
    assert_eq!(eventfd_wait(fd, 1), Wake::Idle, "the wake drained the counter");
    unsafe { libc::close(fd) };
}
