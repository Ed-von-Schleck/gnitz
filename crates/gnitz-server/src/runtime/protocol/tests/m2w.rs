use super::*;

#[test]
fn test_eventfd_signal_wait() {
    let fd = eventfd_create().unwrap();
    eventfd_signal(fd);
    assert_eq!(eventfd_wait(fd, 1000), Wake::Signalled);
    unsafe {
        libc::close(fd);
    }
}

#[test]
fn test_eventfd_wait_timeout() {
    let fd = eventfd_create().unwrap();
    assert_eq!(eventfd_wait(fd, 10), Wake::Idle);
    unsafe {
        libc::close(fd);
    }
}
