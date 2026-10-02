use super::*;

#[test]
fn retry_eintr_retries_only_eintr() {
    let set_errno = |e| unsafe { *libc::__errno_location() = e };
    let mut calls = 0;
    let got = retry_eintr(|| {
        calls += 1;
        if calls == 1 {
            set_errno(libc::EINTR);
            -1
        } else {
            7
        }
    });
    assert_eq!((got.unwrap(), calls), (7, 2));

    let mut calls = 0;
    let got = retry_eintr(|| {
        calls += 1;
        set_errno(libc::EBADF);
        -1
    });
    assert_eq!((got.unwrap_err().raw_os_error(), calls), (Some(libc::EBADF), 1));
}

#[test]
fn set_sockopt_int_reports_a_refused_option() {
    let err = set_sockopt_int(-1, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADF));
}
