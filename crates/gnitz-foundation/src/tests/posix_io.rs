use super::*;

#[cfg(target_os = "linux")]
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

#[test]
fn fsync_dir_syncs_a_directory_and_refuses_a_missing_one() {
    let root = ScratchDir::new("gnitz-test-").unwrap();
    fsync_dir(root.path().to_str().unwrap()).unwrap();
    let missing = root.path().join("absent");
    assert_eq!(
        fsync_dir(missing.to_str().unwrap()).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}

#[test]
fn write_file_durable_replaces_the_file_with_its_own_mode() {
    use std::os::unix::fs::PermissionsExt as _;
    let root = ScratchDir::new("gnitz-test-").unwrap();
    let path = root.path().join("key");
    // A stale, world-readable tmp file a crash left behind.
    std::fs::write(root.path().join("key.tmp"), b"stale").unwrap();
    std::fs::set_permissions(root.path().join("key.tmp"), std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::write(&path, b"old").unwrap();
    write_file_durable(&path, b"new", 0o600).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    assert!(!root.path().join("key.tmp").exists());
}
