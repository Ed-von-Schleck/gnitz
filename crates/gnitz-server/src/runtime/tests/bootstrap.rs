use super::*;

/// In a forked child, because lowering the soft limit is process-wide and
/// would break every concurrently running test. In the parent the limit is
/// already above 1024, so `raise_fd_limit` early-returns and asserts nothing.
#[test]
fn test_raise_fd_limit() {
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());
    if pid == 0 {
        let mut rl: libc::rlimit = unsafe { std::mem::zeroed() };
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
        rl.rlim_cur = 64;
        let lowered = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rl) };
        raise_fd_limit(1024);
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
        let raised = rl.rlim_cur >= 1024.min(rl.rlim_max);
        unsafe { libc::_exit(i32::from(lowered != 0 || !raised)) };
    }
    let mut status = 0;
    unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(libc::WEXITSTATUS(status), 0, "soft limit not raised from 64 to 1024");
}
