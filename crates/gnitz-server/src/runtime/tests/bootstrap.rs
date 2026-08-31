use super::*;

/// In a forked child, because lowering the soft limit is process-wide and
/// would break every concurrently running test: the child lowers it to 64 and
/// exits non-zero unless `raise_fd_limit` pushed it back to 1024.
#[test]
fn the_fd_limit_is_raised_to_the_requested_soft_ceiling() {
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
    unsafe { crate::runtime::test_support::assert_child_exited_ok(pid) };
}
