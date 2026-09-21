use super::*;
use std::os::unix::io::AsRawFd;
/// The file reaches `size` before the mapping exists, so the store lands on
/// a real page instead of raising SIGBUS.
#[test]
fn test_map_file_reserved() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_file().as_raw_fd();
    let ptr = map_file_reserved(fd, 8192).unwrap();
    assert_eq!(fd_size(fd).unwrap(), 8192);
    unsafe {
        *ptr = 42;
        assert_eq!(*ptr, 42);
        libc::munmap(ptr as *mut libc::c_void, 8192);
    }
}

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
    // A forked child that panics unwinds into a copy of the harness whose main
    // thread is gone, so the parent must require the clean exit itself.
    let mut status = 0i32;
    retry_eintr(|| unsafe { libc::waitpid(pid, &mut status, 0) })
        .unwrap_or_else(|e| panic!("waitpid failed on child {pid}: {e}"));
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "child {pid} did not exit cleanly (status {status:#x})"
    );
}
