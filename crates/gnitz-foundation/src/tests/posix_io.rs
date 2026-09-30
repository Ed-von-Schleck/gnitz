use super::*;
use std::os::unix::io::AsRawFd;

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

/// The file reaches `size` before the mapping exists, so the store lands on a
/// real page instead of raising SIGBUS; a remap keeps what the file holds, and a
/// smaller one does not shrink it.
#[test]
fn map_file_reserved_extends_but_never_shrinks() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let fd = tmp.as_file().as_raw_fd();
    let len = || tmp.as_file().metadata().unwrap().len();
    let ptr = map_file_reserved(fd, 8192).unwrap();
    assert_eq!(len(), 8192);
    unsafe {
        *ptr.add(4096) = 42;
        libc::munmap(ptr as *mut libc::c_void, 8192);
    }
    let ptr = map_file_reserved(fd, 4096 + 1).unwrap();
    assert_eq!(len(), 8192);
    unsafe {
        assert_eq!(*ptr.add(4096), 42);
        libc::munmap(ptr as *mut libc::c_void, 4096 + 1);
    }
}

#[test]
fn mmap_maps_the_whole_file_and_refuses_an_empty_one() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    let err = Mmap::from_file(tmp.as_file()).err().unwrap();
    assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
    std::io::Write::write_all(&mut tmp, b"abc").unwrap();
    assert_eq!(Mmap::from_file(tmp.as_file()).unwrap().as_slice(), b"abc");
}

/// In a forked child, because lowering the soft limit is process-wide and
/// would break every concurrently running test: the child lowers it to 64, then
/// checks that `raise_fd_limit` never lowers it, raises it to exactly the
/// target, and caps it at the hard limit.
#[test]
fn the_fd_limit_is_raised_to_the_requested_soft_ceiling() {
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());
    if pid == 0 {
        let soft = || {
            let mut rl: libc::rlimit = unsafe { std::mem::zeroed() };
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
            rl
        };
        let mut rl = soft();
        rl.rlim_cur = 64;
        let lowered = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rl) } == 0;
        raise_fd_limit(32);
        let never_lowers = soft().rlim_cur == 64;
        raise_fd_limit(128);
        let exact = soft().rlim_cur == 128.min(rl.rlim_max);
        raise_fd_limit(u64::MAX);
        let capped = soft().rlim_cur == rl.rlim_max;
        unsafe { libc::_exit(i32::from(!(lowered && never_lowers && exact && capped))) };
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

#[test]
fn create_dir_makes_parents_and_reports_whether_it_created() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("a/b/c");
    let dir = dir.to_str().unwrap();
    assert!(create_dir(dir).unwrap());
    assert!(std::path::Path::new(dir).is_dir());
    assert!(!create_dir(dir).unwrap());
    let file = root.path().join("f");
    std::fs::write(&file, b"x").unwrap();
    assert!(create_dir(file.to_str().unwrap()).is_err());
}

#[test]
fn fsync_dir_syncs_a_directory_and_refuses_a_missing_one() {
    let root = tempfile::tempdir().unwrap();
    fsync_dir(root.path().to_str().unwrap()).unwrap();
    let missing = root.path().join("absent");
    assert_eq!(
        fsync_dir(missing.to_str().unwrap()).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}
