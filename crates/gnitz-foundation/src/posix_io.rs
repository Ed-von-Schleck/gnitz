//! POSIX file-I/O and mmap wrappers. One rule no signature states on its own:
//! every call here returns `io::Result`, so the errno is captured at the
//! syscall and no caller has to read it back out of ambient state.

use libc::c_int;

/// Retry a raw syscall until it succeeds (`>= 0`) or fails with an error other
/// than EINTR, handing back what it returned. Still not for a short `write`: a
/// partial write returns a non-negative count, which this reports as success.
pub fn retry_eintr(mut f: impl FnMut() -> c_int) -> std::io::Result<c_int> {
    loop {
        let ret = f();
        if ret >= 0 {
            return Ok(ret);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINTR) {
            return Err(err);
        }
    }
}

/// Raise the `RLIMIT_NOFILE` soft limit towards `target`, capped by the hard
/// limit. Best-effort: a refusal surfaces later as `EMFILE` at the open that
/// could not be served.
pub fn raise_fd_limit(target: u64) {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 || rl.rlim_cur >= target as libc::rlim_t {
            return;
        }
        rl.rlim_cur = (target as libc::rlim_t).min(rl.rlim_max);
        libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
    }
}

/// `setsockopt(level, opt)` with a `c_int` value. Best-effort: a refused option
/// is not an error worth surfacing anywhere this is called.
pub fn set_sockopt_int(fd: c_int, level: c_int, opt: c_int, val: c_int) {
    // SAFETY: setsockopt on a caller-supplied fd with a properly-sized option value.
    unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<c_int>() as libc::socklen_t,
        );
    }
}

/// Ask btrfs to overwrite `path` in place instead of copying (`FS_NOCOW_FL`).
///
/// Best-effort, like [`madvise_hugepage`]: ext4/xfs/tmpfs reject the flag with
/// `EOPNOTSUPP` and have no copy-on-write path to disable, so a failure here is
/// the normal case off btrfs and carries no information worth reporting.
pub fn try_set_nocow(path: &str) {
    let Ok(file) = std::fs::File::open(path) else { return };
    let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
    const FS_IOC_GETFLAGS: libc::c_ulong = 0x80086601;
    const FS_IOC_SETFLAGS: libc::c_ulong = 0x40086602;
    const FS_NOCOW_FL: libc::c_int = 0x00800000;

    let mut flags: libc::c_int = 0;
    unsafe {
        if libc::ioctl(fd, FS_IOC_GETFLAGS, &mut flags) < 0 {
            return;
        }
        if flags & FS_NOCOW_FL != 0 {
            return;
        }
        flags |= FS_NOCOW_FL;
        libc::ioctl(fd, FS_IOC_SETFLAGS, &flags);
    }
}

/// Size of the file behind `fd` (fstat), in bytes.
pub(crate) fn fd_size(fd: c_int) -> std::io::Result<usize> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(st.st_size as usize)
}

/// Hint the kernel to back [ptr, ptr+size) with transparent hugepages.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
/// For anonymous private memory: requires `enabled` = `madvise` or `always`.
/// For memfd/shmem: requires `shmem_enabled` = `advise` or `within_size`.
/// For writable file-backed mmap: silently ignored by the kernel.
pub fn madvise_hugepage(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_HUGEPAGE);
    }
}

/// An anonymous read-write mapping shared with every `fork()`ed child. Not
/// commit-charged (`MAP_NORESERVE`), so a mapping sized far above its live
/// occupancy does not fail under `vm.overcommit_memory=2`.
pub fn map_anon_shared(size: usize) -> std::io::Result<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }
    Ok(ptr as *mut u8)
}

/// mmap `size` bytes of `fd` `MAP_SHARED` read-write, `fallocate`ing the file to
/// `size` first: `mmap` past the end of a file succeeds and the first store into
/// the resulting hole raises `SIGBUS`, which nothing here handles. Reserving the
/// blocks also means a later store cannot fail for want of disk space.
pub fn map_file_reserved(fd: c_int, size: usize) -> std::io::Result<*mut u8> {
    if fd_size(fd)? < size {
        retry_eintr(|| unsafe { libc::fallocate(fd, 0, 0, size as libc::off_t) })?;
    }
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }
    Ok(ptr as *mut u8)
}

/// RAII handle for a read-only mmap'd file region, so the unmap path lives in
/// exactly one place — no consumer's error return or Drop repeats the `munmap`.
pub struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    /// mmap `[0, len)` of `fd` read-only. `len` must be `> 0`. The mapping holds
    /// its own reference to the inode, so the caller may close `fd` immediately
    /// after.
    pub fn from_fd(fd: c_int, len: usize) -> std::io::Result<Self> {
        debug_assert!(len > 0);
        let raw = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, fd, 0) };
        if raw == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Mmap { ptr: raw as *mut u8, len })
    }

    /// Hint that the mapping will be read front to back (`MADV_SEQUENTIAL`).
    /// Best-effort: an error is ignored.
    pub fn advise_sequential(&self) {
        unsafe {
            libc::madvise(self.ptr as *mut libc::c_void, self.len, libc::MADV_SEQUENTIAL);
        }
    }

    /// Open `path` read-only and mmap the whole (non-empty) file.
    pub fn open_ro(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
        let len = fd_size(fd)?;
        if len == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        let map = Self::from_fd(fd, len)?;
        madvise_hugepage(map.ptr, map.len);
        Ok(map)
    }

    /// The mapped bytes. Never empty: `from_fd` requires a non-zero length and
    /// `open_ro` refuses an empty file, so a caller after the length reads
    /// `as_slice().len()` rather than a second accessor.
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

#[cfg(test)]
#[path = "tests/posix_io.rs"]
mod tests;
