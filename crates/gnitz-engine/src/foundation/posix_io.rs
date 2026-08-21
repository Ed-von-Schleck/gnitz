//! POSIX file-I/O and mmap wrappers. One rule no signature states on its own:
//! every call here returns `io::Result`, so the errno is captured at the
//! syscall and no caller has to read it back out of ambient state.

use std::os::fd::{FromRawFd, OwnedFd};

use libc::c_int;

/// Write all bytes to a raw `fd`, handling partial writes and EINTR — the
/// `File::write_all` of a descriptor nobody owns (stdout, a socketpair end).
pub fn write_all_fd(fd: c_int, data: &[u8]) -> std::io::Result<()> {
    let mut done: usize = 0;
    while done < data.len() {
        let ret = unsafe { libc::write(fd, data.as_ptr().add(done) as *const libc::c_void, data.len() - done) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }
        if ret == 0 {
            // POSIX guarantees regular files never return 0 for a non-zero
            // count, but some device types can — without this guard the loop
            // would spin forever at 100% CPU. Treat it as an error.
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
        }
        done += ret as usize;
    }
    Ok(())
}

/// `libc::openat` returning an `OwnedFd`. The errno is captured here, at the
/// syscall, so no caller has to read it back out of ambient state.
pub(crate) fn openat_owned(dirfd: c_int, name: &std::ffi::CStr, flags: c_int) -> std::io::Result<OwnedFd> {
    let fd = unsafe { libc::openat(dirfd, name.as_ptr(), flags, 0o644 as libc::mode_t) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fresh descriptor from `openat`; the `OwnedFd` is the sole closer.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// `libc::open` returning an `OwnedFd`, the `AT_FDCWD` case of [`openat_owned`].
pub(crate) fn open_owned(path: &std::ffi::CStr, flags: c_int) -> std::io::Result<OwnedFd> {
    openat_owned(libc::AT_FDCWD, path, flags)
}

/// `libc::renameat`, with the errno captured before any cleanup the caller
/// runs on the failure path can overwrite it.
pub(crate) fn renameat(
    olddirfd: c_int,
    old: &std::ffi::CStr,
    newdirfd: c_int,
    new: &std::ffi::CStr,
) -> std::io::Result<()> {
    let rc = unsafe { libc::renameat(olddirfd, old.as_ptr(), newdirfd, new.as_ptr()) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Create an anonymous temporary file (`O_TMPFILE`) on the filesystem backing
/// `dir`.
///
/// The file has NO directory entry: the kernel reclaims its inode and blocks
/// the instant the last fd (or mapping) referencing it goes away — on a normal
/// close, on process exit, and on every abnormal exit (`panic = "abort"`,
/// `SIGKILL`, OOM-kill) — so it can never leak onto disk, with no unlink
/// bookkeeping at all.
///
/// Opened `O_RDWR` so the caller can write the file and later `mmap` it
/// `PROT_READ`. `dir` must be a real directory on a filesystem that supports
/// `O_TMPFILE` (ext4 / xfs / btrfs / tmpfs — every filesystem gnitz stores data
/// on). `O_TMPFILE` already implies `O_DIRECTORY`, so the path is validated as a
/// directory by the kernel.
pub(crate) fn open_tmpfile(dir: &str) -> std::io::Result<OwnedFd> {
    let dir_c = std::ffi::CString::new(dir).map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let fd = unsafe { libc::open(dir_c.as_ptr(), libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC, 0o600) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fresh descriptor from `open`; the `OwnedFd` is the sole closer.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Ask btrfs to overwrite `fd` in place instead of copying (`FS_NOCOW_FL`).
///
/// Best-effort, like [`madvise_hugepage`]: ext4/xfs/tmpfs reject the flag with
/// `EOPNOTSUPP` and have no copy-on-write path to disable, so a failure here is
/// the normal case off btrfs and carries no information worth reporting.
///
/// A `SETFLAGS` that changes nothing still commits a btrfs transaction (~7 µs,
/// against ~0.3 µs for the read alone) and every store re-open lands here, so
/// the already-set case returns after the read.
pub fn try_set_nocow(fd: i32) {
    // FS_IOC_GETFLAGS = 0x80086601, FS_IOC_SETFLAGS = 0x40086602
    // FS_NOCOW_FL = 0x00800000
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
pub fn fd_size(fd: c_int) -> std::io::Result<usize> {
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

/// How a mapping will be read. `MADV_SEQUENTIAL` sets `VM_SEQ_READ`, so every
/// fault issues a full forward window instead of faulting around: right for a
/// single front-to-back pass, wasted I/O for a mapping probed at unpredictable
/// offsets.
#[derive(Clone, Copy)]
pub(crate) enum Advice {
    Sequential,
    Default,
}

/// RAII handle for a read-only mmap'd file region, so the unmap path lives in
/// exactly one place — no consumer's error return or Drop repeats the `munmap`.
pub(crate) struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    /// mmap `[0, len)` of `fd` read-only. `len` must be `> 0`. The mapping holds
    /// its own reference to the inode, so the caller may close `fd` immediately
    /// after.
    pub(crate) fn from_fd(fd: c_int, len: usize, advice: Advice) -> std::io::Result<Self> {
        debug_assert!(len > 0);
        let raw = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, fd, 0) };
        if raw == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        if matches!(advice, Advice::Sequential) {
            // Best-effort read-ahead hint; errors are ignored.
            unsafe {
                libc::madvise(raw, len, libc::MADV_SEQUENTIAL);
            }
        }
        Ok(Mmap {
            ptr: raw as *mut u8,
            len,
        })
    }

    /// Open `path` read-only and mmap the whole (non-empty) file.
    pub(crate) fn open_ro(path: &std::ffi::CStr, advice: Advice) -> std::io::Result<Self> {
        let fd = open_owned(path, libc::O_RDONLY)?;
        let len = fd_size(std::os::fd::AsRawFd::as_raw_fd(&fd))?;
        if len == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        let map = Self::from_fd(std::os::fd::AsRawFd::as_raw_fd(&fd), len, advice)?;
        madvise_hugepage(map.ptr, map.len);
        Ok(map)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
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
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::os::unix::io::AsRawFd;
    #[test]
    fn test_write_all_fd_roundtrip() {
        // Guards the happy path of the partial-write loop (and that the
        // ret==0 guard does not break a normal full write).
        let mut f = tempfile::tempfile().unwrap();
        let data = b"hello write_all_fd partial-write loop";
        write_all_fd(f.as_raw_fd(), data).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_open_tmpfile_is_anonymous_and_round_trips() {
        // O_TMPFILE yields an inode with ZERO directory links: nothing to leak,
        // reclaimed on close. The data written round-trips (fd is O_RDWR).
        let dir = tempfile::tempdir().expect("tempdir");
        let dir_path = dir.path().to_str().expect("utf8 dir");
        let owned = open_tmpfile(dir_path).expect("open_tmpfile");
        let fd = owned.as_raw_fd();

        let data = b"external-sort spill run bytes";
        write_all_fd(fd, data).expect("write_all_fd");

        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstat(fd, &mut st) }, 0, "fstat");
        assert_eq!(st.st_nlink, 0, "O_TMPFILE inode must have no directory entry");
        assert_eq!(st.st_size as usize, data.len(), "written size");

        let mapped = Mmap::from_fd(fd, data.len(), Advice::Sequential).expect("map the anonymous file back");
        assert_eq!(mapped.as_slice(), data, "round-trip through the anonymous file");
    }
}
