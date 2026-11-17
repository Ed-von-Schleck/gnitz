//! Syscall idioms `std` does not offer. Each returns `io::Result`, the errno
//! captured at the syscall.
//!
//! The idioms only the server calls — the EINTR retry, the fork-shared
//! mapping and the memfd — are gated to Linux, where the server runs; the
//! rest builds on any Unix, as a client does.

use std::fs::File;
use std::io;

use libc::c_int;

/// Retry a raw syscall until it succeeds (`>= 0`) or fails with an error other
/// than EINTR, handing back what it returned. Not for a `write`: a partial
/// write returns a non-negative count, which this reports as success.
#[cfg(target_os = "linux")]
pub fn retry_eintr(mut f: impl FnMut() -> c_int) -> io::Result<c_int> {
    loop {
        let ret = f();
        if ret >= 0 {
            return Ok(ret);
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINTR) {
            return Err(err);
        }
    }
}

/// `setsockopt(level, opt)` with a `c_int` value.
pub fn set_sockopt_int(fd: c_int, level: c_int, opt: c_int, val: c_int) -> io::Result<()> {
    // SAFETY: `val` lives across the call and the length passed is its size.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            (&raw const val).cast(),
            size_of::<c_int>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// A read-write mapping shared with every `fork()`ed child: `len` bytes of `fd`
/// from offset 0, or zeroed anonymous memory for `None`. `MAP_NORESERVE`, which
/// only the anonymous form is subject to: its pages are charged as they are
/// touched, so a mapping sized far above its occupancy is not refused for its size.
#[cfg(target_os = "linux")]
pub fn map_shared(fd: Option<std::os::fd::BorrowedFd<'_>>, len: usize) -> io::Result<*mut u8> {
    use std::os::fd::AsRawFd;
    let (flags, raw) = match fd {
        Some(fd) => (libc::MAP_SHARED | libc::MAP_NORESERVE, fd.as_raw_fd()),
        None => (libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1),
    };
    // SAFETY: a fresh mapping at an address the kernel picks.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            raw,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(ptr.cast())
}

/// A close-on-exec memfd named `name`.
#[cfg(target_os = "linux")]
pub fn memfd(name: &std::ffi::CStr) -> io::Result<File> {
    use std::os::fd::FromRawFd;
    // SAFETY: `name` is NUL-terminated.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a descriptor nothing else owns.
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Make the entries of directory `dir` durable.
pub fn fsync_dir(dir: impl AsRef<std::path::Path>) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Make `path`'s own entry durable: sync its parent, the working directory for
/// a bare name.
fn fsync_parent(path: impl AsRef<std::path::Path>) -> io::Result<()> {
    match path.as_ref().parent() {
        Some(p) if !p.as_os_str().is_empty() => fsync_dir(p),
        _ => fsync_dir("."),
    }
}

/// Create `dir` and its missing ancestors; once this returns, `dir`'s entry and
/// every entry it created are durable. `dir`'s is synced even where it already
/// exists, since a creator killed before its sync left it unsynced.
pub fn create_dir_all_durable(dir: impl AsRef<std::path::Path>) -> io::Result<()> {
    let dir = dir.as_ref();
    let missing: Vec<&std::path::Path> = dir
        .ancestors()
        .skip(1)
        .take_while(|d| !d.as_os_str().is_empty() && !d.is_dir())
        .collect();
    std::fs::create_dir_all(dir)?;
    std::iter::once(dir).chain(missing).try_for_each(fsync_parent)
}

/// Replace `path` with `bytes`, permission bits `mode`, whole or not at all, and
/// durably once this returns. The bytes go to a fresh `<path>.tmp` first, so a
/// stale one left by a crash never lends the file its own bits.
pub fn write_file_durable(path: impl AsRef<std::path::Path>, bytes: &[u8], mode: u32) -> io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let path = path.as_ref();
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    fsync_parent(path)
}

/// Write every byte of `bufs`, in order, to `w` in as few vectored writes as
/// it takes: a short write resumes past what was taken, and an interrupted one
/// is retried.
pub fn write_all_vectored(w: &mut impl io::Write, mut bufs: &mut [io::IoSlice<'_>]) -> io::Result<()> {
    while !bufs.is_empty() {
        match w.write_vectored(bufs) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => io::IoSlice::advance_slices(&mut bufs, written),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// A directory this process created for itself under the temp directory,
/// mode 0700 and named `prefix` and six random characters, removed with what
/// it holds when dropped.
pub struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    pub fn new(prefix: &str) -> io::Result<Self> {
        use std::os::unix::ffi::OsStringExt;
        let template = std::env::temp_dir().join(format!("{prefix}XXXXXX"));
        let mut template = std::ffi::CString::new(template.into_os_string().into_vec())?.into_bytes_with_nul();
        // SAFETY: `template` is a NUL-terminated, writable buffer that mkdtemp
        // rewrites in place; it outlives the call.
        if unsafe { libc::mkdtemp(template.as_mut_ptr().cast()) }.is_null() {
            return Err(io::Error::last_os_error());
        }
        template.pop();
        Ok(ScratchDir(std::ffi::OsString::from_vec(template).into()))
    }

    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
#[path = "tests/posix_io.rs"]
mod tests;
