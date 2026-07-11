//! POSIX I/O and Linux syscall wrappers, in two documented tiers:
//!
//! - **File-I/O tier** — safe functions returning `io::Result` (or
//!   `Option<OwnedFd>` where callers use absence semantically): fd read/write
//!   with EINTR/partial-write handling, fdatasync/fsync, fallocate, ftruncate,
//!   O_TMPFILE, NOCOW, madvise, the Unix server socket, fd-limit, `Mmap`.
//! - **IPC tier** — raw return codes, kept deliberately: eventfd, futex,
//!   memfd, `mmap_shared`. Their callers inspect errno (EAGAIN/ETIMEDOUT),
//!   re-read rings rather than trust returns, and manage fd lifecycles
//!   manually across `fork()`; `io::Result`/`OwnedFd` would fight that.

use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::atomic::AtomicU32;

use libc::c_int;

// ---------------------------------------------------------------------------
// File-I/O tier
// ---------------------------------------------------------------------------

/// Write all bytes to `fd`, handling partial writes and EINTR.
pub(crate) fn write_all_fd(fd: c_int, data: &[u8]) -> std::io::Result<()> {
    let mut written: usize = 0;
    while written < data.len() {
        let ret = unsafe {
            libc::write(
                fd,
                data[written..].as_ptr() as *const libc::c_void,
                data.len() - written,
            )
        };
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
        written += ret as usize;
    }
    Ok(())
}

/// pwrite all bytes to `fd` at `offset`, handling short writes and EINTR —
/// the positional twin of [`write_all_fd`].
pub(crate) fn pwrite_all_fd(fd: c_int, buf: &[u8], mut offset: libc::off_t) -> std::io::Result<()> {
    let mut remaining = buf.len();
    let mut p = buf.as_ptr();
    while remaining > 0 {
        let written = unsafe { libc::pwrite(fd, p as *const libc::c_void, remaining, offset) };
        if written < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }
        if written == 0 {
            // POSIX guarantees regular files never return 0 for a non-zero
            // count, but some device types can — without this guard the loop
            // would spin forever at 100% CPU. Treat it as an error.
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
        }
        remaining -= written as usize;
        p = unsafe { p.add(written as usize) };
        offset += written as libc::off_t;
    }
    Ok(())
}

/// Read up to `buf.len()` bytes from `fd`, handling EINTR. Returns the number
/// of bytes read — short only at EOF.
pub(crate) fn read_all_fd(fd: c_int, buf: &mut [u8]) -> std::io::Result<usize> {
    let total = buf.len();
    let mut offset: usize = 0;
    while offset < total {
        let ret = unsafe { libc::read(fd, buf[offset..].as_mut_ptr() as *mut libc::c_void, total - offset) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }
        if ret == 0 {
            break; // EOF
        }
        offset += ret as usize;
    }
    Ok(offset)
}

/// `libc::openat` returning an `OwnedFd`, or `None` on failure (errno untouched).
pub(crate) fn openat_owned(dirfd: c_int, name: &std::ffi::CStr, flags: c_int) -> Option<OwnedFd> {
    let fd = unsafe { libc::openat(dirfd, name.as_ptr(), flags, 0o644 as libc::mode_t) };
    if fd < 0 {
        return None;
    }
    // SAFETY: fresh descriptor from `openat`; the `OwnedFd` is the sole closer.
    Some(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// `libc::open` returning an `OwnedFd`, or `None` on failure (errno untouched).
pub(crate) fn open_owned(path: &std::ffi::CStr, flags: c_int) -> Option<OwnedFd> {
    openat_owned(libc::AT_FDCWD, path, flags)
}

/// Retry a raw syscall until it succeeds (`>= 0`) or fails with an error other
/// than EINTR.
fn retry_eintr(mut f: impl FnMut() -> c_int) -> std::io::Result<()> {
    loop {
        if f() >= 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINTR) {
            return Err(err);
        }
    }
}

/// `fdatasync` with EINTR retry. Returns the OS error on any other failure.
pub(crate) fn fdatasync_eintr(fd: c_int) -> std::io::Result<()> {
    retry_eintr(|| unsafe { libc::fdatasync(fd) })
}

/// `fsync` with EINTR retry. Returns the OS error on any other failure.
pub(crate) fn fsync_eintr(fd: c_int) -> std::io::Result<()> {
    retry_eintr(|| unsafe { libc::fsync(fd) })
}

/// Pre-allocate blocks for fd.
pub fn fallocate(fd: c_int, length: i64) -> std::io::Result<()> {
    retry_eintr(|| unsafe { libc::fallocate(fd, 0, 0, length as libc::off_t) })
}

/// Set file size via ftruncate.
pub fn ftruncate(fd: c_int, size: i64) -> std::io::Result<()> {
    retry_eintr(|| unsafe { libc::ftruncate(fd, size as libc::off_t) })
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
pub fn open_tmpfile(dir: &str) -> std::io::Result<OwnedFd> {
    let dir_c = std::ffi::CString::new(dir).map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let fd = unsafe { libc::open(dir_c.as_ptr(), libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC, 0o600) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fresh descriptor from `open`; the `OwnedFd` is the sole closer.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Set FS_NOCOW_FL on fd (btrfs in-place overwrites).
/// Silently ignored on non-btrfs filesystems.
/// Returns 0 on success, -1 on error (non-fatal).
pub fn try_set_nocow(fd: i32) -> i32 {
    // FS_IOC_GETFLAGS = 0x80086601, FS_IOC_SETFLAGS = 0x40086602
    // FS_NOCOW_FL = 0x00800000
    const FS_IOC_GETFLAGS: libc::c_ulong = 0x80086601;
    const FS_IOC_SETFLAGS: libc::c_ulong = 0x40086602;
    const FS_NOCOW_FL: libc::c_int = 0x00800000;

    let mut flags: libc::c_int = 0;
    unsafe {
        if libc::ioctl(fd, FS_IOC_GETFLAGS, &mut flags) < 0 {
            return -1;
        }
        flags |= FS_NOCOW_FL;
        libc::ioctl(fd, FS_IOC_SETFLAGS, &flags)
    }
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

/// Pre-fault writable page-table entries for [ptr, ptr+size).
/// Uses MADV_POPULATE_WRITE (Linux 5.14+): installs writable PTEs and
/// triggers the filesystem page_mkwrite callback, so later writes don't
/// fault.  Unlike memset, this does not dirty page contents or pollute
/// CPU caches.  Best-effort: ignores errors and is a no-op for null ptr
/// or size 0.
pub fn madvise_populate_write(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_POPULATE_WRITE);
    }
}

/// Hint the kernel to read-ahead [ptr, ptr+size) into page cache.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
pub fn madvise_willneed(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_WILLNEED);
    }
}

/// Capture errno BEFORE the cleanup close() can clobber it. Shared error
/// path of the listener constructors.
fn close_with_errno(fd: c_int) -> std::io::Error {
    let err = std::io::Error::last_os_error();
    unsafe { libc::close(fd) };
    err
}

/// Shared listener tail: listen(1024) + O_NONBLOCK. Closes `fd` on error.
fn listen_nonblock(fd: c_int) -> std::io::Result<()> {
    unsafe {
        if libc::listen(fd, 1024) < 0 {
            return Err(close_with_errno(fd));
        }
        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        if flags < 0 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(close_with_errno(fd));
        }
    }
    Ok(())
}

/// Create a Unix domain SOCK_STREAM server socket: socket + bind + listen.
/// Sets the listen socket to non-blocking.
/// Unlinks any existing socket at `path` before binding.
pub fn server_create(path: &str) -> std::io::Result<OwnedFd> {
    unsafe {
        let fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let path_bytes = path.as_bytes();
        // Reject paths that would not fit with the null terminator.
        if path_bytes.len() >= addr.sun_path.len() {
            libc::close(fd);
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
        }
        for (dst, &src) in addr.sun_path.iter_mut().zip(path_bytes) {
            *dst = src as libc::c_char;
        }

        // addr is zeroed so sun_path is already null-terminated after the copy above.
        libc::unlink(addr.sun_path.as_ptr());

        if libc::bind(
            fd,
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        ) < 0
        {
            return Err(close_with_errno(fd));
        }
        listen_nonblock(fd)?;

        // SAFETY: fresh descriptor from `socket`; the `OwnedFd` is the sole closer.
        Ok(OwnedFd::from_raw_fd(fd))
    }
}

/// Abort both directions of a connected socket (`shutdown(fd, SHUT_RDWR)`).
///
/// Unlike `close`, this forces the kernel to tear the connection down
/// immediately even with data queued, so any io_uring `OP_SEND` still pending
/// on `fd` errors out promptly (`ECONNRESET`/`EPIPE`) and its CQE fires. Used
/// to evict a client that has stopped draining a zero-copy ring-slot egress,
/// releasing the held W2M slot once the send completes. Retries on `EINTR`;
/// tolerates `ENOTCONN` (peer already gone). Returns 0 on success (or
/// `ENOTCONN`), -1 on any other error. Does NOT close the fd — the caller still
/// reaps it through the normal close path.
pub fn shutdown(fd: i32) -> i32 {
    loop {
        let rc = unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
        if rc >= 0 {
            return 0;
        }
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::ENOTCONN) => return 0,
            _ => return -1,
        }
    }
}

/// Create a TCP SOCK_STREAM listen socket on `addr`: socket + SO_REUSEADDR +
/// bind + listen(1024) (same backlog as `server_create`) + O_NONBLOCK.
pub fn tcp_bind(addr: &std::net::SocketAddr) -> std::io::Result<i32> {
    let family = match addr {
        std::net::SocketAddr::V4(_) => libc::AF_INET,
        std::net::SocketAddr::V6(_) => libc::AF_INET6,
    };
    unsafe {
        let fd = libc::socket(family, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let on: c_int = 1;
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &on as *const _ as *const libc::c_void,
            std::mem::size_of::<c_int>() as libc::socklen_t,
        ) < 0
        {
            return Err(close_with_errno(fd));
        }
        let (ss, len) = sockaddr_from_addr(addr);
        if libc::bind(fd, &ss as *const _ as *const libc::sockaddr, len) < 0 {
            return Err(close_with_errno(fd));
        }
        listen_nonblock(fd)?;
        Ok(fd)
    }
}

/// The socket's bound local address (getsockname) — port-0 discovery.
pub fn tcp_local_addr(fd: i32) -> Option<std::net::SocketAddr> {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let rc = unsafe { libc::getsockname(fd, &mut ss as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return None;
    }
    addr_from_sockaddr(&ss)
}

/// Bare `SO_KEEPALIVE` (no interval tuning): a silently half-open TCP
/// connection is reaped by the kernel default probing (~2 h) instead of
/// parking a recv forever.
pub fn set_keepalive(fd: i32) {
    let on: c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &on as *const _ as *const libc::c_void,
            std::mem::size_of::<c_int>() as libc::socklen_t,
        );
    }
}

/// `TCP_NODELAY`: small control frames must not pay Nagle's 40 ms batching
/// delay (AF_UNIX has no Nagle, so this restores latency parity).
pub fn set_nodelay(fd: i32) {
    let on: c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &on as *const _ as *const libc::c_void,
            std::mem::size_of::<c_int>() as libc::socklen_t,
        );
    }
}

/// SocketAddr → (sockaddr_storage, socklen_t). Zero-pads the storage and
/// preserves the IPv6 flowinfo/scope_id so link-local destinations route
/// (without a scope id the kernel rejects an `fe80::` send with EINVAL).
pub fn sockaddr_from_addr(addr: &std::net::SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        std::net::SocketAddr::V4(a) => {
            let sin = unsafe { &mut *(&mut ss as *mut _ as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = a.port().to_be();
            // octets() are already network order; from_ne_bytes keeps the
            // in-memory byte order intact on both endiannesses.
            sin.sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());
            (ss, std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t)
        }
        std::net::SocketAddr::V6(a) => {
            let sin6 = unsafe { &mut *(&mut ss as *mut _ as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.sin6_addr.s6_addr = a.ip().octets();
            sin6.sin6_scope_id = a.scope_id();
            (ss, std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t)
        }
    }
}

/// sockaddr_storage → SocketAddr, discriminated by `ss_family` alone.
/// None for non-AF_INET/AF_INET6. Preserves IPv6 flowinfo/scope_id so a
/// reply to a received `src` reaches a link-local peer.
pub fn addr_from_sockaddr(ss: &libc::sockaddr_storage) -> Option<std::net::SocketAddr> {
    match ss.ss_family as libc::c_int {
        libc::AF_INET => {
            let sin = unsafe { &*(ss as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Some(std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                ip,
                u16::from_be(sin.sin_port),
            )))
        }
        libc::AF_INET6 => {
            let sin6 = unsafe { &*(ss as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Some(std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
                ip,
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

/// Raise RLIMIT_NOFILE soft limit to `target` (capped by hard limit).
/// Returns the new soft limit, or -1 on failure.
pub fn raise_fd_limit(target: u64) -> i64 {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 {
            return -1;
        }
        if rl.rlim_cur >= target as libc::rlim_t {
            return rl.rlim_cur as i64;
        }
        rl.rlim_cur = target as libc::rlim_t;
        if rl.rlim_cur > rl.rlim_max {
            rl.rlim_cur = rl.rlim_max;
        }
        if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 {
            return -1;
        }
        rl.rlim_cur as i64
    }
}

/// Best-effort memory budget for the process, in bytes.
///
/// Reads cgroup v2 `/sys/fs/cgroup/memory.max` — the real container limit —
/// and parses it when finite; falls back to total physical RAM
/// (`_SC_PHYS_PAGES × _SC_PAGE_SIZE`) when the file is absent or reads the
/// literal `"max"` (no limit). This is the technique the JVM and Go use to
/// avoid the sysconf-reports-host-RAM pitfall inside a container.
///
/// Cached in a `OnceLock` (read once — the value is process-lifetime
/// invariant and the sole caller, the master reactor, has no fork-time
/// divergence to worry about). Returns 0 only if every source fails, which
/// callers clamp up to a floor.
pub fn available_memory_bytes() -> usize {
    use std::sync::OnceLock;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        // cgroup v2 `memory.max` when finite. The literal `"max"` (no limit)
        // fails the integer parse and falls through, as does an absent file.
        if let Some(v) = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&v| v > 0)
        {
            return v;
        }
        let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
        if pages > 0 && page_size > 0 {
            (pages as usize).saturating_mul(page_size as usize)
        } else {
            0
        }
    })
}

/// Size of the file behind `fd` (fstat), in bytes.
pub(crate) fn fd_size(fd: c_int) -> std::io::Result<usize> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(st.st_size as usize)
}

/// RAII handle for a read-only mmap'd file region, so the unmap path lives in
/// exactly one place — no consumer's error return or Drop repeats the `munmap`.
pub(crate) struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    /// mmap `[0, len)` of `fd` read-only with a sequential-access hint.
    /// `len` must be `> 0`. The mapping holds its own reference to the inode,
    /// so the caller may close `fd` immediately after.
    pub(crate) fn from_fd(fd: c_int, len: usize) -> std::io::Result<Self> {
        debug_assert!(len > 0);
        let raw = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, fd, 0) };
        if raw == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        // Best-effort sequential read-ahead hint; errors are ignored.
        unsafe {
            libc::madvise(raw, len, libc::MADV_SEQUENTIAL);
        }
        Ok(Mmap {
            ptr: raw as *mut u8,
            len,
        })
    }

    /// Open `path` read-only, mmap the whole (non-empty) file, and apply
    /// huge-page + sequential madvise hints.
    pub(crate) fn open_ro(path: &std::ffi::CStr) -> std::io::Result<Self> {
        let fd = open_owned(path, libc::O_RDONLY).ok_or_else(std::io::Error::last_os_error)?;
        let len = fd_size(std::os::fd::AsRawFd::as_raw_fd(&fd))?;
        if len == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        let map = Self::from_fd(std::os::fd::AsRawFd::as_raw_fd(&fd), len)?;
        madvise_hugepage(map.ptr, map.len);
        Ok(map)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.ptr
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

// ---------------------------------------------------------------------------
// IPC tier
// ---------------------------------------------------------------------------

/// Return the errno of the most recent failed syscall.
#[inline]
pub fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

/// `futex2(2)` flags byte for a 32-bit atomic. Matches the kernel constant
/// `FUTEX2_SIZE_U32` (=2). No `FUTEX2_PRIVATE` bit — W2M is `MAP_SHARED`
/// across `fork()`.
pub const FUTEX2_SIZE_U32: u32 = 2;

/// Create a non-blocking, close-on-exec eventfd. Returns fd or -1 on error.
pub fn eventfd_create() -> i32 {
    unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) }
}

/// Signal an eventfd (increment counter by 1). Returns 0 on success, -1 on error.
pub fn eventfd_signal(efd: i32) -> i32 {
    let v: u64 = 1;
    loop {
        let n = unsafe { libc::write(efd, &v as *const u64 as *const libc::c_void, 8) };
        if n == 8 {
            return 0;
        }
        if n < 0 && errno() == libc::EINTR {
            continue;
        }
        return -1;
    }
}

/// Wait for an eventfd to become readable.
/// Returns >0 if ready (counter drained), 0 on timeout, <0 on error.
pub fn eventfd_wait(efd: i32, timeout_ms: i32) -> i32 {
    let mut pfd = libc::pollfd {
        fd: efd,
        events: libc::POLLIN,
        revents: 0,
    };
    let r = loop {
        let r = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if r < 0 && errno() == libc::EINTR {
            continue;
        }
        break r;
    };
    if r > 0 {
        let mut v: u64 = 0;
        unsafe {
            libc::read(efd, &mut v as *mut u64 as *mut libc::c_void, 8);
        }
    }
    r
}

/// Create an anonymous memory-backed fd with MFD_CLOEXEC.
/// Returns fd on success, -1 on error.
pub fn memfd_create(name: &[u8]) -> i32 {
    // name must be null-terminated for the syscall
    let mut buf = [0u8; 64];
    let len = name.len().min(62);
    buf[..len].copy_from_slice(&name[..len]);
    buf[len] = 0;
    unsafe { libc::memfd_create(buf.as_ptr() as *const libc::c_char, libc::MFD_CLOEXEC) }
}

/// Block on a futex at `ptr` until its value differs from `expected` or
/// a wake arrives. Uses the `v1` futex(2) syscall (opcode `FUTEX_WAIT`)
/// — NOT the `FUTEX_PRIVATE_FLAG` variant, since W2M regions are
/// `MAP_SHARED` across `fork()`. `timeout_ms < 0` means "block forever".
///
/// Returns the syscall return value: 0 on successful wake,
/// -1 on error (inspect `errno` — EAGAIN means value already differed,
/// ETIMEDOUT means the timespec elapsed).
pub fn futex_wait_u32(ptr: *const AtomicU32, expected: u32, timeout_ms: i32) -> i32 {
    let ts = libc::timespec {
        tv_sec: (timeout_ms as i64) / 1000,
        tv_nsec: ((timeout_ms as i64) % 1000) * 1_000_000,
    };
    let ts_ptr: *const libc::timespec = if timeout_ms < 0 { std::ptr::null() } else { &ts };
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            ptr as *const libc::c_void,
            libc::FUTEX_WAIT,
            expected as libc::c_int,
            ts_ptr,
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    }
}

/// Wake up at most `n_waiters` futex waiters parked on `ptr` via v1
/// `FUTEX_WAKE` (no `FUTEX_PRIVATE_FLAG` — W2M is shared). Returns
/// the number of waiters woken, or -1 on error.
pub fn futex_wake_u32(ptr: *const AtomicU32, n_waiters: u32) -> i32 {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            ptr as *const libc::c_void,
            libc::FUTEX_WAKE,
            n_waiters as libc::c_int,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    }
}

/// Kernel ABI `struct futex_waitv` (`futex_waitv(2)`, Linux 5.16+).
#[repr(C)]
#[derive(Clone, Copy)]
struct FutexWaitvRaw {
    val: u64,
    uaddr: u64,
    flags: u32,
    __reserved: u32,
}

/// Kernel `FUTEX_WAITV_MAX` — the hard cap on words per `futex_waitv` call.
/// `foundation` (L0) may not name `runtime::sal::MAX_WORKERS` (layering), so the
/// wrapper carries its own bound; `w2m.rs` statically asserts `MAX_WORKERS <=`
/// this, which is the only call site that builds the word list.
pub(crate) const MAX_FUTEX_WAITV: usize = 128;

/// Synchronously wait on MULTIPLE futex words at once (`SYS_futex_waitv`),
/// returning when ANY differs from its expected value or is woken — the
/// synchronous analogue of the reactor's `IORING_OP_FUTEX_WAITV`. The master
/// must wait on every still-pending worker's `reader_seq`, since a publish by
/// ANY worker wakes only that worker's word and a single-word `futex_wait` would
/// miss it. `ptrs[i]` pairs with `expected[i]`.
///
/// `timeout_ms` becomes an ABSOLUTE `CLOCK_MONOTONIC` deadline — `futex_waitv`
/// requires absolute timeouts, unlike the relative `futex_wait_u32` above; do
/// not "harmonize" the two. `< 0` blocks forever. Returns the woken index
/// (`>= 0`), or `-1` on timeout/error; callers re-read the rings rather than
/// trust the return (the ring data is authoritative, as in `futex_wait_u32`), so
/// any failed syscall degrades to one extra poll rather than a hang.
pub fn futex_waitv_u32(ptrs: &[*const AtomicU32], expected: &[u32], timeout_ms: i32) -> i32 {
    let n = ptrs.len();
    debug_assert_eq!(n, expected.len());
    if n == 0 || n > MAX_FUTEX_WAITV {
        return -1; // unreachable given the w2m.rs assert; defensive.
    }
    let mut waiters = [FutexWaitvRaw {
        val: 0,
        uaddr: 0,
        flags: 0,
        __reserved: 0,
    }; MAX_FUTEX_WAITV];
    for i in 0..n {
        waiters[i] = FutexWaitvRaw {
            val: expected[i] as u64,
            uaddr: ptrs[i] as u64,
            flags: FUTEX2_SIZE_U32,
            __reserved: 0,
        };
    }
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let ts_ptr: *const libc::timespec = if timeout_ms < 0 {
        std::ptr::null()
    } else {
        unsafe {
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
        }
        ts.tv_sec += (timeout_ms as i64) / 1000;
        ts.tv_nsec += ((timeout_ms as i64) % 1000) * 1_000_000;
        if ts.tv_nsec >= 1_000_000_000 {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1_000_000_000;
        }
        &ts
    };
    // libc 0.2.186 exposes `SYS_futex_waitv` per-arch (449 on x86_64/aarch64),
    // matching how `futex_wait_u32` uses `libc::SYS_futex`.
    unsafe {
        libc::syscall(
            libc::SYS_futex_waitv,
            waiters.as_ptr(),
            n as libc::c_uint,
            0u32, // flags
            ts_ptr,
            libc::CLOCK_MONOTONIC,
        ) as i32
    }
}

/// mmap a shared, read-write region of `size` bytes backed by `fd`.
/// Returns the mapped pointer, or null on error.
pub fn mmap_shared(fd: i32, size: usize) -> *mut u8 {
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
        std::ptr::null_mut()
    } else {
        ptr as *mut u8
    }
}

/// Raise RLIMIT_NOFILE soft limit to the hard limit.
/// Called once per process via `std::sync::Once`; safe to invoke from any test.
#[cfg(test)]
pub(crate) fn raise_fd_limit_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe {
        let mut rlim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 && rlim.rlim_cur < rlim.rlim_max {
            rlim.rlim_cur = rlim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    fn test_fallocate() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let raw_fd = tmp.as_file().as_raw_fd();
        fallocate(raw_fd, 1048576).expect("fallocate failed");
        assert_eq!(fd_size(raw_fd).unwrap(), 1048576);
    }

    #[test]
    fn test_try_set_nocow() {
        // On non-btrfs this returns -1, but must not crash
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let raw_fd = tmp.as_file().as_raw_fd();
        let _ = try_set_nocow(raw_fd); // just verify no crash/panic
    }

    #[test]
    fn test_raise_fd_limit() {
        // Should succeed (may be no-op if already ≥ 1024)
        let r = raise_fd_limit(1024);
        assert!(r >= 1024, "expected ≥1024, got {r}");
    }

    #[test]
    fn test_server_create() {
        let path = "/tmp/gnitz_test_sys_server_create.sock";
        let fd = server_create(path).expect("server_create failed");
        drop(fd);
        unsafe {
            libc::unlink(format!("{path}\0").as_ptr() as *const libc::c_char);
        }
    }

    #[test]
    fn test_server_create_path_too_long() {
        // sun_path is 108 bytes; a path of exactly 108 bytes has no room for the null terminator
        let long_path = "/tmp/".to_string() + &"a".repeat(110);
        assert!(long_path.len() >= 108);
        assert!(server_create(&long_path).is_err(), "expected error for overlong path");
    }

    #[test]
    fn test_sockaddr_roundtrip_v4() {
        let addr: std::net::SocketAddr = "192.168.7.13:5432".parse().unwrap();
        let (ss, len) = sockaddr_from_addr(&addr);
        assert_eq!(len as usize, std::mem::size_of::<libc::sockaddr_in>());
        assert_eq!(addr_from_sockaddr(&ss), Some(addr));
    }

    #[test]
    fn test_sockaddr_roundtrip_v6() {
        // flowinfo + scope_id must survive the round-trip (link-local replies).
        let addr = std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
            "fe80::1234:5678".parse().unwrap(),
            9999,
            7,
            3,
        ));
        let (ss, len) = sockaddr_from_addr(&addr);
        assert_eq!(len as usize, std::mem::size_of::<libc::sockaddr_in6>());
        assert_eq!(addr_from_sockaddr(&ss), Some(addr));
    }

    #[test]
    fn test_addr_from_sockaddr_unknown_family() {
        let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        ss.ss_family = libc::AF_UNIX as libc::sa_family_t;
        assert_eq!(addr_from_sockaddr(&ss), None);
    }

    #[test]
    fn test_shutdown_aborts_connected_socket() {
        // On a connected socketpair, `shutdown(SHUT_RDWR)` aborts the write
        // side, so a subsequent send on that end fails (EPIPE) instead of
        // queueing — the property the reactor relies on to force a stalled
        // client's pending OP_SEND to error out. MSG_NOSIGNAL suppresses
        // SIGPIPE so the failing send returns rather than killing the process.
        let mut fds = [0i32; 2];
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(rc, 0, "socketpair failed");
        let (a, b) = (fds[0], fds[1]);
        assert_eq!(shutdown(a), 0, "shutdown on connected socket must succeed");
        let buf = [0u8; 4];
        let n = unsafe { libc::send(a, buf.as_ptr() as *const libc::c_void, buf.len(), libc::MSG_NOSIGNAL) };
        assert!(n < 0, "send after SHUT_RDWR must fail, got {n}");
        unsafe {
            libc::close(a);
            libc::close(b);
        }
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

        assert_eq!(unsafe { libc::lseek(fd, 0, libc::SEEK_SET) }, 0, "rewind");
        let mut back = vec![0u8; data.len()];
        assert_eq!(read_all_fd(fd, &mut back).expect("read back"), data.len());
        assert_eq!(&back, data, "round-trip through the anonymous file");
    }

    #[test]
    fn test_shutdown_tolerates_enotconn() {
        // An unconnected socket → shutdown returns ENOTCONN, which the wrapper
        // maps to success (0): evicting an already-gone peer is not an error.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(fd >= 0, "socket() failed");
        assert_eq!(shutdown(fd), 0, "ENOTCONN must be tolerated as success");
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_server_create_is_nonblocking() {
        let path = "/tmp/gnitz_test_sys_server_nonblocking.sock";
        let fd = server_create(path).expect("server_create failed");
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL, 0) };
        drop(fd);
        unsafe {
            libc::unlink(format!("{path}\0").as_ptr() as *const libc::c_char);
        }
        assert!(flags >= 0, "F_GETFL failed: {flags}");
        assert!(
            flags & libc::O_NONBLOCK != 0,
            "socket is not non-blocking, flags={flags:#o}"
        );
    }

    #[test]
    fn test_eventfd_create_close() {
        let fd = eventfd_create();
        assert!(fd >= 0, "eventfd_create failed: {fd}");
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_eventfd_signal_wait() {
        let fd = eventfd_create();
        assert!(fd >= 0);
        assert_eq!(eventfd_signal(fd), 0);
        let r = eventfd_wait(fd, 1000);
        assert!(r > 0, "expected >0, got {r}");
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_eventfd_wait_timeout() {
        let fd = eventfd_create();
        assert!(fd >= 0);
        let r = eventfd_wait(fd, 10);
        assert_eq!(r, 0, "expected 0 (timeout), got {r}");
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_cross_process_atomic() {
        use std::sync::atomic::{AtomicU64, Ordering};

        // Create shared mmap region
        let fd = unsafe { libc::memfd_create(c"test".as_ptr(), 0) };
        assert!(fd >= 0);
        unsafe {
            libc::ftruncate(fd, 4096);
        }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                4096,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED);

        let efd = eventfd_create();
        assert!(efd >= 0);

        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // Child: write atomic value, signal parent
            let atomic = unsafe { &*(ptr as *const AtomicU64) };
            atomic.store(0xDEADBEEF, Ordering::Release);
            eventfd_signal(efd);
            unsafe {
                libc::_exit(0);
            }
        }

        // Parent: wait for child signal, read atomic
        let r = eventfd_wait(efd, 5000);
        assert!(r > 0, "eventfd_wait timed out");
        let atomic = unsafe { &*(ptr as *const AtomicU64) };
        let val = atomic.load(Ordering::Acquire);
        assert_eq!(val, 0xDEADBEEF);

        // Cleanup
        let mut status: i32 = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
            libc::munmap(ptr, 4096);
            libc::close(fd);
            libc::close(efd);
        }
    }

    /// A parent `futex_wait_u32` against a `MAP_SHARED` memfd unblocks when
    /// a forked child stores a new value and issues `futex_wake_u32`. Proves
    /// the cross-process shared-futex contract the W2M migration depends on.
    #[test]
    fn test_cross_process_futex_on_mapshared() {
        use std::sync::atomic::Ordering;

        let fd = memfd_create(b"test_futex_shared");
        assert!(fd >= 0);
        ftruncate(fd, 4096).unwrap();
        let ptr = mmap_shared(fd, 4096);
        assert!(!ptr.is_null());

        let atomic_ptr = ptr as *mut AtomicU32;
        unsafe {
            (*atomic_ptr).store(7, Ordering::Release);
        }

        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // Child: bump the atomic, then wake the parent.
            unsafe {
                (*atomic_ptr).store(8, Ordering::Release);
            }
            let _ = futex_wake_u32(atomic_ptr as *const AtomicU32, 1);
            unsafe {
                libc::_exit(0);
            }
        }

        // Parent: wait on atomic=7 with a 5-second timeout. Return <=0 via
        // timeout = failure. Value-already-differed (EAGAIN) is also a
        // successful proof — the child got ahead of us.
        let rc = futex_wait_u32(atomic_ptr as *const AtomicU32, 7, 5000);
        let errno = errno();
        // 0 = woke normally; -1 w/ EAGAIN = value was already != 7 by the
        // time we issued the syscall (equally acceptable proof).
        assert!(
            rc == 0 || (rc == -1 && errno == libc::EAGAIN),
            "futex_wait_u32 returned rc={rc} errno={errno}",
        );
        let final_val = unsafe { (*atomic_ptr).load(Ordering::Acquire) };
        assert_eq!(final_val, 8);

        let mut status: i32 = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
            libc::munmap(ptr as *mut libc::c_void, 4096);
            libc::close(fd);
        }
    }

    /// Submit a `FutexWaitV` SQE on a local io_uring, fork a child that
    /// updates the atomic and wakes it via raw FUTEX_WAKE, drain the CQE
    /// in the parent. Smoke test for the reactor's FUTEX_WAITV wiring.
    #[test]
    fn test_futex_waitv_via_io_uring() {
        use io_uring::{opcode, types, IoUring};
        use std::sync::atomic::Ordering;

        let fd = memfd_create(b"test_futex_waitv");
        assert!(fd >= 0);
        ftruncate(fd, 4096).unwrap();
        let ptr = mmap_shared(fd, 4096);
        assert!(!ptr.is_null());

        let atomic_ptr = ptr as *mut AtomicU32;
        unsafe {
            (*atomic_ptr).store(0, Ordering::Release);
        }

        let mut ring = match IoUring::new(8) {
            Ok(r) => r,
            Err(e) => panic!("IoUring::new: {e}"),
        };

        // One-entry FutexWaitV array (heap-stable), waiting for value 0.
        let futexv: Box<[types::FutexWaitV; 1]> = Box::new([types::FutexWaitV::new()
            .val(0)
            .uaddr(atomic_ptr as u64)
            .flags(FUTEX2_SIZE_U32)]);

        let entry = opcode::FutexWaitV::new(futexv.as_ptr(), 1)
            .build()
            .user_data(0xAABBCCDD);
        unsafe {
            ring.submission().push(&entry).expect("sqe push");
        }
        // Flush the SQE to the kernel without blocking for a CQE.
        ring.submit().expect("submit");

        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // Child: brief delay so the parent has time to park, then wake.
            unsafe {
                libc::usleep(50_000);
            }
            unsafe {
                (*atomic_ptr).store(1, Ordering::Release);
            }
            let _ = futex_wake_u32(atomic_ptr as *const AtomicU32, 1);
            unsafe {
                libc::_exit(0);
            }
        }

        // Parent: block until at least one CQE arrives.
        ring.submitter().submit_and_wait(1).expect("submit_and_wait");
        let cqe = ring.completion().next().expect("cqe");
        assert_eq!(
            cqe.user_data(),
            0xAABBCCDD,
            "user_data must round-trip; got 0x{:X}",
            cqe.user_data(),
        );
        // Unsupported opcode → -EINVAL or -ENOSYS. Success → 0 or >0.
        // Val mismatch → -EAGAIN (would happen if the child raced ahead).
        let res = cqe.result();
        assert!(
            res == 0 || res == -libc::EAGAIN,
            "unexpected FutexWaitV result: {} (ENOSYS={} EINVAL={}) — kernel may not support FUTEX_WAITV (Linux 6.7+ required)",
            res, -libc::ENOSYS, -libc::EINVAL,
        );

        let mut status: i32 = 0;
        unsafe {
            libc::waitpid(pid, &mut status, 0);
            libc::munmap(ptr as *mut libc::c_void, 4096);
            libc::close(fd);
        }
        drop(futexv);
    }

    /// The raw multi-word wrapper, mirroring `test_futex_waitv_via_io_uring`:
    /// a wake on a NON-FIRST word wakes the multi-word wait; no-wake times out
    /// to -1; a value mismatch fast-returns. Timing assertions, robust to the
    /// return-code convention.
    #[test]
    fn test_futex_waitv_u32_wakes_on_any_word() {
        use std::sync::atomic::Ordering;
        use std::time::Instant;
        let fd = memfd_create(b"test_waitv_u32");
        ftruncate(fd, 4096).unwrap();
        let ptr = mmap_shared(fd, 4096);
        let w0 = ptr as *const AtomicU32;
        let w1 = unsafe { ptr.add(64) } as *const AtomicU32;
        unsafe {
            (*w0).store(0, Ordering::Release);
            (*w1).store(0, Ordering::Release);
        }

        let t = Instant::now(); // value mismatch → must not block
        let _ = futex_waitv_u32(&[w0, w1], &[0, 999], 2000);
        assert!(t.elapsed().as_millis() < 500);

        let t = Instant::now(); // no wake → -1 near the 200 ms deadline
        assert_eq!(futex_waitv_u32(&[w0, w1], &[0, 0], 200), -1);
        assert!((150..1000).contains(&t.elapsed().as_millis()));

        let pid = unsafe { libc::fork() }; // wake on the NON-FIRST word
        if pid == 0 {
            unsafe {
                libc::usleep(50_000);
                (*w1).fetch_add(1, Ordering::Release);
            }
            let _ = futex_wake_u32(w1, 1);
            unsafe { libc::_exit(0) };
        }
        let t = Instant::now();
        let _ = futex_waitv_u32(&[w0, w1], &[0, 0], 5000);
        assert!(t.elapsed().as_millis() < 2000, "non-first-word wake must wake promptly");
        unsafe {
            let mut s = 0;
            libc::waitpid(pid, &mut s, 0);
            libc::munmap(ptr as *mut libc::c_void, 4096);
            libc::close(fd);
        }
    }

    #[test]
    fn test_memfd_create_and_ftruncate() {
        let fd = memfd_create(b"test_memfd");
        assert!(fd >= 0, "memfd_create failed: {fd}");
        ftruncate(fd, 4096).unwrap();
        assert_eq!(fd_size(fd).unwrap(), 4096);
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_mmap_shared() {
        let fd = memfd_create(b"test_mmap");
        assert!(fd >= 0);
        ftruncate(fd, 8192).unwrap();
        let ptr = mmap_shared(fd, 8192);
        assert!(!ptr.is_null(), "mmap_shared returned null");
        // Write and read back
        unsafe {
            *ptr = 42;
            assert_eq!(*ptr, 42);
            libc::munmap(ptr as *mut libc::c_void, 8192);
            libc::close(fd);
        }
    }
}
