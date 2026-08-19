//! POSIX I/O and Linux syscall wrappers, in three labelled sections. Two rules
//! no signature states on its own:
//!
//! - The **file-I/O tier** returns `io::Result`, so the errno is captured at the
//!   syscall and no caller has to read it back out of ambient state.
//! - The **IPC tier** returns raw codes instead: its callers inspect errno
//!   (EAGAIN/ETIMEDOUT) and re-read the rings rather than trust the return, and
//!   `eventfd_create`/`memfd_create` hand back a raw fd because their callers
//!   hold the descriptor across `fork()` and close it by hand, which an
//!   `OwnedFd` would fight.
//!
//! The third section is the unaligned `*_raw` accessors, documented there.

use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::atomic::AtomicU32;

use libc::c_int;

// ---------------------------------------------------------------------------
// File-I/O tier
// ---------------------------------------------------------------------------

/// Write all bytes to a raw `fd`, handling partial writes and EINTR — the
/// `File::write_all` of a descriptor nobody owns (stdout, a socketpair end).
pub(crate) fn write_all_fd(fd: c_int, data: &[u8]) -> std::io::Result<()> {
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
pub(crate) fn try_set_nocow(fd: i32) {
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

/// Hint the kernel to back [ptr, ptr+size) with transparent hugepages.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
/// For anonymous private memory: requires `enabled` = `madvise` or `always`.
/// For memfd/shmem: requires `shmem_enabled` = `advise` or `within_size`.
/// For writable file-backed mmap: silently ignored by the kernel.
pub(crate) fn madvise_hugepage(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_HUGEPAGE);
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
pub(crate) fn server_create(path: &str) -> std::io::Result<OwnedFd> {
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
/// a peer that is already gone (`ENOTCONN`) is the goal state, not an error.
/// Does NOT close the fd — the caller still reaps it through the normal close
/// path.
pub(crate) fn shutdown(fd: i32) {
    while unsafe { libc::shutdown(fd, libc::SHUT_RDWR) } < 0 {
        if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            return;
        }
    }
}

/// Create a TCP SOCK_STREAM listen socket on `addr`: socket + SO_REUSEADDR +
/// bind + listen(1024) (same backlog as `server_create`) + O_NONBLOCK.
pub(crate) fn tcp_bind(addr: &std::net::SocketAddr) -> std::io::Result<i32> {
    let family = match addr {
        std::net::SocketAddr::V4(_) => libc::AF_INET,
        std::net::SocketAddr::V6(_) => libc::AF_INET6,
    };
    unsafe {
        let fd = libc::socket(family, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 1) < 0 {
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
pub(crate) fn tcp_local_addr(fd: i32) -> Option<std::net::SocketAddr> {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let rc = unsafe { libc::getsockname(fd, &mut ss as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return None;
    }
    addr_from_sockaddr(&ss)
}

/// `setsockopt` of a single `int` option. Returns the raw return code.
fn setsockopt_int(fd: c_int, level: c_int, opt: c_int, val: c_int) -> c_int {
    unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<c_int>() as libc::socklen_t,
        )
    }
}

/// Bare `SO_KEEPALIVE` (no interval tuning): a silently half-open TCP
/// connection is reaped by the kernel default probing (~2 h) instead of
/// parking a recv forever.
pub(crate) fn set_keepalive(fd: i32) {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);
}

/// `TCP_NODELAY`: small control frames must not pay Nagle's 40 ms batching
/// delay (AF_UNIX has no Nagle, so this restores latency parity).
pub(crate) fn set_nodelay(fd: i32) {
    setsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1);
}

/// SocketAddr → (sockaddr_storage, socklen_t). Zero-pads the storage and
/// preserves the IPv6 flowinfo/scope_id so link-local destinations route
/// (without a scope id the kernel rejects an `fe80::` send with EINVAL).
fn sockaddr_from_addr(addr: &std::net::SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
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
fn addr_from_sockaddr(ss: &libc::sockaddr_storage) -> Option<std::net::SocketAddr> {
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

/// Raise the `RLIMIT_NOFILE` soft limit towards `target`, capped by the hard
/// limit. Best-effort: the engine opens far fewer descriptors than `target` on
/// a small database, so a refusal only matters once the partition count grows,
/// and then it surfaces as `EMFILE` at the open that could not be served.
pub(crate) fn raise_fd_limit(target: u64) {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 || rl.rlim_cur >= target as libc::rlim_t {
            return;
        }
        rl.rlim_cur = (target as libc::rlim_t).min(rl.rlim_max);
        libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
    }
}

/// Best-effort memory budget for the process, in bytes: the cgroup v2 limit
/// when there is one, else total physical RAM (`_SC_PHYS_PAGES × _SC_PAGE_SIZE`).
///
/// Cached in a `OnceLock`: the value is invariant for the process's lifetime.
/// Returns 0 only if every source fails, which callers clamp up to a floor.
pub(crate) fn available_memory_bytes() -> usize {
    use std::sync::OnceLock;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Some(v) = cgroup_v2_memory_max() {
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

/// The tightest finite cgroup v2 `memory.max` from this process's own cgroup up
/// to the root; `None` when nothing on the path sets one.
///
/// The path has to come from `/proc/self/cgroup`: a bare `/sys/fs/cgroup/`
/// prefix names the *root* cgroup, which on an ordinary systemd host has no
/// `memory.max` at all, so a `MemoryMax=`d unit would read as host RAM. **v2
/// only** — a v1/hybrid host writes one line per controller instead of the
/// unified `0::<path>`, matches nothing, and falls back to physical RAM.
fn cgroup_v2_memory_max() -> Option<usize> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    // The v2 path is absolute; `join` would discard the mount point.
    let rel = cgroup.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
    std::path::Path::new("/sys/fs/cgroup")
        .join(rel.trim_start_matches('/'))
        .ancestors()
        .take_while(|d| d.starts_with("/sys/fs/cgroup"))
        // The literal `"max"` (unlimited at this level) fails the parse, as
        // does an absent file; both are skipped.
        .filter_map(|d| std::fs::read_to_string(d.join("memory.max")).ok())
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|&v| v > 0)
        .min()
}

/// Size of the file behind `fd` (fstat), in bytes.
fn fd_size(fd: c_int) -> std::io::Result<usize> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(st.st_size as usize)
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

// ---------------------------------------------------------------------------
// IPC tier
// ---------------------------------------------------------------------------

/// Return the errno of the most recent failed syscall.
#[inline]
pub(crate) fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

/// `futex2(2)` flags byte for a 32-bit atomic. Matches the kernel constant
/// `FUTEX2_SIZE_U32` (=2). No `FUTEX2_PRIVATE` bit — W2M is `MAP_SHARED`
/// across `fork()`.
pub(crate) const FUTEX2_SIZE_U32: u32 = 2;

/// Create a non-blocking, close-on-exec eventfd. Returns fd or -1 on error.
pub(crate) fn eventfd_create() -> i32 {
    unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) }
}

/// Signal an eventfd (increment counter by 1), retrying EINTR. A failure needs
/// no reporting: the counter is a wake hint whose loss the reader recovers from
/// by re-reading its ring, so no caller inspects the outcome.
pub(crate) fn eventfd_signal(efd: i32) {
    let v: u64 = 1;
    loop {
        let n = unsafe { libc::write(efd, &v as *const u64 as *const libc::c_void, 8) };
        if n < 0 && errno() == libc::EINTR {
            continue;
        }
        return;
    }
}

/// Wait for an eventfd to become readable.
/// Returns >0 if ready (counter drained), 0 on timeout, <0 on error.
pub(crate) fn eventfd_wait(efd: i32, timeout_ms: i32) -> i32 {
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
pub(crate) fn memfd_create(name: &[u8]) -> i32 {
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
pub(crate) fn futex_wait_u32(ptr: *const AtomicU32, expected: u32, timeout_ms: i32) -> i32 {
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
pub(crate) fn futex_wake_u32(ptr: *const AtomicU32, n_waiters: u32) -> i32 {
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

/// Synchronously wait on MULTIPLE futex words at once (`SYS_futex_waitv`),
/// returning when ANY differs from its expected value or is woken — the
/// synchronous analogue of the reactor's `IORING_OP_FUTEX_WAITV`, and built from
/// the same [`io_uring::types::FutexWaitV`] so there is one declaration of the
/// kernel struct rather than two that can drift. The master must wait on every
/// still-pending worker's `reader_seq`, since a publish by ANY worker wakes only
/// that worker's word and a single-word `futex_wait` would miss it.
///
/// `timeout_ms` becomes an ABSOLUTE `CLOCK_MONOTONIC` deadline — `futex_waitv`
/// requires absolute timeouts, unlike the relative `futex_wait_u32` above; do
/// not "harmonize" the two. `< 0` blocks forever. Returns the woken index
/// (`>= 0`), or `-1` on timeout/error; callers re-read the rings rather than
/// trust the return (the ring data is authoritative, as in `futex_wait_u32`), so
/// any failed syscall degrades to one extra poll rather than a hang.
pub(crate) fn futex_waitv_u32(waiters: &[io_uring::types::FutexWaitV], timeout_ms: i32) -> i32 {
    if waiters.is_empty() {
        return -1;
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
    unsafe {
        libc::syscall(
            libc::SYS_futex_waitv,
            waiters.as_ptr(),
            waiters.len() as libc::c_uint,
            0u32, // flags
            ts_ptr,
            libc::CLOCK_MONOTONIC,
        ) as i32
    }
}

/// How [`map_shared_sized`] grows the backing object before mapping it.
#[derive(Clone, Copy)]
pub(crate) enum Backing {
    /// `fallocate` — reserves the blocks, so a later store cannot fail for
    /// want of space. For a real file, where the space is on disk.
    Reserved,
    /// `ftruncate` — sets the size only. For a memfd, whose pages are RAM
    /// charged on first touch; `fallocate` would commit the whole region up
    /// front (1 GiB per worker for the W2M rings).
    Sized,
}

/// mmap `size` bytes of `fd` `MAP_SHARED` read-write, after growing the backing
/// object to at least `size`.
///
/// The growth is the point: `mmap` past the end of the object succeeds, and the
/// first store into the resulting hole raises `SIGBUS`, for which the server
/// installs no handler. Failing to size the object has to abort here, where the
/// errno still says why, rather than at an arbitrary later write.
pub(crate) fn map_shared_sized(fd: c_int, size: usize, how: Backing) -> std::io::Result<*mut u8> {
    if fd_size(fd)? < size {
        let len = size as libc::off_t;
        match how {
            Backing::Reserved => retry_eintr(|| unsafe { libc::fallocate(fd, 0, 0, len) })?,
            Backing::Sized => retry_eintr(|| unsafe { libc::ftruncate(fd, len) })?,
        }
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

// ---------------------------------------------------------------------------
// Unaligned raw accessors
// ---------------------------------------------------------------------------

// The four `*_raw` accessors below do unaligned `u32`/`u64` reads and writes at
// `base + offset` bytes for the SAL and W2M mmap paths, where the offset is
// computed from a `*mut u8` base pointer that need not meet the alignment a
// `*mut u{32,64}` dereference requires — hence `read_unaligned`/`write_unaligned`.
// Each `# Safety` clause is the same contract: `base + offset + N` must lie
// inside a live allocation, writable for the writes and readable for the reads.

/// # Safety
/// `base + offset + 8` must lie inside a live, writable allocation.
#[inline]
pub(crate) unsafe fn write_u64_raw(base: *mut u8, offset: usize, val: u64) {
    (base.add(offset) as *mut u64).write_unaligned(val);
}

/// # Safety
/// `base + offset + 8` must lie inside a live, readable allocation.
#[inline]
pub(crate) unsafe fn read_u64_raw(base: *const u8, offset: usize) -> u64 {
    (base.add(offset) as *const u64).read_unaligned()
}

/// # Safety
/// `base + offset + 4` must lie inside a live, writable allocation.
#[inline]
pub(crate) unsafe fn write_u32_raw(base: *mut u8, offset: usize, val: u32) {
    (base.add(offset) as *mut u32).write_unaligned(val);
}

/// # Safety
/// `base + offset + 4` must lie inside a live, readable allocation.
#[inline]
pub(crate) unsafe fn read_u32_raw(base: *const u8, offset: usize) -> u32 {
    (base.add(offset) as *const u32).read_unaligned()
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
        shutdown(a);
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

        let mapped = Mmap::from_fd(fd, data.len(), Advice::Sequential).expect("map the anonymous file back");
        assert_eq!(mapped.as_slice(), data, "round-trip through the anonymous file");
    }

    #[test]
    fn test_shutdown_tolerates_enotconn() {
        // An unconnected socket → shutdown fails with ENOTCONN; the wrapper
        // swallows it, since evicting an already-gone peer is not an error.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(fd >= 0, "socket() failed");
        shutdown(fd);
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_server_create_is_nonblocking() {
        // Under a private tempdir, not a fixed `/tmp` name: `/tmp` is shared and
        // sticky, so a socket another user (or a panicked run) left at the bare
        // path is unlinkable and every later run fails to bind. The `TempDir`
        // guard removes the socket with the directory.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("server.sock");
        let fd = server_create(path.to_str().expect("utf8 path")).expect("server_create failed");
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL, 0) };
        drop(fd);
        assert!(flags >= 0, "F_GETFL failed: {flags}");
        assert!(
            flags & libc::O_NONBLOCK != 0,
            "socket is not non-blocking, flags={flags:#o}"
        );
    }

    #[test]
    fn test_eventfd_signal_wait() {
        let fd = eventfd_create();
        assert!(fd >= 0);
        eventfd_signal(fd);
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

    /// The M2W wake contract: a forked child's store through a `MAP_SHARED`
    /// region is visible to the parent once its eventfd signal arrives.
    #[test]
    fn test_cross_process_atomic() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let fd = memfd_create(b"test");
        assert!(fd >= 0);
        let ptr = map_shared_sized(fd, 4096, Backing::Sized).unwrap() as *mut libc::c_void;

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
        let ptr = map_shared_sized(fd, 4096, Backing::Sized).unwrap();

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

    /// The raw multi-word wrapper: a value mismatch fast-returns EAGAIN, no wake
    /// times out, and a wake on a NON-FIRST word wakes the multi-word wait.
    ///
    /// Each case asserts the errno, not just `-1`: the wrapper returns `-1` for
    /// both a mismatch (EAGAIN) and a timeout (ETIMEDOUT), so a broken wake path
    /// would satisfy a bare `-1` after sleeping out the deadline. Only the
    /// timeout case bounds wall-clock, and only from below — an upper bound
    /// would be asserting the machine is idle.
    #[test]
    fn test_futex_waitv_u32_wakes_on_any_word() {
        use io_uring::types::FutexWaitV;
        use std::sync::atomic::Ordering;
        use std::time::Instant;
        let fd = memfd_create(b"test_waitv_u32");
        let ptr = map_shared_sized(fd, 4096, Backing::Sized).unwrap();
        let w0 = ptr as *const AtomicU32;
        let w1 = unsafe { ptr.add(64) } as *const AtomicU32;
        unsafe {
            (*w0).store(0, Ordering::Release);
            (*w1).store(0, Ordering::Release);
        }
        let word = |w: *const AtomicU32, val: u64| FutexWaitV::new().val(val).uaddr(w as u64).flags(FUTEX2_SIZE_U32);

        let rc = futex_waitv_u32(&[word(w0, 0), word(w1, 999)], 2000);
        assert_eq!((rc, errno()), (-1, libc::EAGAIN), "value mismatch must fast-return");

        let t = Instant::now();
        let rc = futex_waitv_u32(&[word(w0, 0), word(w1, 0)], 200);
        assert_eq!((rc, errno()), (-1, libc::ETIMEDOUT), "no wake must time out");
        assert!(t.elapsed().as_millis() >= 150, "timed out before the deadline");

        let pid = unsafe { libc::fork() }; // wake on the NON-FIRST word
        if pid == 0 {
            unsafe {
                libc::usleep(50_000);
                (*w1).fetch_add(1, Ordering::Release);
            }
            let _ = futex_wake_u32(w1, 1);
            unsafe { libc::_exit(0) };
        }
        let rc = futex_waitv_u32(&[word(w0, 0), word(w1, 0)], 5000);
        // EAGAIN = the child got ahead of us, an equally good proof. ETIMEDOUT
        // is the failure this asserts against.
        assert!(
            rc >= 0 || (rc == -1 && errno() == libc::EAGAIN),
            "non-first-word wake must not time out (rc={rc} errno={})",
            errno()
        );
        unsafe {
            let mut s = 0;
            libc::waitpid(pid, &mut s, 0);
            libc::munmap(ptr as *mut libc::c_void, 4096);
            libc::close(fd);
        }
    }

    /// Both growth arms: whichever one runs, the object reaches `size` before
    /// the mapping exists, so the store lands on a real page instead of raising
    /// SIGBUS. Production maps the W2M rings `Sized` (memfd) and the SAL
    /// `Reserved` (a real file).
    #[test]
    fn test_map_shared_sized() {
        let memfd = unsafe { OwnedFd::from_raw_fd(memfd_create(b"test_mmap")) };
        let tmp = tempfile::NamedTempFile::new().unwrap();
        for (how, fd) in [
            (Backing::Sized, memfd.as_raw_fd()),
            (Backing::Reserved, tmp.as_file().as_raw_fd()),
        ] {
            assert!(fd >= 0);
            let ptr = map_shared_sized(fd, 8192, how).unwrap();
            assert_eq!(fd_size(fd).unwrap(), 8192);
            unsafe {
                *ptr = 42;
                assert_eq!(*ptr, 42);
                libc::munmap(ptr as *mut libc::c_void, 8192);
            }
        }
    }
}
