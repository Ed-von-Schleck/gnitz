//! POSIX I/O primitives and non-IPC Linux syscall wrappers: fd read/write with
//! EINTR/partial-write handling, fdatasync/fsync, fallocate, ftruncate, NOCOW,
//! madvise, server socket, fd-limit, plus the `catch_unwind` panic guard.

use std::os::fd::{FromRawFd, OwnedFd};

use libc::c_int;

/// Write all bytes to fd, handling partial writes and EINTR.
/// Returns 0 on success, -3 on error.
pub unsafe fn write_all_fd(fd: c_int, data: &[u8]) -> i32 {
    let mut written: usize = 0;
    let total = data.len();
    while written < total {
        let ret = libc::write(fd, data[written..].as_ptr() as *const libc::c_void, total - written);
        if ret < 0 {
            let e = *libc::__errno_location();
            if e == libc::EINTR {
                continue;
            }
            return -3;
        }
        if ret == 0 {
            // POSIX guarantees regular files never return 0 for a non-zero
            // count, but some device types can — without this guard the loop
            // would spin forever at 100% CPU. Treat it as an error.
            return -3;
        }
        written += ret as usize;
    }
    0
}

/// Read up to `buf.len()` bytes from fd. Returns bytes read, or -3 on error.
pub unsafe fn read_all_fd(fd: c_int, buf: &mut [u8]) -> i64 {
    let total = buf.len();
    let mut offset: usize = 0;
    while offset < total {
        let ret = libc::read(fd, buf[offset..].as_mut_ptr() as *mut libc::c_void, total - offset);
        if ret < 0 {
            let e = *libc::__errno_location();
            if e == libc::EINTR {
                continue;
            }
            return -3;
        }
        if ret == 0 {
            break; // EOF
        }
        offset += ret as usize;
    }
    offset as i64
}

/// `libc::open` returning an `OwnedFd`, or `None` on failure (errno untouched).
pub(crate) fn open_owned(path: &std::ffi::CStr, flags: c_int) -> Option<OwnedFd> {
    let fd = unsafe { libc::open(path.as_ptr(), flags, 0o644 as libc::mode_t) };
    if fd < 0 {
        return None;
    }
    // SAFETY: fresh descriptor from `open`; the `OwnedFd` is the sole closer.
    Some(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// `fdatasync` with EINTR retry. Returns the OS error on any other failure.
pub(crate) fn fdatasync_eintr(fd: c_int) -> std::io::Result<()> {
    loop {
        let rc = unsafe { libc::fdatasync(fd) };
        if rc >= 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// `fsync` with EINTR retry. Returns the OS error on any other failure.
pub(crate) fn fsync_eintr(fd: c_int) -> std::io::Result<()> {
    loop {
        let rc = unsafe { libc::fsync(fd) };
        if rc >= 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// Pre-allocate blocks for fd. Returns 0 on success, -1 on error.
pub fn fallocate(fd: i32, length: i64) -> i32 {
    unsafe { libc::fallocate(fd, 0, 0, length as libc::off_t) }
}

/// Set file size via ftruncate. Returns 0 on success, -1 on error.
pub fn ftruncate(fd: i32, size: i64) -> i32 {
    unsafe { libc::ftruncate(fd, size as libc::off_t) }
}

/// Create an anonymous temporary file (`O_TMPFILE`) on the filesystem backing
/// `dir`, returning its fd (`>= 0`) or -1 on error.
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
pub fn open_tmpfile(dir: &str) -> i32 {
    let dir_c = match std::ffi::CString::new(dir) {
        Ok(c) => c,
        Err(_) => return -1,
    };
    unsafe { libc::open(dir_c.as_ptr(), libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC, 0o600) }
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
/// CPU caches.  Returns 0 on success, -1 on error.
pub fn madvise_populate_write(ptr: *mut u8, size: usize) -> i32 {
    if ptr.is_null() || size == 0 {
        return 0;
    }
    unsafe { libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_POPULATE_WRITE) }
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

/// Hint the kernel to read-ahead [ptr, ptr+size) sequentially.
/// Best-effort: ignores errors and is a no-op for null ptr or size 0.
pub fn madvise_sequential(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, size, libc::MADV_SEQUENTIAL);
    }
}

/// Create a Unix domain SOCK_STREAM server socket: socket + bind + listen.
/// Sets the listen socket to non-blocking.
/// Unlinks any existing socket at `path` before binding.
/// Returns the server fd, or a negative value on error.
#[allow(clippy::needless_range_loop)]
pub fn server_create(path: &str) -> i32 {
    unsafe {
        let fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return -1;
        }

        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let path_bytes = path.as_bytes();
        // Reject paths that would not fit with the null terminator.
        // Consistent with the client-side check in gnitz-core/src/protocol/transport.rs.
        if path_bytes.len() >= addr.sun_path.len() {
            libc::close(fd);
            return -1;
        }
        for i in 0..path_bytes.len() {
            addr.sun_path[i] = path_bytes[i] as libc::c_char;
        }

        // addr is zeroed so sun_path is already null-terminated after the copy above.
        libc::unlink(addr.sun_path.as_ptr());

        if libc::bind(
            fd,
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        ) < 0
        {
            libc::close(fd);
            return -2;
        }
        if libc::listen(fd, 1024) < 0 {
            libc::close(fd);
            return -3;
        }

        // Set non-blocking for the listen socket.
        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        if flags < 0 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            libc::close(fd);
            return -4;
        }

        fd
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

/// Create + bind a UDP socket on `addr`. Requests `rcvbuf_bytes` /
/// `sndbuf_bytes` via SO_RCVBUF/SO_SNDBUF (best-effort; the kernel may
/// clamp). Returns the fd, or a negative value on error (same convention
/// as `server_create`).
///
/// The socket is left unconnected — every send carries an explicit
/// destination — and blocking (O_NONBLOCK is irrelevant under io_uring).
#[allow(dead_code)] // test-only until the network transport binds its QUIC socket
pub fn udp_bind(addr: std::net::SocketAddr, rcvbuf_bytes: usize, sndbuf_bytes: usize) -> i32 {
    let family = match addr {
        std::net::SocketAddr::V4(_) => libc::AF_INET,
        std::net::SocketAddr::V6(_) => libc::AF_INET6,
    };
    unsafe {
        let fd = libc::socket(family, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return -1;
        }
        if !set_sock_buf(fd, libc::SO_RCVBUF, rcvbuf_bytes) {
            libc::close(fd);
            return -2;
        }
        if !set_sock_buf(fd, libc::SO_SNDBUF, sndbuf_bytes) {
            libc::close(fd);
            return -3;
        }
        let (ss, len) = sockaddr_from_addr(&addr);
        if libc::bind(fd, &ss as *const libc::sockaddr_storage as *const libc::sockaddr, len) < 0 {
            libc::close(fd);
            return -4;
        }
        fd
    }
}

/// Set a socket buffer-size option (SO_RCVBUF / SO_SNDBUF). These options
/// take a *const c_int (4 bytes); passing a pointer to `usize` would hand
/// the kernel 4 of its 8 bytes — correct only by luck on little-endian.
/// Copy into a c_int and point at that.
unsafe fn set_sock_buf(fd: i32, opt: libc::c_int, bytes: usize) -> bool {
    let v = bytes as libc::c_int;
    libc::setsockopt(
        fd,
        libc::SOL_SOCKET,
        opt,
        &v as *const libc::c_int as *const libc::c_void,
        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
    ) >= 0
}

/// getsockname() as a SocketAddr — resolves the real port after a
/// port-0 bind. Returns None on error or non-INET family.
#[allow(dead_code)] // test-only until the network transport binds its QUIC socket
pub fn udp_local_addr(fd: i32) -> Option<std::net::SocketAddr> {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let rc = unsafe { libc::getsockname(fd, &mut ss as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return None;
    }
    addr_from_sockaddr(&ss)
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

/// Run `f` under `catch_unwind`. On panic, returns
/// `Err("internal server error (panic in <op>)")`. Otherwise the closure's
/// `Result` is returned unchanged. Used in async handlers and the committer
/// task where a panic must not propagate (per async-invariants V.4 / V.7).
pub fn guard_panic<T, F>(op: &'static str, f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => Err(format!("internal server error (panic in {op})")),
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
        // Guards the happy path of the partial-write loop (and that the new
        // ret==0 guard does not break a normal full write).
        let mut f = tempfile::tempfile().unwrap();
        let data = b"hello write_all_fd partial-write loop";
        let rc = unsafe { write_all_fd(f.as_raw_fd(), data) };
        assert_eq!(rc, 0);
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_fallocate() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let raw_fd = tmp.as_file().as_raw_fd();
        let r = fallocate(raw_fd, 1048576);
        assert_eq!(r, 0, "fallocate failed");
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        unsafe {
            libc::fstat(raw_fd, &mut stat);
        }
        assert_eq!(stat.st_size, 1048576);
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
        let fd = server_create(path);
        assert!(fd >= 0, "server_create failed: {fd}");
        unsafe {
            libc::close(fd);
            libc::unlink(format!("{path}\0").as_ptr() as *const libc::c_char);
        }
    }

    #[test]
    fn test_server_create_path_too_long() {
        // sun_path is 108 bytes; a path of exactly 108 bytes has no room for the null terminator
        let long_path = "/tmp/".to_string() + &"a".repeat(110);
        assert!(long_path.len() >= 108);
        let fd = server_create(&long_path);
        assert!(fd < 0, "expected error for overlong path, got fd={fd}");
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
    fn test_udp_bind_resolves_port() {
        let fd = udp_bind("127.0.0.1:0".parse().unwrap(), 1 << 20, 1 << 20);
        assert!(fd >= 0, "udp_bind failed: {fd}");
        let local = udp_local_addr(fd).expect("udp_local_addr");
        assert!(local.is_ipv4());
        assert_eq!(local.ip().to_string(), "127.0.0.1");
        assert_ne!(local.port(), 0, "port-0 bind must resolve to a real port");
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn test_udp_bind_v6() {
        let fd = udp_bind("[::1]:0".parse().unwrap(), 1 << 20, 1 << 20);
        assert!(fd >= 0, "udp_bind v6 failed: {fd}");
        let local = udp_local_addr(fd).expect("udp_local_addr");
        assert!(local.is_ipv6());
        assert_ne!(local.port(), 0);
        unsafe {
            libc::close(fd);
        }
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
        let fd = open_tmpfile(dir_path);
        assert!(fd >= 0, "open_tmpfile failed on {dir_path}: {fd}");

        let data = b"external-sort spill run bytes";
        assert_eq!(unsafe { write_all_fd(fd, data) }, 0, "write_all_fd");

        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstat(fd, &mut st) }, 0, "fstat");
        assert_eq!(st.st_nlink, 0, "O_TMPFILE inode must have no directory entry");
        assert_eq!(st.st_size as usize, data.len(), "written size");

        assert_eq!(unsafe { libc::lseek(fd, 0, libc::SEEK_SET) }, 0, "rewind");
        let mut back = vec![0u8; data.len()];
        assert_eq!(unsafe { read_all_fd(fd, &mut back) }, data.len() as i64, "read back");
        assert_eq!(&back, data, "round-trip through the anonymous file");
        unsafe {
            libc::close(fd);
        }
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
        let fd = server_create(path);
        assert!(fd >= 0, "server_create failed: {fd}");
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        unsafe {
            libc::close(fd);
            libc::unlink(format!("{path}\0").as_ptr() as *const libc::c_char);
        }
        assert!(flags >= 0, "F_GETFL failed: {flags}");
        assert!(
            flags & libc::O_NONBLOCK != 0,
            "socket is not non-blocking, flags={flags:#o}"
        );
    }
}
