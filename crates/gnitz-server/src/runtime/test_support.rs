//! Helpers shared by `runtime`'s test suites, across `orchestration`,
//! `protocol` and `suites`.
//!
//! Here rather than in the engine's testkit because nothing below this crate
//! uses them: a ring is a `runtime` shape, and a helper that crosses no crate
//! boundary should not sit on one's published surface.

/// Anonymous `MAP_SHARED` region for IPC-shaped tests; unmapped on drop.
/// `MAP_SHARED` so a `fork()`ed child sees the same pages (a child that
/// `_exit`s never runs drops, so only the parent unmaps). Pages are
/// kernel-zeroed and lazily populated, and `MAP_NORESERVE` keeps even a
/// ring-sized region off `Committed_AS`.
pub(crate) struct SharedRegion {
    ptr: *mut u8,
    size: usize,
}

impl SharedRegion {
    pub(crate) fn new(size: usize) -> Self {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED, "SharedRegion mmap failed");
        SharedRegion {
            ptr: ptr as *mut u8,
            size,
        }
    }

    pub(crate) fn ptr(&self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for SharedRegion {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.size);
        }
    }
}
