//! A read-only mapping of a whole file, unmapped on drop.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

pub(super) struct Mmap {
    ptr: *const u8,
    len: usize,
}

impl Mmap {
    /// Map the whole of `file` read-only. The mapping holds its own reference to
    /// the inode, so the caller may close `file` at once. An empty file is refused
    /// (`EINVAL`).
    pub(super) fn from_file(file: &File) -> io::Result<Self> {
        let len = file.metadata()?.len() as usize;
        // SAFETY: a fresh mapping at an address the kernel picks.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Mmap { ptr: raw.cast(), len })
    }

    /// `MADV_SEQUENTIAL` over the mapping; best-effort.
    pub(super) fn advise_sequential(&self) {
        self.advise(libc::MADV_SEQUENTIAL);
    }

    /// `MADV_HUGEPAGE` over the mapping; best-effort.
    pub(super) fn advise_hugepage(&self) {
        self.advise(libc::MADV_HUGEPAGE);
    }

    fn advise(&self, advice: libc::c_int) {
        // SAFETY: the range is this mapping, and neither advice changes what it reads.
        unsafe { libc::madvise(self.ptr.cast_mut().cast(), self.len, advice) };
    }

    #[inline(always)]
    pub(super) fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` maps `len` readable bytes until drop.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: the mapping `from_file` made, unmapped once.
        unsafe { libc::munmap(self.ptr.cast_mut().cast(), self.len) };
    }
}

#[cfg(test)]
#[path = "tests/mmap.rs"]
mod tests;
