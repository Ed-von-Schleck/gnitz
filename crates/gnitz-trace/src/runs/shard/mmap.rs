//! A read-only mapping of a whole file, unmapped on drop.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

pub(super) struct Mmap {
    ptr: *const u8,
    len: usize,
}

impl Mmap {
    /// Map the whole of `file` read-only; `None` for an empty file. The mapping
    /// holds its own reference to the inode, so the caller may close `file` at
    /// once.
    pub(super) fn of_file(file: &File) -> io::Result<Option<Self>> {
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(None);
        }
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
        Ok(Some(Mmap { ptr: raw.cast(), len }))
    }

    /// `advice` over the mapping; best-effort.
    pub(super) fn advise(&self, advice: libc::c_int) {
        // SAFETY: the range is this mapping, and the advice its callers give changes no byte it reads.
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
        // SAFETY: the mapping `of_file` made, unmapped once.
        unsafe { libc::munmap(self.ptr.cast_mut().cast(), self.len) };
    }
}
