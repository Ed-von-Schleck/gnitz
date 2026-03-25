//! File-level reference counting and deferred deletion for shard files.

use std::collections::HashMap;
use std::ffi::CString;

pub struct RefCounter {
    /// filename → (fd, refcount). fd holds a shared flock.
    locks: HashMap<String, (i32, u32)>,
    pending_deletion: Vec<String>,
}

impl RefCounter {
    pub fn new() -> Self {
        RefCounter {
            locks: HashMap::new(),
            pending_deletion: Vec::new(),
        }
    }

    pub fn acquire(&mut self, filename: &str) -> Result<(), i32> {
        if let Some(entry) = self.locks.get_mut(filename) {
            entry.1 += 1;
            return Ok(());
        }

        let cpath = CString::new(filename).map_err(|_| -1)?;
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err(-1);
        }

        if unsafe { libc::flock(fd, libc::LOCK_SH) } < 0 {
            unsafe { libc::close(fd); }
            return Err(-1);
        }

        // Verify file wasn't unlinked between open and lock
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } < 0 || st.st_nlink == 0 {
            unsafe { libc::flock(fd, libc::LOCK_UN); }
            unsafe { libc::close(fd); }
            return Err(-2);
        }

        self.locks.insert(filename.to_string(), (fd, 1));
        Ok(())
    }

    pub fn release(&mut self, filename: &str) {
        let remove = if let Some(entry) = self.locks.get_mut(filename) {
            entry.1 -= 1;
            entry.1 == 0
        } else {
            false
        };

        if remove {
            if let Some((fd, _)) = self.locks.remove(filename) {
                unsafe {
                    libc::flock(fd, libc::LOCK_UN);
                    libc::close(fd);
                }
            }
        }
    }

    pub fn can_delete(&self, filename: &str) -> bool {
        !self.locks.contains_key(filename)
    }

    pub fn mark_for_deletion(&mut self, filename: &str) {
        for existing in &self.pending_deletion {
            if existing == filename {
                return;
            }
        }
        self.pending_deletion.push(filename.to_string());
    }

    pub fn try_cleanup(&mut self) {
        let mut remaining = Vec::new();
        let pending = std::mem::take(&mut self.pending_deletion);

        for filename in pending {
            if self.locks.contains_key(&filename) {
                remaining.push(filename);
                continue;
            }

            let cpath = match CString::new(filename.as_str()) {
                Ok(c) => c,
                Err(_) => { remaining.push(filename); continue; }
            };

            let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };
            if fd < 0 {
                // File already gone or inaccessible — either way, done
                continue;
            }

            let locked = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0;
            if locked {
                unsafe { libc::unlink(cpath.as_ptr()); }
                unsafe { libc::close(fd); }
            } else {
                unsafe { libc::close(fd); }
                remaining.push(filename);
            }
        }

        self.pending_deletion = remaining;
    }
}

impl Drop for RefCounter {
    fn drop(&mut self) {
        for (_, (fd, _)) in self.locks.drain() {
            unsafe {
                libc::flock(fd, libc::LOCK_UN);
                libc::close(fd);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, b"test").unwrap();
        let filename = path.to_str().unwrap();

        let mut rc = RefCounter::new();
        assert!(rc.acquire(filename).is_ok());
        assert!(!rc.can_delete(filename));
        rc.release(filename);
        assert!(rc.can_delete(filename));
    }

    #[test]
    fn refcount_increments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, b"test").unwrap();
        let filename = path.to_str().unwrap();

        let mut rc = RefCounter::new();
        rc.acquire(filename).unwrap();
        rc.acquire(filename).unwrap();
        rc.release(filename);
        assert!(!rc.can_delete(filename));
        rc.release(filename);
        assert!(rc.can_delete(filename));
    }

    #[test]
    fn mark_and_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deleteme.db");
        std::fs::write(&path, b"test").unwrap();
        let filename = path.to_str().unwrap();

        let mut rc = RefCounter::new();
        rc.mark_for_deletion(filename);
        rc.try_cleanup();
        assert!(!path.exists());
    }

    #[test]
    fn cleanup_skips_locked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("locked.db");
        std::fs::write(&path, b"test").unwrap();
        let filename = path.to_str().unwrap();

        let mut rc = RefCounter::new();
        rc.acquire(filename).unwrap();
        rc.mark_for_deletion(filename);
        rc.try_cleanup();
        assert!(path.exists()); // still locked
        rc.release(filename);
        rc.try_cleanup();
        assert!(!path.exists()); // now deleted
    }
}
