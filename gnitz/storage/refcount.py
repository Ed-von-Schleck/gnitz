from gnitz.storage import errors, mmap_posix
from rpython.rlib import rposix
import os

class FileLockHandle(object):
    _immutable_fields_ = ['fd']
    def __init__(self, fd):
        self.fd = fd
        self.ref_count = 1

class RefCounter(object):
    """
    Process-safe reference counter using flock on sidecar lock files.
    Ensures readers (LOCK_SH) prevent deletion (LOCK_EX).
    """
    def __init__(self):
        # Maps filename -> FileLockHandle
        self.handles = {}
        # List of filenames marked for deletion
        self.pending_deletion = []
    
    def _get_lock_path(self, filename):
        return filename + ".lock"
    
    def can_delete(self, filename):
        """
        Returns True if the current process holds no active references.
        Global safety is guaranteed by try_cleanup probing other processes.
        """
        return filename not in self.handles

    def acquire(self, filename):
        """
        Acquires a shared lock (LOCK_SH) on the file's lock-file.
        """
        if filename in self.handles:
            self.handles[filename].ref_count += 1
            return

        lock_path = self._get_lock_path(filename)
        try:
            fd = rposix.open(lock_path, os.O_RDWR | os.O_CREAT, 0o666)
            mmap_posix.lock_shared(fd)
            self.handles[filename] = FileLockHandle(fd)
        except OSError:
            raise errors.StorageError("Failed to acquire lock for: " + filename)
    
    def release(self, filename):
        """
        Decrements local reference. Releases flock if count reaches zero.
        """
        if filename not in self.handles:
            raise errors.StorageError("Attempted to release unacquired file: " + filename)
        
        handle = self.handles[filename]
        handle.ref_count -= 1
        
        if handle.ref_count <= 0:
            mmap_posix.unlock_file(handle.fd)
            rposix.close(handle.fd)
            del self.handles[filename]
            
    def mark_for_deletion(self, filename):
        """Schedules a file for eventual unlinking."""
        for f in self.pending_deletion:
            if f == filename: return
        self.pending_deletion.append(filename)
    
    def try_cleanup(self):
        """
        Attempts to delete marked files. Safety check:
        1. No local references held (must not be in self.handles).
        2. No other process references held (must acquire LOCK_EX | LOCK_NB).
        """
        deleted = []
        remaining = []
        
        for filename in self.pending_deletion:
            if not self.can_delete(filename):
                remaining.append(filename)
                continue
                
            lock_path = self._get_lock_path(filename)
            lock_fd = -1
            try:
                if not os.path.exists(lock_path):
                    if os.path.exists(filename):
                        os.unlink(filename)
                    deleted.append(filename)
                    continue

                lock_fd = rposix.open(lock_path, os.O_RDWR, 0o666)
                if mmap_posix.try_lock_exclusive(lock_fd):
                    # Successfully proved no other process holds a shared lock
                    if os.path.exists(filename):
                        os.unlink(filename)
                    if os.path.exists(lock_path):
                        os.unlink(lock_path)
                    
                    deleted.append(filename)
                    rposix.close(lock_fd)
                else:
                    # File is currently in use by another process
                    rposix.close(lock_fd)
                    remaining.append(filename)
            except OSError:
                if lock_fd != -1: rposix.close(lock_fd)
                remaining.append(filename)
        
        self.pending_deletion = remaining
        return deleted
