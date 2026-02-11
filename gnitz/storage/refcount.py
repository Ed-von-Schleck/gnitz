from gnitz.storage import errors, mmap_posix
from rpython.rlib import rposix
import os

class FileLockHandle(object):
    """
    Holds the file descriptor and the lock for an active shard reference.
    """
    _immutable_fields_ = ['fd']
    def __init__(self, fd):
        self.fd = fd
        self.ref_count = 1

class RefCounter(object):
    """
    Hardened Process-safe reference counter.
    Uses flock directly on data files to coordinate between Compactor (Writer) 
    and Readers.
    """
    def __init__(self):
        # Maps filename -> FileLockHandle
        self.handles = {}
        # List of filenames marked for deletion by the compactor
        self.pending_deletion = []
    
    def can_delete(self, filename):
        """Returns True if this process has no active handles for the file."""
        return filename not in self.handles

    def acquire(self, filename):
        """
        Acquires a shared lock on the data file.
        Verifies the file is still linked in the filesystem.
        """
        if filename in self.handles:
            self.handles[filename].ref_count += 1
            return

        fd = -1
        try:
            # Open the actual data file for locking
            fd = rposix.open(filename, os.O_RDONLY, 0)
            
            # Shared lock: blocks if a compactor is unlinking this file
            mmap_posix.lock_shared(fd)
            
            # Critical Check: Verify the file wasn't unlinked before we got the lock.
            # If st_nlink == 0, the file exists as an open FD but has been 
            # removed from the directory structure.
            st = os.fstat(fd)
            if st.st_nlink == 0:
                mmap_posix.unlock_file(fd)
                rposix.close(fd)
                raise errors.StorageError("Shard was unlinked before lock acquisition: " + filename)
                
            self.handles[filename] = FileLockHandle(fd)
        except OSError:
            if fd != -1:
                rposix.close(fd)
            raise errors.StorageError("Failed to acquire shard reference: " + filename)
    
    def release(self, filename):
        """
        Decrements local reference count. Releases the flock and closes the FD
        when the count reaches zero.
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
        """Schedules a shard for unlinking."""
        for f in self.pending_deletion:
            if f == filename: return
        self.pending_deletion.append(filename)
    
    def try_cleanup(self):
        """
        Attempts to delete marked shards.
        Safe Cleanup Protocol:
        1. Ensure no local references are held.
        2. Attempt LOCK_EX | LOCK_NB on the data file.
        3. If lock is acquired, no other process is reading; unlink the file.
        """
        deleted = []
        remaining = []
        
        for filename in self.pending_deletion:
            # Skip if we are still using it locally
            if not self.can_delete(filename):
                remaining.append(filename)
                continue
            
            if not os.path.exists(filename):
                deleted.append(filename)
                continue

            fd = -1
            try:
                # Open for exclusive lock probing
                fd = rposix.open(filename, os.O_RDONLY, 0)
                
                if mmap_posix.try_lock_exclusive(fd):
                    # No other process holds LOCK_SH. 
                    # We can safely unlink while holding LOCK_EX.
                    os.unlink(filename)
                    deleted.append(filename)
                    # Implicitly unlocks on close
                    rposix.close(fd)
                else:
                    # Shard is still being used by a reader process
                    rposix.close(fd)
                    remaining.append(filename)
            except OSError:
                if fd != -1:
                    rposix.close(fd)
                remaining.append(filename)
        
        self.pending_deletion = remaining
        return deleted
