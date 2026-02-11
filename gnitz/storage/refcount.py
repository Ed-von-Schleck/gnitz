import os
import errno
from gnitz.storage import errors, mmap_posix
from rpython.rlib import rposix

class FileLockHandle(object):
    _immutable_fields_ = ['fd']
    def __init__(self, fd):
        self.fd = fd
        self.ref_count = 1

class RefCounter(object):
    def __init__(self):
        self.handles = {}
        self.pending_deletion = []
    
    def can_delete(self, filename):
        return filename not in self.handles

    def acquire(self, filename):
        if filename in self.handles:
            self.handles[filename].ref_count += 1
            return

        fd = -1
        try:
            fd = rposix.open(filename, os.O_RDONLY, 0)
            mmap_posix.lock_shared(fd)
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
        if filename not in self.handles:
            raise errors.StorageError("Attempted to release unacquired file: " + filename)
        
        handle = self.handles[filename]
        handle.ref_count -= 1
        
        if handle.ref_count <= 0:
            mmap_posix.unlock_file(handle.fd)
            rposix.close(handle.fd)
            del self.handles[filename]
            
    def mark_for_deletion(self, filename):
        for f in self.pending_deletion:
            if f == filename: return
        self.pending_deletion.append(filename)
    
    def try_cleanup(self):
        deleted = []
        remaining = []
        
        for filename in self.pending_deletion:
            if not self.can_delete(filename):
                remaining.append(filename)
                continue
            
            # If the file is already gone, it's a success.
            if not os.path.exists(filename):
                deleted.append(filename)
                continue

            fd = -1
            try:
                fd = rposix.open(filename, os.O_RDONLY, 0)
                
                if mmap_posix.try_lock_exclusive(fd):
                    os.unlink(filename)
                    deleted.append(filename)
                    rposix.close(fd)
                else:
                    rposix.close(fd)
                    remaining.append(filename)
            except OSError as e:
                if fd != -1:
                    rposix.close(fd)
                # If the file disappeared during cleanup, it's successful.
                if e.errno == errno.ENOENT:
                    deleted.append(filename)
                else:
                    # Bubble up unexpected system errors.
                    raise e
        
        self.pending_deletion = remaining
        return deleted
