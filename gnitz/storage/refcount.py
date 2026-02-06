from gnitz.storage import errors
import os

class RefCounter(object):
    """
    Reference counter for safe shard deletion.
    Tracks how many processes/handles are using each file.
    """
    def __init__(self):
        # Maps filename -> reference count
        self.counts = {}
        # Set of files marked for deletion
        self.pending_deletion = []
    
    def acquire(self, filename):
        """
        Increments the reference count for a file.
        
        Args:
            filename: Path to the file
        """
        if filename not in self.counts:
            self.counts[filename] = 0
        self.counts[filename] += 1
    
    def release(self, filename):
        """
        Decrements the reference count for a file.
        
        Args:
            filename: Path to the file
        
        Raises:
            StorageError: If trying to release a file with zero refs
        """
        if filename not in self.counts:
            raise errors.StorageError()  # Cannot release unreferenced file
        
        self.counts[filename] -= 1
        
        if self.counts[filename] < 0:
            raise errors.StorageError()  # Reference count went negative
        
        # If count reaches zero, remove from tracking
        if self.counts[filename] == 0:
            del self.counts[filename]
    
    def can_delete(self, filename):
        """
        Returns True if the file can be safely deleted.
        
        Args:
            filename: Path to the file
        
        Returns:
            True if reference count is zero or file is not tracked
        """
        if filename not in self.counts:
            return True
        return self.counts[filename] == 0
    
    def mark_for_deletion(self, filename):
        """
        Marks a file for deletion.
        The file will be deleted when its reference count reaches zero.
        
        Args:
            filename: Path to the file
        """
        # Check if already marked
        for fn in self.pending_deletion:
            if fn == filename:
                return  # Already marked
        
        self.pending_deletion.append(filename)
    
    def try_cleanup(self):
        """
        Attempts to delete all files marked for deletion that have zero references.
        
        Returns:
            List of filenames that were successfully deleted
        """
        deleted = []
        remaining = []
        
        for filename in self.pending_deletion:
            if self.can_delete(filename):
                # Try to delete the file
                try:
                    if os.path.exists(filename):
                        os.unlink(filename)
                    deleted.append(filename)
                except OSError:
                    # If deletion fails, keep it in pending
                    remaining.append(filename)
            else:
                # Still has references, keep in pending
                remaining.append(filename)
        
        self.pending_deletion = remaining
        return deleted
    
    def get_refcount(self, filename):
        """
        Returns the current reference count for a file.
        
        Args:
            filename: Path to the file
        
        Returns:
            Reference count (0 if not tracked)
        """
        if filename not in self.counts:
            return 0
        return self.counts[filename]
    
    def clear_pending(self):
        """
        Clears all pending deletion entries without deleting files.
        Useful for testing or recovery scenarios.
        """
        self.pending_deletion = []
