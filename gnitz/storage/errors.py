"""
gnitz/storage/errors.py

Exception hierarchy for GnitzDB storage subsystem.
"""

class GnitzError(Exception):
    """Base class for all GnitzDB exceptions."""
    pass

class StorageError(GnitzError):
    """Base class for storage subsystem errors."""
    pass

class CorruptShardError(StorageError):
    """Raised when magic numbers mismatch or checksums fail."""
    pass

class BoundsError(StorageError):
    """Raised when accessing memory outside defined region bounds."""
    pass

class LayoutError(StorageError):
    """Raised when data violates physical alignment rules."""
    pass

class MemTableFullError(StorageError):
    """Raised when MemTable arena is exhausted."""
    pass
