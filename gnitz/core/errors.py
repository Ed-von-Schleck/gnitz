"""
gnitz/core/errors.py

Exception hierarchy for GnitzDB storage subsystem.
"""

class GnitzError(Exception):
    """Base class for all GnitzDB exceptions."""
    def __init__(self, msg=""):
        self.msg = msg

class StorageError(GnitzError):
    """Base class for storage subsystem errors."""
    pass

class CorruptShardError(StorageError):
    """Raised when magic numbers mismatch or checksums fail."""
    def __init__(self, msg=""):
        self.msg = msg

class BoundsError(StorageError):
    """Raised when accessing memory outside defined region bounds."""

    def __init__(self, offset, length, limit):
        self.msg = "Out of bounds: offset=%d length=%d limit=%d" % (offset, length, limit)

class LayoutError(StorageError):
    """Raised when data violates physical alignment rules."""
    pass

class MemTableFullError(StorageError):
    """Raised when MemTable arena is exhausted."""
    pass
