# gnitz/storage/errors.py  — backward-compat shim, remove in Phase 5
from gnitz.core.errors import (
    GnitzError, StorageError, CorruptShardError,
    BoundsError, LayoutError, MemTableFullError
)
