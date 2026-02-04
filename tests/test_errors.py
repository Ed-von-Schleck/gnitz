import pytest
from gnitz.storage.errors import GnitzError, StorageError, BoundsError

def test_exception_hierarchy():
    # Verify BoundsError is a StorageError
    try:
        raise BoundsError("Out of bounds")
    except StorageError:
        pass
    except:
        pytest.fail("BoundsError should be caught as StorageError")

def test_base_exception():
    # Verify StorageError is a GnitzError
    try:
        raise StorageError("Disk full")
    except GnitzError:
        pass
    except:
        pytest.fail("StorageError should be caught as GnitzError")
