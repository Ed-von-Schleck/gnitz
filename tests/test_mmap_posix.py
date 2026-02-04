import mmap
import pytest
from gnitz.storage import mmap_posix

def test_constants():
    assert mmap_posix.PROT_READ == mmap.PROT_READ
    assert mmap_posix.MAP_SHARED == mmap.MAP_SHARED

def test_map_failed_logic():
    assert mmap_posix.MAP_FAILED is not None

def test_error_instantiation():
    err = mmap_posix.MMapError("test")
    assert err.msg == "test"
