import pytest
from gnitz.storage.arena import Arena
from gnitz.storage.errors import StorageError

def test_alignment():
    # Size 1024 bytes
    arena = Arena(1024)
    
    # First allocation (offset 0)
    p1 = arena.alloc(5)
    # Next allocation should be at offset 8 (aligned)
    p2 = arena.alloc(1)
    
    # We can't easily compare raw pointers in pytest, 
    # but we can check the internal offset state
    assert arena.offset == 9 # 8 (start) + 1 (size)
    
    arena.free()

def test_overflow():
    arena = Arena(10)
    arena.alloc(5)
    with pytest.raises(StorageError):
        arena.alloc(10) # 8 (aligned start) + 10 > 10
    arena.free()

def test_reset():
    arena = Arena(100)
    arena.alloc(50)
    assert arena.offset > 0
    arena.reset()
    assert arena.offset == 0
    arena.free()
