import pytest
from gnitz.storage.buffer import MappedBuffer
from gnitz.storage.errors import BoundsError

class MockPtr:
    def __init__(self, data):
        self.data = data
    def __getitem__(self, idx):
        return self.data[idx]

def test_buffer_bounds():
    buf = MappedBuffer("fake_ptr", 10)
    with pytest.raises(BoundsError):
        buf.read_i64(5) # 5 + 8 > 10

def test_buffer_read_logic_mocked(monkeypatch):
    # This is a bit tricky to test in pure python because of rffi.cast,
    # but we can verify the check_bounds calls.
    buf = MappedBuffer("12345678", 8)
    buf._check_bounds(0, 8) # Should pass
    with pytest.raises(BoundsError):
        buf._check_bounds(1, 8)
