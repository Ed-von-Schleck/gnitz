import pytest
from gnitz.storage.writer import ShardWriter, encode_varint

def test_varint_encoding():
    assert "".join(encode_varint(127)) == "\x7f"
    assert "".join(encode_varint(128)) == "\x80\x01"
    assert "".join(encode_varint(0)) == "\x00"

def test_alignment_logic():
    writer = ShardWriter()
    assert writer._align(64) == 0
    assert writer._align(65) == 63

def test_accumulation():
    writer = ShardWriter()
    writer.add_entry("a", "b", 1)
    assert writer.count == 1
    assert len(writer.key_heap) == 2 # len 1 + 'a'
